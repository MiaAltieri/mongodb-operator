# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class, we manage client database relations.

This class creates a user and database for each application relation
and expose needed information for client connection via fields in
external relation.
"""
import json
import logging
import re
from collections import namedtuple
from typing import List, Optional, Set

from charms.data_platform_libs.v0.data_interfaces import DatabaseProvides
from charms.mongodb.v0.mongo import MongoConfiguration, MongoConnection
from charms.mongodb.v1.helpers import generate_password
from ops.charm import (
    CharmBase,
    EventBase,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationEvent,
)
from ops.framework import Object
from ops.model import Relation
from pymongo.errors import PyMongoError

from config import Config

# The unique Charmhub library identifier, never change it
LIBID = "4067879ef7dd4261bf6c164bc29d94b1"

# Increment this major API version when introducing breaking changes
LIBAPI = 1

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 16

logger = logging.getLogger(__name__)
REL_NAME = "database"
MONGOS_RELATIONS = "cluster"
MONGOS_CLIENT_RELATIONS = "mongos_proxy"
MANAGED_USERS_KEY = "managed-users-key"

# We expect the MongoDB container to use the default ports

Diff = namedtuple("Diff", "added changed deleted")
Diff.__doc__ = """
A tuple for storing the diff between two data mappings.
added — keys that were added
changed — keys that still exist but have new values
deleted — key that were deleted."""


class FailedToGetHostsError(Exception):
    """Raised when charm fails to retrieve hosts."""


class MongoDBProvider(Object):
    """In this class, we manage client database relations."""

    def __init__(self, charm: CharmBase, substrate="k8s", relation_name: str = REL_NAME) -> None:
        """Constructor for MongoDBProvider object.

        Args:
            charm: the charm for which this relation is provided
            substrate: host type, either "k8s" or "vm"
            relation_name: the name of the relation
        """
        self.substrate = substrate
        self.charm = charm

        super().__init__(charm, relation_name)
        self.framework.observe(
            charm.on[relation_name].relation_departed,
            self.charm.check_relation_broken_or_scale_down,
        )
        self.framework.observe(charm.on[relation_name].relation_broken, self._on_relation_event)
        self.framework.observe(charm.on[relation_name].relation_changed, self._on_relation_event)

        # Charm events defined in the database provides charm library.
        self.database_provides = DatabaseProvides(self.charm, relation_name=relation_name)
        self.framework.observe(
            self.database_provides.on.database_requested, self._on_relation_event
        )

    def pass_sanity_hook_checks(self) -> bool:
        """Runs reusable and event agnostic checks."""
        # We shouldn't try to create or update users if the database is not
        # initialised. We will create users as part of initialisation.
        if not self.charm.db_initialised:
            return False

        # Warning: the sanity_hook_checks can pass when the call is
        # issued by a config-sever because the config-server is allowed to manage the users
        # in MongoDB. This is not well named and does not protect integration of a config-server
        # to a client application. The mongos charm however doesn't care
        # because it supports only one integration that uses MongoDBProvider.
        if not self.charm.is_role(Config.Role.MONGOS) and not self.charm.is_relation_feasible(
            self.get_relation_name()
        ):
            logger.info("Skipping code for relations.")
            return False

        if not self.charm.unit.is_leader():
            return False

        return True

    def pass_hook_checks(self, event: RelationEvent) -> bool:
        """Runs the pre-hooks checks for MongoDBProvider, returns True if all pass."""
        # First, ensure that the relation is valid, useless to do anything else otherwise
        if not self.charm.is_role(Config.Role.MONGOS) and not self.charm.is_relation_feasible(
            event.relation.name
        ):
            return False

        if not self.pass_sanity_hook_checks():
            return False

        if self.charm.upgrade_in_progress:
            logger.warning(
                "Adding relations is not supported during an upgrade. The charm may be in a broken, unrecoverable state."
            )
            event.defer()
            return False

        return True

    def _on_relation_event(self, event):
        """Handle relation joined events.

        When relations join, change, or depart, the :class:`MongoDBClientRelation`
        creates or drops MongoDB users and sets credentials into relation
        data. As a result, related charm gets credentials for accessing the
        MongoDB database.
        """
        if not self.pass_hook_checks(event):
            logger.info("Skipping %s: hook checks did not pass", type(event))
            return

        departed_relation_id = None
        if type(event) is RelationBrokenEvent:
            # Only relation_deparated events can check if scaling down
            departed_relation_id = event.relation.id
            if not self.charm.has_departed_run(departed_relation_id):
                logger.info(
                    "Deferring, must wait for relation departed hook to decide if relation should be removed."
                )
                event.defer()
                return

            # check if were scaling down and add a log message
            if self.charm.is_scaling_down(departed_relation_id):
                logger.info(
                    "Relation broken event occurring due to scale down, do not proceed to remove users."
                )
                return

            logger.info(
                "Relation broken event occurring due to relation removal, proceed to remove user."
            )

        try:
            self.oversee_users(departed_relation_id, event)
        except (PyMongoError, FailedToGetHostsError) as e:
            # Failed to get hosts error is unique to mongos-k8s charm. In other charms we do not
            # foresee issues to retrieve hosts. However in external mongos-k8s, the leader can
            # attempt to retrieve hosts while non-leader units are still enabling node port
            # resulting in an exception.
            logger.error("Deferring _on_relation_event since: error=%r", e)
            event.defer()
            return

    def oversee_users(self, departed_relation_id: Optional[int], event):
        """Oversees the users of the application.

        Function manages user relations by removing, updated, and creating
        users; and dropping databases when necessary.

        Args:
            departed_relation_id: When specified execution of functions
                makes sure to exclude the users and databases and remove
                them if necessary.
            event: relation event.

        When the function is executed in relation departed event, the departed
        relation is still on the list of all relations. Therefore, for proper
        work of the function, we need to exclude departed relation from the list.

        Raises:
            PyMongoError
        """
        with MongoConnection(self.charm.mongo_config) as mongo:
            database_users = mongo.get_users()

        users_being_managed = database_users.intersection(self._get_relational_users_to_manage())
        expected_current_users = self._get_users_from_relations(departed_relation_id)

        self.remove_users(users_being_managed, expected_current_users)
        self.add_users(users_being_managed, expected_current_users)
        self.update_users(event, users_being_managed, expected_current_users)
        self.auto_delete_dbs(departed_relation_id)

    def remove_users(
        self, users_being_managed: Set[str], expected_current_users: Set[str]
    ) -> None:
        """Removes users from Charmed MongoDB.

        Note this only removes users that this application of Charmed MongoDB is responsible for
        managing. It won't remove:
        1. users created from other applications
        2. users created from other mongos routers.

        Raises:
            PyMongoError
        """
        with MongoConnection(self.charm.mongo_config) as mongo:
            for username in users_being_managed - expected_current_users:
                logger.info("Remove relation user: %s", username)
                if (
                    self.charm.is_role(Config.Role.MONGOS)
                    and username == self.charm.mongo_config.username
                ):
                    continue

                # for user removal of mongos-k8s router, we let the router remove itself
                if (
                    self.charm.is_role(Config.Role.CONFIG_SERVER)
                    and self.substrate == Config.Substrate.K8S
                ):
                    logger.info("K8s routers will remove themselves.")
                    self._remove_from_relational_users_to_manage(username)
                    return

                mongo.drop_user(username)
                self._remove_from_relational_users_to_manage(username)

    def add_users(self, users_being_managed: Set[str], expected_current_users: Set[str]) -> None:
        """Adds users to Charmed MongoDB.

        Raises:
            PyMongoError
        """
        with MongoConnection(self.charm.mongo_config) as mongo:
            for username in expected_current_users - users_being_managed:
                config = self._get_config(username, None)
                if config.database is None:
                    # We need to wait for the moment when the provider library
                    # set the database name into the relation.
                    continue
                logger.info("Create relation user: %s on %s", config.username, config.database)

                mongo.create_user(config)
                self._add_to_relational_users_to_manage(username)
                self._set_relation(config)

    def update_users(
        self, event: EventBase, users_being_managed: Set[str], expected_current_users: Set[str]
    ) -> None:
        """Updates existing users in Charmed MongoDB.

        Raises:
            PyMongoError
        """
        with MongoConnection(self.charm.mongo_config) as mongo:
            for username in expected_current_users.intersection(users_being_managed):
                config = self._get_config(username, None)
                logger.info("Update relation user: %s on %s", config.username, config.database)
                mongo.update_user(config)
                logger.info("Updating relation data according to diff")
                self._diff(event)

    def auto_delete_dbs(self, departed_relation_id):
        """Delete's unused dbs if configured to do so.

        Raises:
            PyMongoError
        """
        with MongoConnection(self.charm.mongo_config) as mongo:

            if not self.charm.model.config["auto-delete"]:
                return

            database_dbs = mongo.get_databases()
            relation_dbs = self._get_databases_from_relations(departed_relation_id)
            for database in database_dbs - relation_dbs:
                logger.info("Drop database: %s", database)
                mongo.drop_database(database)

    def _diff(self, event: RelationChangedEvent) -> Diff:
        """Retrieves the diff of the data in the relation changed databag.

        Args:
            event: relation changed event.

        Returns:
            a Diff instance containing the added, deleted and changed
                keys from the event relation databag.
        """
        if not isinstance(event, RelationChangedEvent):
            logger.info("Cannot compute diff of event type: %s", type(event))
            return
        # TODO import marvelous unit tests in a future PR
        # Retrieve the old data from the data key in the application relation databag.
        old_data = json.loads(event.relation.data[self.charm.model.app].get("data", "{}"))
        # Retrieve the new data from the event relation databag.
        new_data = {
            key: value for key, value in event.relation.data[event.app].items() if key != "data"
        }

        # These are the keys that were added to the databag and triggered this event.
        added = new_data.keys() - old_data.keys()
        # These are the keys that were removed from the databag and triggered this event.
        deleted = old_data.keys() - new_data.keys()
        # These are the keys that already existed in the databag
        # but had their values changed.
        changed = {
            key for key in old_data.keys() & new_data.keys() if old_data[key] != new_data[key]
        }

        # TODO: update when evaluation of the possibility of losing the diff is completed
        # happens in the charm before the diff is completely checked (DPE-412).
        # Convert the new_data to a serializable format and save it for a next diff check.
        event.relation.data[self.charm.model.app].update({"data": json.dumps(new_data)})

        # Return the diff with all possible changes.
        return Diff(added, changed, deleted)

    def update_app_relation_data(self) -> None:
        """Helper function to update application relation data."""
        if not self.pass_sanity_hook_checks():
            return

        database_users = set()

        with MongoConnection(self.charm.mongo_config) as mongo:
            database_users = mongo.get_users()

        for relation in self._get_relations():
            username = self._get_username_from_relation_id(relation.id)
            password = self._get_or_set_password(relation)
            config = self._get_config(username, password)
            # relations with the mongos server should not connect though the config-server directly
            if self.charm.is_role(Config.Role.CONFIG_SERVER):
                continue

            if username in database_users:
                self.database_provides.set_endpoints(
                    relation.id,
                    ",".join(config.hosts),
                )
                self.database_provides.set_uris(
                    relation.id,
                    config.uri,
                )

    def _get_or_set_password(self, relation: Relation) -> str:
        """Retrieve password from cache or generate a new one.

        Args:
            relation (Relation): The relation for each the password is cached.

        Returns:
            str: The password.
        """
        password = self.database_provides.fetch_my_relation_field(relation.id, "password")
        if password:
            return password
        password = generate_password()
        self.database_provides.update_relation_data(relation.id, {"password": password})
        return password

    def _get_config(
        self, username: str, password: Optional[str], event=None
    ) -> MongoConfiguration:
        """Construct the config object for future user creation."""
        relation = self._get_relation_from_username(username)
        if not password:
            password = self._get_or_set_password(relation)

        database_name = self._get_database_from_relation(relation)

        mongo_args = {
            "database": database_name,
            "username": username,
            "password": password,
            "hosts": self.charm.mongo_config.hosts,
            "roles": self._get_roles_from_relation(relation),
            "tls_external": False,
            "tls_internal": False,
        }

        if self.charm.is_role(Config.Role.MONGOS):
            mongo_args["port"] = Config.MONGOS_PORT
            if self.substrate == Config.Substrate.K8S:
                mongo_args["hosts"] = self.charm.get_mongos_hosts_for_client()
                mongo_args["port"] = self.charm.get_mongos_port()
        else:
            mongo_args["replset"] = self.charm.app.name

        return MongoConfiguration(**mongo_args)

    def _set_relation(self, config: MongoConfiguration):
        """Save all output fields into application relation."""
        relation = self._get_relation_from_username(config.username)
        if relation is None:
            return None

        self.database_provides.set_credentials(relation.id, config.username, config.password)
        self.database_provides.set_database(relation.id, config.database)

        # relations with the mongos server should not connect though the config-server directly
        if self.charm.is_role(Config.Role.CONFIG_SERVER):
            return

        self.database_provides.set_endpoints(
            relation.id,
            ",".join(config.hosts),
        )
        if not self.charm.is_role(Config.Role.MONGOS):
            self.database_provides.set_replset(
                relation.id,
                config.replset,
            )
        self.database_provides.set_uris(
            relation.id,
            config.uri,
        )

    @staticmethod
    def _get_username_from_relation_id(relation_id: int) -> str:
        """Construct username."""
        return f"relation-{relation_id}"

    def _get_users_from_relations(self, departed_relation_id: Optional[int]):
        """Return usernames for all relations except departed relation."""
        relations = self._get_relations()
        return set(
            [
                self._get_username_from_relation_id(relation.id)
                for relation in relations
                if relation.id != departed_relation_id
            ]
        )

    def _get_databases_from_relations(self, departed_relation_id: Optional[int]) -> Set[str]:
        """Return database names from all relations.

        Args:
            departed_relation_id: when specified return all database names
                except for those databases that belong to the departing
                relation specified.
        """
        relations = self._get_relations()
        databases = set()
        for relation in relations:
            if relation.id == departed_relation_id:
                continue
            database = self._get_database_from_relation(relation)
            if database is not None:
                databases.add(database)
        return databases

    def _get_relation_from_username(self, username: str) -> Relation:
        """Parse relation ID from a username and return Relation object."""
        match = re.match(r"^relation-(\d+)$", username)
        # We generated username in `_get_users_from_relations`
        # func and passed it into this function later.
        # It means the username here MUST match regex.
        assert match is not None, "No relation match"
        relation_id = int(match.group(1))
        logger.debug("Relation ID: %s", relation_id)
        relation_name = self.get_relation_name()
        return self.model.get_relation(relation_name, relation_id)

    def _get_relations(self) -> List[Relation]:
        """Return the set of relations for users.

        We create users for either direct relations to charm or for relations through the mongos
        charm.
        """
        return self.model.relations[self.get_relation_name()]

    def get_relation_name(self):
        """Returns the name of the relation to use."""
        if self.charm.is_role(Config.Role.CONFIG_SERVER):
            return MONGOS_RELATIONS
        elif self.charm.is_role(Config.Role.MONGOS):
            return MONGOS_CLIENT_RELATIONS
        else:
            return REL_NAME

    def _get_relational_users_to_manage(self) -> Set[str]:
        """Returns a set of the users to manage.

        Note json cannot serialise sets. Convert from list.
        """
        return set(json.loads(self.charm.app_peer_data.get(MANAGED_USERS_KEY, "[]")))

    def _update_relational_users_to_manage(self, new_users: Set[str]) -> None:
        """Updates the set of the users to manage.

        Note json cannot serialise sets. Convert from list.
        """
        if not self.charm.unit.is_leader():
            raise Exception("Cannot update relational data on non-leader unit")

        self.charm.app_peer_data[MANAGED_USERS_KEY] = json.dumps(list(new_users))

    def _remove_from_relational_users_to_manage(self, user_to_remove: str) -> None:
        """Removes the provided user from the set of the users to manage."""
        current_users = self._get_relational_users_to_manage()
        updated_users = current_users - {user_to_remove}
        self._update_relational_users_to_manage(updated_users)

    def _add_to_relational_users_to_manage(self, user_to_add: str) -> None:
        """Adds the provided user to the set of the users to manage."""
        current_users = self._get_relational_users_to_manage()
        current_users.add(user_to_add)
        self._update_relational_users_to_manage(current_users)

    def remove_all_relational_users(self):
        """Removes all users from DB.

        Raises: PyMongoError.
        """
        with MongoConnection(self.charm.mongo_config) as mongo:
            database_users = mongo.get_users()

        users_being_managed = database_users.intersection(self._get_relational_users_to_manage())
        self.remove_users(users_being_managed, expected_current_users=set())

        # now we must remove all of their connection info
        for relation in self._get_relations():
            fields = self.database_provides.fetch_my_relation_data([relation.id])[relation.id]
            self.database_provides.delete_relation_data(relation.id, fields=list(fields))

            # unforatunately the above doesn't work to remove secrets, so we forcibly remove the
            # rest manually remove the secret before clearing the databag
            for unit in relation.units:
                secret_id = json.loads(relation.data[unit]["data"])["secret-user"]
                # secret id is the same on all units for `secret-user`
                break

            user_secrets = self.charm.model.get_secret(id=secret_id)
            user_secrets.remove_all_revisions()
            user_secrets.get_content(refresh=True)
            relation.data[self.charm.app].clear()

    @staticmethod
    def _get_database_from_relation(relation: Relation) -> Optional[str]:
        """Return database name from relation."""
        database = relation.data[relation.app].get("database", None)
        return database

    @staticmethod
    def _get_roles_from_relation(relation: Relation) -> Set[str]:
        """Return additional user roles from relation if specified or return None."""
        roles = relation.data[relation.app].get("extra-user-roles", None)
        if roles is not None:
            return set(roles.split(","))
        return {"default"}
