# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class, we manage relations between config-servers and shards.

This class handles the sharing of secrets between sharded components, adding shards, and removing
shards.
"""
import logging
from typing import Optional

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
    DatabaseRequestedEvent,
    DatabaseRequires,
)
from charms.mongodb.v0.mongo import MongoConnection
from charms.mongodb.v1.mongos import MongosConnection
from ops.charm import (
    CharmBase,
    EventBase,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationCreatedEvent,
)
from ops.framework import Object
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    StatusBase,
    WaitingStatus,
)
from pymongo.errors import PyMongoError

from config import Config

logger = logging.getLogger(__name__)
KEYFILE_KEY = "key-file"
KEY_FILE = "keyFile"
HOSTS_KEY = "host"
CONFIG_SERVER_DB_KEY = "config-server-db"
MONGOS_SOCKET_URI_FMT = "%2Fvar%2Fsnap%2Fcharmed-mongodb%2Fcommon%2Fvar%2Fmongodb-27018.sock"
INT_TLS_CA_KEY = f"int-{Config.TLS.SECRET_CA_LABEL}"

# The unique Charmhub library identifier, never change it
LIBID = "58ad1ccca4974932ba22b97781b9b2a0"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 16


class ClusterProvider(Object):
    """Manage relations between the config server and mongos router on the config-server side."""

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str = Config.Relations.CLUSTER_RELATIONS_NAME,
        substrate: str = Config.Substrate.VM,
    ) -> None:
        """Constructor for ShardingProvider object."""
        self.substrate = substrate
        self.relation_name = relation_name
        self.charm = charm
        self.database_provides = DatabaseProvides(self.charm, relation_name=self.relation_name)

        super().__init__(charm, self.relation_name)
        self.framework.observe(
            self.database_provides.on.database_requested, self._on_database_requested
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed, self._on_relation_changed
        )

        self.framework.observe(
            charm.on[self.relation_name].relation_departed,
            self.charm.check_relation_broken_or_scale_down,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken, self._on_relation_broken
        )

    def pass_hook_checks(self, event: EventBase) -> bool:
        """Runs the pre-hooks checks for ClusterProvider, returns True if all pass."""
        if not self.charm.unit.is_leader():
            return False

        if not self.charm.db_initialised:
            logger.info("Deferring %s. db is not initialised.", type(event))
            event.defer()
            return False

        if not self.is_valid_mongos_integration():
            logger.info(
                "Skipping %s. ClusterProvider is only be executed by config-server",
                type(event),
            )
            return False

        if self.charm.upgrade_in_progress:
            logger.warning(
                "Processing mongos applications is not supported during an upgrade. The charm may be in a broken, unrecoverable state."
            )
            event.defer()
            return False

        return True

    def is_valid_mongos_integration(self) -> bool:
        """Returns true if the integration to mongos is valid."""
        is_integrated_to_mongos = len(
            self.charm.model.relations[Config.Relations.CLUSTER_RELATIONS_NAME]
        )

        if not self.charm.is_role(Config.Role.CONFIG_SERVER) and is_integrated_to_mongos:
            return False

        return True

    def _on_database_requested(self, event: DatabaseRequestedEvent | RelationChangedEvent) -> None:
        """Handles the database requested event.

        The first time secrets are written to relations should be on this event.

        Note: If secrets are written for the first time on other events we risk
        the chance of writing secrets in plain sight.
        """
        if not self.pass_hook_checks(event):
            if not self.is_valid_mongos_integration():
                self.charm.status.set_and_share_status(
                    BlockedStatus(
                        "Relation to mongos not supported, config role must be config-server"
                    )
                )
            logger.info("Skipping relation joined event: hook checks did not pass")
            return
        config_server_db = self.generate_config_server_db()
        # create user and set secrets for mongos relation
        self.charm.client_relations.oversee_users(None, None)
        relation_data = {
            KEYFILE_KEY: self.charm.get_secret(
                Config.Relations.APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME
            ),
            CONFIG_SERVER_DB_KEY: config_server_db,
        }

        # if tls enabled
        int_tls_ca = self.charm.tls.get_tls_secret(
            internal=True, label_name=Config.TLS.SECRET_CA_LABEL
        )
        if int_tls_ca:
            relation_data[INT_TLS_CA_KEY] = int_tls_ca
        self.database_provides.update_relation_data(event.relation.id, relation_data)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handles providing mongos with KeyFile and hosts."""
        # First we need to ensure that the database requested event has run
        # otherwise we risk the chance of writing secrets in plain sight.
        if not self.database_provides.fetch_relation_field(event.relation.id, "database"):
            logger.info("Database Requested has not run yet, skipping.")
            event.defer()
            return

        # TODO : This workflow is a fix until we have time for a better and complete fix (DPE-5513)
        self._on_database_requested(event)

    def _on_relation_broken(self, event) -> None:
        if self.charm.upgrade_in_progress:
            logger.warning(
                "Removing integration to mongos is not supported during an upgrade. The charm may be in a broken, unrecoverable state."
            )

        # Only relation_deparated events can check if scaling down
        departed_relation_id = event.relation.id
        if not self.charm.has_departed_run(departed_relation_id):
            logger.info(
                "Deferring, must wait for relation departed hook to decide if relation should be removed."
            )
            event.defer()
            return

        if not self.pass_hook_checks(event):
            logger.info("Skipping relation broken event: hook checks did not pass")
            return

        if not self.charm.proceed_on_broken_event(event):
            logger.info("Skipping relation broken event, broken event due to scale down")
            return

        # mongos-k8s router is in charge of removing its own users.
        if self.substrate == Config.Substrate.VM:
            self.charm.client_relations.oversee_users(departed_relation_id, event)

    def update_config_server_db(self, event):
        """Provides related mongos applications with new config server db."""
        if not self.pass_hook_checks(event):
            logger.info("Skipping update_config_server_db: hook checks did not pass")
            return

        config_server_db = self.generate_config_server_db()

        if not self.charm.unit.is_leader():
            return

        for relation in self.charm.model.relations[self.relation_name]:
            self.database_provides.update_relation_data(
                relation.id,
                {
                    CONFIG_SERVER_DB_KEY: config_server_db,
                },
            )

    def generate_config_server_db(self) -> str:
        """Generates the config server database for mongos to connect to."""
        replica_set_name = self.charm.app.name
        hosts = []
        for host in self.charm.app_hosts:
            hosts.append(f"{host}:{Config.MONGODB_PORT}")

        hosts = ",".join(hosts)
        return f"{replica_set_name}/{hosts}"

    def update_ca_secret(self, new_ca: str) -> None:
        """Updates the new CA for all related shards."""
        for relation in self.charm.model.relations[self.relation_name]:
            if new_ca is None:
                self.database_provides.delete_relation_data(relation.id, {INT_TLS_CA_KEY: new_ca})
            else:
                self.database_provides.update_relation_data(relation.id, {INT_TLS_CA_KEY: new_ca})


class ClusterRequirer(Object):
    """Manage relations between the config server and mongos router on the mongos side."""

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str = Config.Relations.CLUSTER_RELATIONS_NAME,
        substrate: str = Config.Substrate.VM,
    ) -> None:
        """Constructor for ShardingProvider object."""
        self.substrate = substrate
        self.relation_name = relation_name
        self.charm = charm
        self.database_requires = DatabaseRequires(
            self.charm,
            relation_name=self.relation_name,
            relations_aliases=[self.relation_name],
            database_name=self.charm.database,
            extra_user_roles=self.charm.extra_user_roles,
            additional_secret_fields=[KEYFILE_KEY, INT_TLS_CA_KEY],
        )

        super().__init__(charm, self.relation_name)
        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._on_relation_created_handler,
        )

        self.framework.observe(
            self.database_requires.on.database_created, self._on_database_created
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed, self._on_relation_changed
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_departed,
            self.charm.check_relation_broken_or_scale_down,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken, self._on_relation_broken
        )

    def _on_relation_created_handler(self, event: RelationCreatedEvent) -> None:
        logger.info("Integrating to config-server")
        self.charm.status.set_and_share_status(WaitingStatus("Connecting to config-server"))
        self.database_requires._on_relation_created_event(event)

    def _on_database_created(self, event) -> None:
        if self.charm.upgrade_in_progress:
            logger.warning(
                "Processing client applications is not supported during an upgrade. The charm may be in a broken, unrecoverable state."
            )
            event.defer()
            return

        if not self.charm.unit.is_leader():
            return

        logger.info("Database and user created for mongos application")
        self.charm.set_secret(Config.Relations.APP_SCOPE, Config.Secrets.USERNAME, event.username)
        self.charm.set_secret(Config.Relations.APP_SCOPE, Config.Secrets.PASSWORD, event.password)

        # K8s charm have a 1:Many client scheme and share connection info in a different manner.
        if self.substrate == Config.Substrate.VM:
            self.charm.share_connection_info()

    def _on_relation_changed(self, event) -> None:
        """Starts/restarts monogs with config server information."""
        if not self.pass_hook_checks(event):
            logger.info("pre-hook checks did not pass, not executing event")
            return

        key_file_contents = self.database_requires.fetch_relation_field(
            event.relation.id, KEYFILE_KEY
        )
        config_server_db_uri = self.database_requires.fetch_relation_field(
            event.relation.id, CONFIG_SERVER_DB_KEY
        )
        if not key_file_contents or not config_server_db_uri:
            self.charm.status.set_and_share_status(
                WaitingStatus("Waiting for secrets from config-server")
            )
            return

        updated_keyfile = self.update_keyfile(key_file_contents=key_file_contents)
        updated_config = self.update_config_server_db(config_server_db=config_server_db_uri)

        # avoid restarting mongos when possible
        if not updated_keyfile and not updated_config and self.is_mongos_running():
            # mongos-k8s router must update its users on start
            self._update_k8s_users(event)
            return

        # mongos is not available until it is using new secrets
        logger.info("Restarting mongos with new secrets")
        self.charm.status.set_and_share_status(MaintenanceStatus("starting mongos"))
        self.charm.restart_charm_services()

        # restart on high loaded databases can be very slow (e.g. up to 10-20 minutes).
        if not self.is_mongos_running():
            logger.info("mongos has not started, deferring")
            self.charm.status.set_and_share_status(WaitingStatus("Waiting for mongos to start"))
            event.defer()
            return

        self.charm.status.set_and_share_status(ActiveStatus())
        if self.charm.unit.is_leader():
            self.charm.mongos_initialised = True

        # mongos-k8s router must update its users on start
        self._update_k8s_users(event)

    def _update_k8s_users(self, event) -> None:
        if self.substrate != Config.Substrate.K8S:
            return

        # K8s can handle its 1:Many users after being initialized
        try:
            self.charm.client_relations.oversee_users(None, None)
        except PyMongoError:
            event.defer()
            logger.debug("failed to add users on mongos-k8s router, will defer and try again.")

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        # Only relation_deparated events can check if scaling down
        if not self.charm.has_departed_run(event.relation.id):
            logger.info(
                "Deferring, must wait for relation departed hook to decide if relation should be removed."
            )
            event.defer()
            return

        if not self.charm.proceed_on_broken_event(event):
            logger.info("Skipping relation broken event, broken event due to scale down")
            return

        try:
            self.handle_mongos_k8s_users_removal()
        except PyMongoError:
            logger.debug("Trouble removing router users, will defer and try again")
            event.defer()
            return

        self.charm.stop_mongos_service()
        logger.info("Stopped mongos daemon")

        if not self.charm.unit.is_leader():
            return

        logger.info("Database and user removed for mongos application")
        self.charm.remove_secret(Config.Relations.APP_SCOPE, Config.Secrets.USERNAME)
        self.charm.remove_secret(Config.Relations.APP_SCOPE, Config.Secrets.PASSWORD)

        # K8s charm have a 1:Many client scheme and share connection info in a different manner.
        if self.substrate == Config.Substrate.VM:
            self.charm.remove_connection_info()
        else:
            self.charm.db_initialised = False

    # BEGIN: helper functions
    def handle_mongos_k8s_users_removal(self) -> None:
        """Handles the removal of all client mongos-k8s users and the mongos-k8s admin user.

        Raises:
            PyMongoError
        """
        if not self.charm.unit.is_leader() or self.substrate != Config.Substrate.K8S:
            return

        self.charm.client_relations.remove_all_relational_users()

        # now that the client mongos users have been removed we can remove ourself
        with MongoConnection(self.charm.mongo_config) as mongo:
            mongo.drop_user(self.charm.mongo_config.username)

    def pass_hook_checks(self, event):
        """Runs the pre-hooks checks for ClusterRequirer, returns True if all pass."""
        if self.is_mongos_tls_missing():
            logger.info(
                "Deferring %s. Config-server uses TLS, but mongos does not. Please synchronise encryption methods.",
                str(type(event)),
            )
            event.defer()
            return False

        if self.is_config_server_tls_missing():
            logger.info(
                "Deferring %s. mongos uses TLS, but config-server does not. Please synchronise encryption methods.",
                str(type(event)),
            )
            event.defer()
            return False

        # race condition where mongos cannot start without TLS certificates, but mongos cannot
        # request TLS certificates without knowing the name of the config-server.
        if self.is_waiting_to_request_certs():
            logger.info(
                "Mongos was waiting for config-server to enable TLS. Wait for TLS to be enabled until starting mongos."
            )
            event.defer()
            return False

        if self.charm.upgrade_in_progress:
            logger.warning(
                "Processing client applications is not supported during an upgrade. The charm may be in a broken, unrecoverable state."
            )
            event.defer()
            return False

        return True

    def is_mongos_running(self) -> bool:
        """Returns true if mongos service is running."""
        connection_uri = f"mongodb://{self.charm.get_mongos_host()}"

        # use the mongos port for k8s charms and external connections on VM
        if self.substrate == Config.Substrate.K8S or self.charm.is_external_client:
            connection_uri = connection_uri + f":{Config.MONGOS_PORT}"

        with MongosConnection(None, connection_uri) as mongo:
            return mongo.is_ready

    def update_config_server_db(self, config_server_db) -> bool:
        """Updates config server str when necessary."""
        if self.charm.config_server_db == config_server_db:
            return False

        if self.substrate == Config.Substrate.VM:
            self.charm.update_mongos_args(config_server_db)

        return True

    def update_keyfile(self, key_file_contents: str) -> bool:
        """Updates keyfile when necessary."""
        # keyfile is set by leader in application data, application data does not necessarily
        # match what is on the machine.
        current_key_file = self.charm.get_keyfile_contents()
        if not key_file_contents or key_file_contents == current_key_file:
            return False

        # put keyfile on the machine with appropriate permissions
        self.charm.push_file_to_unit(
            parent_dir=Config.MONGOD_CONF_DIR,
            file_name=KEY_FILE,
            file_contents=key_file_contents,
        )

        if self.charm.unit.is_leader():
            self.charm.set_secret(
                Config.Relations.APP_SCOPE,
                Config.Secrets.SECRET_KEYFILE_NAME,
                key_file_contents,
            )

        return True

    def get_tls_statuses(self) -> Optional[StatusBase]:
        """Returns statuses relevant to TLS."""
        if self.is_mongos_tls_missing():
            return BlockedStatus("mongos requires TLS to be enabled.")

        if self.is_config_server_tls_missing():
            return BlockedStatus("mongos has TLS enabled, but config-server does not.")

        if not self.is_ca_compatible():
            logger.error(
                "mongos is integrated to a different CA than the config server. Please use the same CA for all cluster components."
            )
            return BlockedStatus("mongos CA and Config-Server CA don't match.")

        return None

    def get_config_server_name(self) -> Optional[str]:
        """Returns the name of the Juju Application that mongos is using as a config server."""
        if not self.model.get_relation(self.relation_name):
            return None

        # metadata.yaml prevents having multiple config servers
        return self.model.get_relation(self.relation_name).app.name

    def get_config_server_uri(self) -> str:
        """Returns the short form URI of the config server."""
        return self.database_requires.fetch_relation_field(
            self.model.get_relation(Config.Relations.CLUSTER_RELATIONS_NAME).id,
            CONFIG_SERVER_DB_KEY,
        )

    def is_waiting_to_request_certs(self) -> bool:
        """Returns True if mongos has been waiting for config server in order to request certs."""
        if not self.charm.model.get_relation(Config.TLS.TLS_PEER_RELATION):
            return False

        mongos_tls_ca = self.charm.tls.get_tls_secret(
            internal=True, label_name=Config.TLS.SECRET_CA_LABEL
        )

        # our CA is none until certs have been requested. We cannot request certs until integrated
        # to config-server.
        if not mongos_tls_ca:
            return True

        return False

    def is_ca_compatible(self) -> bool:
        """Returns true if both the mongos and the config server use the same CA."""
        config_server_relation = self.charm.model.get_relation(self.relation_name)
        # base-case: nothing to compare
        if not config_server_relation:
            return True

        config_server_tls_ca = self.database_requires.fetch_relation_field(
            config_server_relation.id, INT_TLS_CA_KEY
        )

        mongos_tls_ca = self.charm.tls.get_tls_secret(
            internal=True, label_name=Config.TLS.SECRET_CA_LABEL
        )

        # base-case: missing one or more CA's to compare
        if not config_server_tls_ca and not mongos_tls_ca:
            return True

        return config_server_tls_ca == mongos_tls_ca

    def is_mongos_tls_missing(self) -> bool:
        """Returns true if the config-server has TLS enabled but mongos does not."""
        config_server_relation = self.charm.model.get_relation(self.relation_name)
        if not config_server_relation:
            return False

        mongos_has_tls = self.charm.model.get_relation(Config.TLS.TLS_PEER_RELATION) is not None
        config_server_has_tls = (
            self.database_requires.fetch_relation_field(config_server_relation.id, INT_TLS_CA_KEY)
            is not None
        )
        if config_server_has_tls and not mongos_has_tls:
            return True

        return False

    def is_config_server_tls_missing(self) -> bool:
        """Returns true if the mongos has TLS enabled but the config-server does not."""
        config_server_relation = self.charm.model.get_relation(self.relation_name)
        if not config_server_relation:
            return False

        mongos_has_tls = self.charm.model.get_relation(Config.TLS.TLS_PEER_RELATION) is not None
        config_server_has_tls = (
            self.database_requires.fetch_relation_field(config_server_relation.id, INT_TLS_CA_KEY)
            is not None
        )
        if not config_server_has_tls and mongos_has_tls:
            return True

        return False

    # END: helper functions
