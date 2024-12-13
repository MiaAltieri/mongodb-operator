#!/usr/bin/env python3
"""Charm code for MongoDB service."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import os
import pwd
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.mongodb.v0.config_server_interface import ClusterProvider
from charms.mongodb.v0.mongo import MongoConfiguration
from charms.mongodb.v0.mongodb_secrets import SecretCache, generate_secret_label
from charms.mongodb.v0.set_status import MongoDBStatusHandler
from charms.mongodb.v1.helpers import (
    KEY_FILE,
    TLS_EXT_CA_FILE,
    TLS_EXT_PEM_FILE,
    TLS_INT_CA_FILE,
    TLS_INT_PEM_FILE,
    copy_licenses_to_unit,
    generate_keyfile,
    generate_password,
    get_create_user_cmd,
    safe_exec,
)
from charms.mongodb.v1.mongodb import MongoDBConnection, NotReadyError
from charms.mongodb.v1.mongodb_backups import MongoDBBackups
from charms.mongodb.v1.mongodb_provider import MongoDBProvider
from charms.mongodb.v1.mongodb_tls import MongoDBTLS
from charms.mongodb.v1.shards_interface import ConfigServerRequirer, ShardingProvider
from charms.mongodb.v1.users import (
    CHARM_USERS,
    BackupUser,
    MongoDBUser,
    MonitorUser,
    OperatorUser,
)
from charms.operator_libs_linux.v0 import sysctl
from charms.operator_libs_linux.v1.systemd import service_running
from charms.operator_libs_linux.v2 import snap
from data_platform_helpers.version_check import (
    CrossAppVersionChecker,
    get_charm_revision,
)
from ops import StorageAttachedEvent
from ops.charm import (
    ActionEvent,
    CharmBase,
    ConfigChangedEvent,
    InstallEvent,
    LeaderElectedEvent,
    RelationDepartedEvent,
    RelationEvent,
    RelationJoinedEvent,
    SecretChangedEvent,
    SecretRemoveEvent,
    StartEvent,
    StorageDetachingEvent,
    UpdateStatusEvent,
)
from ops.main import main
from ops.model import (
    ActiveStatus,
    Application,
    BlockedStatus,
    MaintenanceStatus,
    Relation,
    Unit,
    WaitingStatus,
)
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from tenacity import Retrying, before_log, retry, stop_after_attempt, wait_fixed

from config import Config, Package
from exceptions import (
    AdminUserCreationError,
    ApplicationHostNotFoundError,
    NotConfigServerError,
)
from machine_helpers import (
    MONGO_USER,
    ROOT_USER_GID,
    setup_logrotate_and_cron,
    update_mongod_service,
)
from upgrades.mongodb_upgrade import MongoDBUpgrade

logger = logging.getLogger(__name__)

APP_SCOPE = Config.Relations.APP_SCOPE
UNIT_SCOPE = Config.Relations.UNIT_SCOPE
Scopes = Config.Relations.Scopes


class MongodbOperatorCharm(CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self._port = Config.MONGODB_PORT

        # lifecycle events
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(self.on.mongodb_storage_attached, self._on_storage_attached)
        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_joined, self._on_relation_joined
        )
        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_changed, self._on_relation_handler
        )
        self.framework.observe(
            self.on[Config.Relations.PEERS].relation_departed, self._on_relation_departed
        )

        # if a new leader has been elected update hosts of MongoDB
        self.framework.observe(self.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self.on.mongodb_storage_detaching, self._on_storage_detaching)

        # actions
        self.framework.observe(self.on.get_primary_action, self._on_get_primary_action)
        self.framework.observe(self.on.get_password_action, self._on_get_password)
        self.framework.observe(self.on.set_password_action, self._on_set_password)

        # secrets
        self.framework.observe(self.on.secret_remove, self._on_secret_remove)
        self.framework.observe(self.on.secret_changed, self._on_secret_changed)

        # handle provider side of relations
        self.client_relations = MongoDBProvider(self, substrate=Config.SUBSTRATE)
        self.tls = MongoDBTLS(self, Config.Relations.PEERS, substrate=Config.SUBSTRATE)
        self.backups = MongoDBBackups(self)

        self.version_checker = CrossAppVersionChecker(
            self,
            version=get_charm_revision(self.unit, local_version=self.get_charm_internal_revision),
            relations_to_check=[
                Config.Relations.SHARDING_RELATIONS_NAME,
                Config.Relations.CONFIG_SERVER_RELATIONS_NAME,
            ],
        )
        self.upgrade = MongoDBUpgrade(self)
        self.config_server = ShardingProvider(self, substrate=Config.SUBSTRATE)
        self.cluster = ClusterProvider(self)
        self.shard = ConfigServerRequirer(self)
        self.status = MongoDBStatusHandler(self)

        # relation events for Prometheus metrics are handled in the MetricsEndpointProvider
        self._grafana_agent = COSAgentProvider(
            self,
            metrics_rules_dir=Config.Monitoring.METRICS_RULES_DIR,
            logs_rules_dir=Config.Monitoring.LOGS_RULES_DIR,
            log_slots=Config.Monitoring.LOG_SLOTS,
            scrape_configs=self._mongo_scrape_config,
        )

        self.secrets = SecretCache(self)

        self.sysctl_config = sysctl.Config(name=self.app.name)
        self.framework.observe(getattr(self.on, "remove"), self._on_remove)

    # BEGIN: properties
    def _mongo_scrape_config(self) -> List[Dict]:
        """Generates scrape config for the mongo metrics endpoint."""
        return [
            {
                "metrics_path": "/metrics",
                "static_configs": [
                    {
                        "targets": [
                            f"{self.unit_host(self.unit)}:{Config.Monitoring.MONGODB_EXPORTER_PORT}"
                        ],
                        "labels": {
                            "cluster": self.get_config_server_name() or self.app.name,
                        },
                    }
                ],
            }
        ]

    @property
    def primary(self) -> str:
        """Retrieves the unit with the primary replica."""
        try:
            with MongoDBConnection(self.mongodb_config) as mongo:
                primary_ip = mongo.primary()
        except PyMongoError as e:
            logger.error("Unable to access primary due to: %s", e)
            return None

        # check if current unit matches primary ip
        if primary_ip == self.unit_host(self.unit):
            return self.unit.name

        # check if peer unit matches primary ip
        for unit in self.peers_units:
            if primary_ip == self.unit_host(unit):
                return unit.name

        return None

    @property
    def drained(self) -> bool:
        """Returns whether the shard has been drained."""
        if not self.is_role(Config.Role.SHARD):
            logger.info("Component %s is not a shard, cannot check draining status.", self.role)
            return False

        return self.unit_peer_data.get("drained", False)

    @property
    def app_hosts(self) -> List[str]:
        """Retrieve IP addresses associated with MongoDB application.

        Returns:
            a list of IP address associated with MongoDB application.
        """
        peer_addresses = [self.unit_host(unit) for unit in self.peers_units]

        logger.debug("peer addresses: %s", peer_addresses)
        self_address = self.unit_host(self.unit)
        logger.debug("unit address: %s", self_address)
        addresses = []
        if peer_addresses:
            addresses.extend(peer_addresses)
        addresses.append(self_address)
        return addresses

    @property
    def _replica_set_hosts(self):
        """Fetch current list of hosts in the replica set.

        Returns:
            A list of hosts addresses (strings).
        """
        return json.loads(self.app_peer_data.get("replica_set_hosts", "[]"))

    @property
    def mongos_config(self) -> MongoConfiguration:
        """Generates a MongoConfiguration object for mongos in the deployment of MongoDB."""
        return self._get_mongos_config_for_user(OperatorUser, set(self.app_hosts))

    def remote_mongos_config(self, hosts) -> MongoConfiguration:
        """Generates a MongoConfiguration object for mongos in the deployment of MongoDB."""
        # mongos that are part of the cluster have the same username and password, but different
        # hosts
        return self._get_mongos_config_for_user(OperatorUser, hosts)

    def remote_mongodb_config(self, hosts, replset=None, standalone=None) -> MongoConfiguration:
        """Generates a MongoConfiguration object for mongod in the deployment of MongoDB."""
        # mongos that are part of the cluster have the same username and password, but different
        # hosts
        return self._get_mongodb_config_for_user(
            OperatorUser, hosts, replset=replset, standalone=standalone
        )

    @property
    def mongo_config(self) -> MongoConfiguration:
        """Returns a MongoConfiguration object for shared libs with agnostic mongo commands."""
        return self.mongodb_config

    @property
    def mongodb_config(self) -> MongoConfiguration:
        """Generates a MongoConfiguration object for this deployment of MongoDB."""
        return self._get_mongodb_config_for_user(OperatorUser, set(self.app_hosts))

    @property
    def monitor_config(self) -> MongoConfiguration:
        """Generates a MongoConfiguration object for monitoring."""
        return self._get_mongodb_config_for_user(MonitorUser, MonitorUser.get_hosts())

    @property
    def backup_config(self) -> MongoConfiguration:
        """Generates a MongoConfiguration object for backup."""
        self._check_or_set_user_password(BackupUser)
        return self._get_mongodb_config_for_user(
            BackupUser, BackupUser.get_hosts(), standalone=True
        )

    @property
    def unit_peer_data(self) -> Dict:
        """Peer relation data object."""
        if not self.peers:
            return {}

        return self.peers.data[self.unit]

    @property
    def app_peer_data(self) -> Dict:
        """Peer relation data object."""
        if not self.peers:
            return {}

        return self.peers.data[self.app]

    @property
    def peers(self) -> Optional[Relation]:
        """Fetch the peer relation.

        Returns:
             An `ops.model.Relation` object representing the peer relation.
        """
        return self.model.get_relation(Config.Relations.PEERS)

    @property
    def peers_units(self) -> list[Unit]:
        """Get peers units in a safe way."""
        if not self.peers:
            return []
        else:
            return self.peers.units

    @property
    def db_initialised(self) -> bool:
        """Check if MongoDB is initialised."""
        # Needs to lowercase it so it also work with older versions
        return json.loads(self.app_peer_data.get("db_initialised", "false").lower())

    @property
    def role(self) -> str:
        """Returns role of MongoDB deployment."""
        if (
            "role" not in self.app_peer_data
            and self.unit.is_leader()
            and self.model.config["role"]
        ):
            self.app_peer_data["role"] = self.model.config["role"]
            # app data bag isn't set until function completes
            return self.model.config["role"]
        elif "role" not in self.app_peer_data:
            # if leader hasn't set the role yet, use the one set by model
            return self.model.config["role"]

        return self.app_peer_data.get("role")

    def is_role_changed(self) -> bool:
        """Checks if application is running in provided role."""
        return self.role != self.model.config["role"]

    def is_role(self, role_name: str) -> bool:
        """Checks if application is running in provided role."""
        return self.role == role_name

    @db_initialised.setter
    def db_initialised(self, value):
        """Set the db_initialised flag."""
        if isinstance(value, bool):
            self.app_peer_data["db_initialised"] = json.dumps(value)
        else:
            raise ValueError(
                f"'db_initialised' must be a boolean value. Proivded: {value} is of type {type(value)}"
            )

    @property
    def upgrade_in_progress(self):
        """Whether upgrade is in progress."""
        if not self.upgrade._upgrade:
            return False
        return self.upgrade._upgrade.in_progress

    @property
    def get_charm_internal_revision(self) -> str:
        """Returns the contents of the get_charm_internal_revision file."""
        with open(Config.CHARM_INTERNAL_VERSION_FILE, "r") as f:
            return f.read().strip()

    # END: properties

    # BEGIN: charm event handlers

    def _on_install(self, event: InstallEvent) -> None:
        """Handle the install event (fired on startup)."""
        self._set_os_config()
        self.status.set_and_share_status(MaintenanceStatus("installing MongoDB"))
        try:
            self.install_snap_packages(packages=Config.SNAP_PACKAGES)

        except snap.SnapError:
            self.status.set_and_share_status(BlockedStatus("couldn't install MongoDB"))
            return

        # clear the default config file - user provided config files will be added in the config
        # changed hook
        try:
            with open(Config.MONGOD_CONF_FILE_PATH, "r+") as f:
                f.truncate(0)
        except IOError:
            self.status.set_and_share_status(BlockedStatus("Could not install MongoDB"))
            return

        # Construct the mongod startup commandline args for systemd and reload the daemon.
        update_mongod_service(
            machine_ip=self.unit_host(self.unit),
            config=self.mongodb_config,
            role=self.role,
        )
        setup_logrotate_and_cron()
        # add licenses
        copy_licenses_to_unit()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        """Listen to changes in application configuration.

        To prevent a user from migrating a cluster, and causing the component to become
        unresponsive therefore causing a cluster failure, error the component. This prevents it
        from executing other hooks with a new role.
        """
        if self.is_role_changed():

            if self.upgrade_in_progress:
                logger.warning(
                    "Changing config options is not permitted during an upgrade. The charm may be in a broken, unrecoverable state."
                )
                event.defer()
                return

            # TODO in the future (24.04) support migration of components
            logger.error(
                f"cluster migration currently not supported, cannot change from {self.model.config['role']} to {self.role}"
            )
            raise ShardingMigrationError(
                f"Migration of sharding components not permitted, revert config role to {self.role}"
            )

    def _on_start(self, event: StartEvent) -> None:
        """Enables MongoDB service and initialises replica set.

        Args:
            event: The triggering start event.
        """
        # mongod requires keyFile and TLS certificates on the file system
        self._instatiate_keyfile(event)
        self.push_tls_certificate_to_workload()

        try:
            logger.debug("starting MongoDB.")
            self.status.set_and_share_status(MaintenanceStatus("starting MongoDB"))
            self.start_charm_services()
            self.status.set_and_share_status(ActiveStatus())
        except snap.SnapError as e:
            logger.error("An exception occurred when starting mongod agent, error: %s.", str(e))
            self.status.set_and_share_status(BlockedStatus("couldn't start MongoDB"))
            return

        try:
            ports = [self._port]
            if self.is_role(Config.Role.CONFIG_SERVER):
                ports.append(Config.MONGOS_PORT)

            self._open_ports_tcp(ports)
        except subprocess.CalledProcessError:
            self.status.set_and_share_status(BlockedStatus("failed to open TCP port for MongoDB"))
            return

        # check if this unit's deployment of MongoDB is ready
        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            if not direct_mongo.is_ready:
                logger.debug("mongodb service is not ready yet.")
                self.status.set_and_share_status(WaitingStatus("waiting for MongoDB to start"))
                event.defer()
                return

        # mongod is now active
        self.status.set_and_share_status(ActiveStatus())

        try:
            self._connect_mongodb_exporter()
        except snap.SnapError as e:
            logger.error(
                "An exception occurred when starting mongodb exporter, error: %s.", str(e)
            )
            self.status.set_and_share_status(BlockedStatus("couldn't start mongodb exporter"))
            return

        # only leader should initialise the replica set
        if not self.unit.is_leader():
            return

        self._initialise_replica_set(event)

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        """Add peer to replica set.

        Args:
            event: The triggering relation joined event.
        """
        if not self.unit.is_leader():
            return

        if self.upgrade_in_progress:
            logger.warning(
                "Adding replicas during an upgrade is not supported. The charm may be in a broken, unrecoverable state"
            )
            event.defer()
            return

        self._on_relation_handler(event)

        self._update_related_hosts(event)

    def _on_relation_handler(self, event: RelationEvent) -> None:
        """Adds the unit as a replica to the MongoDB replica set.

        Args:
            event: The triggering relation joined/changed event.
        """
        if self.upgrade_in_progress:
            logger.warning(
                "Adding/Removing/Changing replicas during an upgrade is not supported. The charm may be in a broken, unrecoverable state"
            )
            event.defer()
            return

        # changing the monitor password will lead to non-leader units receiving a relation changed
        # event. We must update the monitor and pbm URI if the password changes so that COS/pbm
        # can continue to work
        self._connect_mongodb_exporter()
        self._connect_pbm_agent()

        # only leader should configure replica set and app-changed-events can trigger the relation
        # changed hook resulting in no JUJU_REMOTE_UNIT if this is the case we should return
        # further reconfiguration can be successful only if a replica set is initialised.
        if not (self.unit.is_leader() and event.unit) or not self.db_initialised:
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members()
                # compare set of mongod replica set members and juju hosts to avoid the unnecessary
                # reconfiguration.
                if replset_members == self.mongodb_config.hosts:
                    return

                for member in self.mongodb_config.hosts - replset_members:
                    logger.debug("Adding %s to replica set", member)
                    with MongoDBConnection(
                        self.mongodb_config, member, direct=True
                    ) as direct_mongo:
                        if not direct_mongo.is_ready:
                            self.status.set_and_share_status(
                                WaitingStatus("waiting to reconfigure replica set")
                            )
                            logger.debug("Deferring reconfigure: %s is not ready yet.", member)
                            event.defer()
                            return
                    mongo.add_replset_member(member)
                    self.status.set_and_share_status(ActiveStatus())
            except NotReadyError:
                self.status.set_and_share_status(
                    WaitingStatus("waiting to reconfigure replica set")
                )
                logger.error("Deferring reconfigure: another member doing sync right now")
                event.defer()
            except PyMongoError as e:
                self.status.set_and_share_status(
                    WaitingStatus("waiting to reconfigure replica set")
                )
                logger.error("Deferring reconfigure: error=%r", e)
                event.defer()

    def _on_leader_elected(self, event: LeaderElectedEvent) -> None:
        """Generates necessary keyfile and updates replica hosts."""
        if not self.get_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME):
            self._generate_secrets()

        self._update_hosts(event)

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Remove peer from replica set if it wasn't able to remove itself.

        Args:
            event: The triggering relation departed event.
        """
        # allow leader to update relation data and hosts if it isn't leaving
        if not self.unit.is_leader() or event.departing_unit == self.unit:
            return

        if self.upgrade_in_progress:
            # do not defer or return here, if a user removes a unit, the config will be incorrect
            # and lead to MongoDB reporting that the replica set is unhealthy, we should make an
            # attempt to fix the replica set configuration even if an upgrade is occurring.
            logger.warning(
                "Removing replicas during an upgrade is not supported. The charm may be in a broken, unrecoverable state"
            )

        self._update_hosts(event)

    def _on_storage_attached(self, event: StorageAttachedEvent) -> None:
        """Handler for `storage_attached` event.

        This should handle fixing the permissions for the data dir.
        """
        safe_exec(f"chmod -R 770 {Config.MONGODB_COMMON_PATH}".split())
        safe_exec(f"chown -R {Config.SNAP_USER}:root  {Config.MONGODB_COMMON_PATH}".split())

    def _on_storage_detaching(self, event: StorageDetachingEvent) -> None:
        """Before storage detaches, allow removing unit to remove itself from the set.

        If the removing unit is primary also allow it to step down and elect another unit as
        primary while it still has access to its storage.
        """
        if self.upgrade_in_progress:
            # We cannot defer and prevent a user from removing a unit, log a warning instead.
            logger.warning(
                "Removing replicas during an upgrade is not supported. The charm may be in a broken, unrecoverable state"
            )

        # A single replica cannot step down as primary and we cannot reconfigure the replica set to
        # have 0 members.
        if self._is_removing_last_replica:
            # removing config-server from a sharded cluster can be disaterous.
            if self.is_role(Config.Role.CONFIG_SERVER) and self.config_server.has_shards():
                current_shards = self.config_server.get_related_shards()
                early_removal_message = f"Cannot remove config-server, still related to shards {', '.join(current_shards)}"
                logger.error(early_removal_message)
                raise EarlyRemovalOfConfigServerError(early_removal_message)

            # cannot drain shard after storage detached.
            if self.is_role(Config.Role.SHARD) and self.shard.has_config_server():
                logger.info("Wait for shard to drain before detaching storage.")
                self.status.set_and_share_status(MaintenanceStatus("Draining shard from cluster"))
                mongos_hosts = self.shard.get_mongos_hosts()
                self.shard.wait_for_draining(mongos_hosts)
                logger.info("Shard successfully drained storage.")

            return

        try:
            # retries over a period of 10 minutes in an attempt to resolve race conditions it is
            # not possible to defer in storage detached.
            logger.debug("Removing %s from replica set", self.unit_host(self.unit))
            for attempt in Retrying(
                stop=stop_after_attempt(10),
                wait=wait_fixed(1),
                reraise=True,
            ):
                with attempt:
                    # remove_replset_member retries for 60 seconds
                    with MongoDBConnection(self.mongodb_config) as mongo:
                        mongo.remove_replset_member(self.unit_host(self.unit))
        except NotReadyError:
            logger.info(
                "Failed to remove %s from replica set, another member is syncing", self.unit.name
            )
        except PyMongoError as e:
            logger.error("Failed to remove %s from replica set, error=%r", self.unit.name, e)

    def _on_remove(self, _) -> None:
        """Handler for removal."""
        self.sysctl_config.remove()

    def _on_update_status(self, event: UpdateStatusEvent):
        # user-made mistakes might result in other incorrect statues. Prioritise informing users of
        # their mistake.
        invalid_integration_status = self.status.get_invalid_integration_status()
        if invalid_integration_status:
            self.status.set_and_share_status(invalid_integration_status)
            return

        # no need to report on replica set status until initialised
        if not self.db_initialised:
            return

        # Cannot check more advanced MongoDB statuses if mongod hasn't started.
        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            if not direct_mongo.is_ready:
                # edge case: mongod will fail to run if 1. they are running as shard and 2. they
                # have already been added to the cluster with internal membership via TLS and 3.
                # they remove support for TLS
                if self.is_role(Config.Role.SHARD) and self.shard.is_shard_tls_missing():
                    self.status.set_and_share_status(
                        BlockedStatus("Shard requires TLS to be enabled.")
                    )
                    return
                else:
                    self.status.set_and_share_status(WaitingStatus("Waiting for MongoDB to start"))
                    return

        try:
            self.perform_self_healing(event)
        except ServerSelectionTimeoutError:
            # health checks that are performed too early will fail if the hasn't elected a primary
            # yet. This can occur if the deployment has restarted or is undergoing a re-election
            deployment_mode = "replica set" if self.is_role(Config.Role.REPLICATION) else "cluster"
            WaitingStatus(f"Waiting to sync internal membership across the {deployment_mode}")

        self.status.set_and_share_status(self.status.process_statuses())

    def _on_get_primary_action(self, event: ActionEvent):
        event.set_results({"replica-set-primary": self.primary})

    def _on_get_password(self, event: ActionEvent) -> None:
        """Returns the password for the user as an action response."""
        username = self._get_user_or_fail_event(
            event, default_username=OperatorUser.get_username()
        )
        if not username:
            return
        key_name = MongoDBUser.get_password_key_name_for_user(username)
        event.set_results(
            {Config.Actions.PASSWORD_PARAM_NAME: self.get_secret(APP_SCOPE, key_name)}
        )

    def _on_set_password(self, event: ActionEvent) -> None:
        """Set the password for the admin user."""
        # check conditions for setting the password and fail if necessary
        if not self.pass_pre_set_password_checks(event):
            return

        username = self._get_user_or_fail_event(
            event, default_username=OperatorUser.get_username()
        )
        if not username:
            return

        new_password = event.params.get(Config.Actions.PASSWORD_PARAM_NAME, generate_password())
        if len(new_password) > Config.Secrets.MAX_PASSWORD_LENGTH:
            event.fail(
                f"Password cannot be longer than {Config.Secrets.MAX_PASSWORD_LENGTH} characters."
            )
            return

        try:
            secret_id = self.set_password(username, new_password)
        except SetPasswordError as e:
            event.fail(str(e))
            return

        if username == BackupUser.get_username():
            self._connect_pbm_agent()

        if username == MonitorUser.get_username():
            self._connect_mongodb_exporter()

        # rotate password to shards
        # TODO in the future support rotating passwords of pbm across shards
        if username in [OperatorUser.get_username(), BackupUser.get_username()]:
            self.config_server.update_credentials(
                MongoDBUser.get_password_key_name_for_user(username),
                new_password,
            )

        event.set_results(
            {Config.Actions.PASSWORD_PARAM_NAME: new_password, "secret-id": secret_id}
        )

    def set_password(self, username, password) -> int:
        """Sets the password for a given username and return the secret id.

        Raises:
            SetPasswordError
        """
        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                mongo.set_user_password(username, password)
            except NotReadyError:
                raise SetPasswordError(
                    "Failed changing the password: Not all members healthy or finished initial sync."
                )
            except PyMongoError as e:
                raise SetPasswordError(f"Failed changing the password: {e}")

        return self.set_secret(
            APP_SCOPE, MongoDBUser.get_password_key_name_for_user(username), password
        )

    def _on_secret_remove(self, event: SecretRemoveEvent):
        # We are keeping this function empty on purpose until the issue with secrets
        # is not fixed. The issue is: https://bugs.launchpad.net/juju/+bug/2023364
        logging.error(
            f"_on_secret_remove: Secret {event._id} seems to have no observers, could be removed"
        )

    def _on_secret_changed(self, event: SecretChangedEvent):
        """Handles secrets changes event.

        When user run set-password action, juju leader changes the password inside the database
        and inside the secret object. This action runs the restart for monitoring tool and
        for backup tool on non-leader units to keep them working with MongoDB. The same workflow
        occurs on TLS certs change.
        """
        label = None
        if generate_secret_label(self, Config.Relations.APP_SCOPE) == event.secret.label:
            label = generate_secret_label(self, Config.Relations.APP_SCOPE)
            scope = APP_SCOPE
        elif generate_secret_label(self, Config.Relations.UNIT_SCOPE) == event.secret.label:
            label = generate_secret_label(self, Config.Relations.UNIT_SCOPE)
            scope = UNIT_SCOPE
        else:
            logging.debug("Secret %s changed, but it's unknown", event.secret.id)
            return
        logging.debug("Secret %s for scope %s changed, refreshing", event.secret.id, scope)

        # Refreshing cache
        self.secrets.get(label)

        # changed secrets means that the URIs used for PBM and mongodb_exporter are now out of date
        self._connect_mongodb_exporter()
        self._connect_pbm_agent()

    # END: charm event handlers

    # BEGIN: users management

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_operator_user(self) -> None:
        """Creates initial admin user for MongoDB.

        Initial admin user can be created only through localhost connection.
        see https://www.mongodb.com/docs/manual/core/localhost-exception/
        unfortunately, pymongo unable to create connection that considered
        as local connection by MongoDB, even if socket connection used.
        As a result, where are only hackish ways to create initial user.
        It is needed to install mongodb-clients inside charm container to make
        this function work correctly.
        """
        if self._is_user_created(OperatorUser) or not self.unit.is_leader():
            return

        out = subprocess.run(
            get_create_user_cmd(self.mongodb_config),
            input=self.mongodb_config.password.encode(),
        )
        if out.returncode == 0:
            raise AdminUserCreationError

        logger.debug(f"{OperatorUser.get_username()} user created")
        self._set_user_created(OperatorUser)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_monitor_user(self):
        """Creates the monitor user on the MongoDB database."""
        if self._is_user_created(MonitorUser):
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            logger.debug("creating the monitor user roles...")
            mongo.create_role(
                role_name=MonitorUser.get_mongodb_role(), privileges=MonitorUser.get_privileges()
            )
            logger.debug("creating the monitor user...")
            mongo.create_user(self.monitor_config)
            self._set_user_created(MonitorUser)

        # leader should reconnect to exporter after creating the monitor user - since the snap
        # will have an authorisation error until the the user has been created and the daemon
        # has been restarted
        self._connect_mongodb_exporter()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
    )
    def _init_backup_user(self):
        """Creates the backup user on the MongoDB database."""
        if self._is_user_created(BackupUser):
            return

        with MongoDBConnection(self.mongodb_config) as mongo:
            # first we must create the necessary roles for the PBM tool
            logger.debug("creating the backup user roles...")
            mongo.create_role(
                role_name=BackupUser.get_mongodb_role(), privileges=BackupUser.get_privileges()
            )
            logger.debug("creating the backup user...")
            mongo.create_user(self.backup_config)
            self._set_user_created(BackupUser)

    # END: users management

    # BEGIN: helper functions
    def _is_user_created(self, user: MongoDBUser) -> bool:
        return f"{user.get_username()}-user-created" in self.app_peer_data

    def _set_user_created(self, user: MongoDBUser) -> None:
        self.app_peer_data[f"{user.get_username()}-user-created"] = "True"

    def _get_mongos_config_for_user(
        self, user: MongoDBUser, hosts: Set[str]
    ) -> MongoConfiguration:
        external_ca, _ = self.tls.get_tls_files(internal=False)
        internal_ca, _ = self.tls.get_tls_files(internal=True)

        return MongoConfiguration(
            database=user.get_database_name(),
            username=user.get_username(),
            password=self.get_secret(APP_SCOPE, user.get_password_key_name()),
            hosts=hosts,
            port=Config.MONGOS_PORT,
            roles=user.get_roles(),
            tls_external=external_ca is not None,
            tls_internal=internal_ca is not None,
        )

    def _get_mongodb_config_for_user(
        self, user: MongoDBUser, hosts: Set[str], standalone: bool = False, replset: str = None
    ) -> MongoConfiguration:
        external_ca, _ = self.tls.get_tls_files(internal=False)
        internal_ca, _ = self.tls.get_tls_files(internal=True)

        return MongoConfiguration(
            replset=replset or self.app.name,
            database=user.get_database_name(),
            username=user.get_username(),
            password=self.get_secret(APP_SCOPE, user.get_password_key_name()),
            hosts=hosts,
            roles=user.get_roles(),
            tls_external=external_ca is not None,
            tls_internal=internal_ca is not None,
            standalone=standalone,
        )

    def _get_user_or_fail_event(self, event: ActionEvent, default_username: str) -> Optional[str]:
        """Returns MongoDBUser object or raises ActionFail if user doesn't exist."""
        username = event.params.get(Config.Actions.USERNAME_PARAM_NAME, default_username)
        if username not in CHARM_USERS:
            event.fail(
                f"The action can be run only for users used by the charm:"
                f" {', '.join(CHARM_USERS)} not {username}"
            )
            return
        return username

    def pass_pre_set_password_checks(self, event: ActionEvent) -> bool:
        """Checks conditions for setting the password and fail if necessary."""
        if self.is_role(Config.Role.SHARD):
            event.fail("Cannot set password on shard, please set password on config-server.")
            return False

        # changing the backup password while a backup/restore is in progress can be disastrous
        pbm_status = self.backups.get_pbm_status()
        if isinstance(pbm_status, MaintenanceStatus):
            event.fail("Cannot change password while a backup/restore is in progress.")
            return False

        # only leader can write the new password into peer relation.
        if not self.unit.is_leader():
            event.fail("The action can be run only on leader unit.")
            return False

        if self.upgrade_in_progress:
            logger.debug("Do not set the password while a backup/restore is in progress.")
            event.fail("Cannot set passwords while an upgrade is in progress.")
            return False

        return True

    def _check_or_set_user_password(self, user: MongoDBUser) -> None:
        key = user.get_password_key_name()
        if not self.get_secret(APP_SCOPE, key):
            self.set_secret(APP_SCOPE, key, generate_password())

    def _generate_secrets(self) -> None:
        """Generate secrets and put them into peer relation.

        The same keyFile and admin password on all members needed, hence it is generated once and
        share between members via the app data.
        """
        self._check_or_set_user_password(OperatorUser)
        self._check_or_set_user_password(MonitorUser)

        if not self.get_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME):
            self.set_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME, generate_keyfile())

    def _update_hosts(self, event: LeaderElectedEvent) -> None:
        """Update replica set hosts and remove any unremoved replicas from the config."""
        if not self.db_initialised:
            return

        self.process_unremoved_units(event)
        self.app_peer_data["replica_set_hosts"] = json.dumps(self.app_hosts)

        self._update_related_hosts(event)

    def _update_related_hosts(self, event) -> None:
        # app relations should be made aware of the new set of hosts
        try:
            if not self.is_role(Config.Role.SHARD):
                self.client_relations.update_app_relation_data()
            self.config_server.update_mongos_hosts()
            self.cluster.update_config_server_db(event)
        except PyMongoError as e:
            logger.error("Deferring on updating app relation data since: error: %r", e)
            event.defer()
            return

    def process_unremoved_units(self, event: LeaderElectedEvent) -> None:
        """Removes replica set members that are no longer running as a juju hosts."""
        with MongoDBConnection(self.mongodb_config) as mongo:
            try:
                replset_members = mongo.get_replset_members()
                for member in replset_members - self.mongodb_config.hosts:
                    logger.debug("Removing %s from replica set", member)
                    mongo.remove_replset_member(member)
            except NotReadyError:
                logger.info("Deferring process_unremoved_units: another member is syncing")
                event.defer()
            except PyMongoError as e:
                logger.error("Deferring process_unremoved_units: error=%r", e)
                event.defer()

    def perform_self_healing(self, event: UpdateStatusEvent):
        """Reconfigures the replica set if necessary.

        Incidents such as network cuts can lead to new IP addresses and therefore will require a
        reconfigure. Especially in the case that the leader's IP address changed, it will not
        receive a relation event.
        """
        if not self.unit.is_leader():
            logger.debug("only the leader can perform reconfigurations to the replica set.")
            return

        # remove any IPs that are no longer juju hosts & update app data.
        self._update_hosts(event)

        # Add in any new IPs to the replica set. Relation handlers require a reference to
        # a unit.
        event.unit = self.unit
        self._on_relation_handler(event)

        # make sure all nodes in the replica set have the same priority for re-election. This is
        # necessary in the case that pre-upgrade hook fails to reset the priority of election for
        # cluster nodes.
        with MongoDBConnection(self.mongodb_config) as mongod:
            mongod.set_replicaset_election_priority(priority=1)

    def _open_ports_tcp(self, ports: int) -> None:
        """Open the given port.

        Args:
            ports: The ports to open.
        """
        for port in ports:
            try:
                logger.debug("opening tcp port")
                subprocess.check_call(["open-port", "{}/TCP".format(port)])
            except subprocess.CalledProcessError as e:
                logger.exception("failed opening port: %s", str(e))
                raise

    def install_snap_packages(self, packages: List[Package]) -> None:
        """Installs package(s) to container.

        Args:
            packages: list of packages to install.
        """
        for snap_name, snap_channel, snap_revision in packages:
            try:
                snap_cache = snap.SnapCache()
                snap_package = snap_cache[snap_name]
                snap_package.ensure(snap.SnapState.Latest, revision=snap_revision)
                # snaps will auto refresh so it is necessary to hold the current revision
                snap_package.hold()

            except snap.SnapError as e:
                logger.error(
                    "An exception occurred when installing %s. Reason: %s", snap_name, str(e)
                )
                raise

    def _set_os_config(self) -> None:
        """Sets sysctl config."""
        try:
            self.sysctl_config.configure(Config.Sysctl.OS_REQUIREMENTS)
        except (sysctl.ApplyError, sysctl.ValidationError, sysctl.CommandError) as e:
            # we allow events to continue in the case that we are not able to correctly configure
            # sysctl config, since we can  still run the workload with wrong sysctl parameters
            # even if it is not optimal.
            logger.error(f"Error setting values on sysctl: {e.message}")
            # containers share the kernel with the host system, and some sysctl parameters are
            # set at kernel level.
            logger.warning("sysctl params cannot be set. Is the machine running on a container?")

    def _instatiate_keyfile(self, event: StartEvent) -> None:
        # wait for keyFile to be created by leader unit
        if not (keyfile := self.get_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME)):
            logger.debug("waiting for leader unit to generate keyfile contents")
            event.defer()
            return

        # put keyfile on the machine with appropriate permissions
        self.push_file_to_unit(
            parent_dir=Config.MONGOD_CONF_DIR,
            file_name=KEY_FILE,
            file_contents=keyfile,
        )

    def get_keyfile_contents(self) -> str:
        """Retrieves the contents of the keyfile on host machine."""
        # wait for keyFile to be created by leader unit
        if not self.get_secret(APP_SCOPE, Config.Secrets.SECRET_KEYFILE_NAME):
            logger.debug("waiting for leader unit to generate keyfile contents")
            return

        key_file_path = f"{Config.MONGOD_CONF_DIR}/{KEY_FILE}"
        key_file = Path(key_file_path)
        if not key_file.is_file():
            logger.info("no keyfile present")
            return

        with open(key_file_path, "r") as file:
            key = file.read()

        return key

    def push_file_to_unit(self, parent_dir, file_name, file_contents) -> None:
        """K8s charms can push files to their containers easily, this is a vm charm workaround."""
        Path(parent_dir).mkdir(parents=True, exist_ok=True)
        file_name = f"{parent_dir}/{file_name}"
        with open(file_name, "w") as write_file:
            write_file.write(file_contents)

        # MongoDB limitation; it is needed 400 rights for keyfile and we need 440 rights on tls
        # certs to be able to connect via MongoDB shell
        if Config.TLS.KEY_FILE_NAME in file_name:
            os.chmod(file_name, 0o400)
        else:
            os.chmod(file_name, 0o440)
        mongodb_user = pwd.getpwnam(MONGO_USER)
        os.chown(file_name, mongodb_user.pw_uid, ROOT_USER_GID)

    def remove_file_from_unit(self, parent_dir, file_name) -> None:
        """Remove file from vm unit."""
        if os.path.exists(f"{parent_dir}/{file_name}"):
            os.remove(f"{parent_dir}/{file_name}")

    def push_tls_certificate_to_workload(self) -> None:
        """Uploads certificate to the workload container."""
        external_ca, external_pem = self.tls.get_tls_files(internal=False)
        if external_ca is not None:
            self.push_file_to_unit(
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=TLS_EXT_CA_FILE,
                file_contents=external_ca,
            )

        if external_pem is not None:
            self.push_file_to_unit(
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=TLS_EXT_PEM_FILE,
                file_contents=external_pem,
            )

        internal_ca, internal_pem = self.tls.get_tls_files(internal=True)
        if internal_ca is not None:
            self.push_file_to_unit(
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=TLS_INT_CA_FILE,
                file_contents=internal_ca,
            )

        if internal_pem is not None:
            self.push_file_to_unit(
                parent_dir=Config.MONGOD_CONF_DIR,
                file_name=TLS_INT_PEM_FILE,
                file_contents=internal_pem,
            )

    def delete_tls_certificate_from_workload(self) -> None:
        """Deletes certificate from VM."""
        logger.info("Deleting TLS certificate from VM")

        for file in [
            Config.TLS.EXT_CA_FILE,
            Config.TLS.EXT_PEM_FILE,
            Config.TLS.INT_CA_FILE,
            Config.TLS.INT_PEM_FILE,
        ]:
            self.remove_file_from_unit(Config.MONGOD_CONF_DIR, file)

    def _connect_mongodb_exporter(self) -> None:
        """Exposes the endpoint to mongodb_exporter."""
        if not self.db_initialised:
            return

        # must wait for leader to set URI before connecting
        if not self.get_secret(APP_SCOPE, MonitorUser.get_password_key_name()):
            return

        snap_cache = snap.SnapCache()
        mongodb_snap = snap_cache["charmed-mongodb"]
        mongodb_snap.set({Config.Monitoring.URI_PARAM_NAME: self.monitor_config.uri})
        mongodb_snap.restart(services=[Config.Monitoring.SERVICE_NAME])

    def _connect_pbm_agent(self) -> None:
        """Updates URI for pbm-agent."""
        if not self.db_initialised:
            return

        # must wait for leader to set URI before any attempts to update are made
        if not self.get_secret(APP_SCOPE, BackupUser.get_password_key_name()):
            return

        snap_cache = snap.SnapCache()
        pbm_snap = snap_cache["charmed-mongodb"]

        try:
            current_pbm_uri = pbm_snap.get(Config.Backup.URI_PARAM_NAME)
        except snap.SnapError:
            # if a snap variable has not been set, the retrieve of it fails
            current_pbm_uri = ""

        if current_pbm_uri == self.backup_config.uri and service_running(
            "snap.charmed-mongodb.pbm-agent.service"
        ):
            return

        logger.debug("PBM uri needs to be updated, resetting the URI and restarting the daemon")

        pbm_snap.stop(services=[Config.Backup.SERVICE_NAME])
        pbm_snap.set({Config.Backup.URI_PARAM_NAME: self.backup_config.uri})
        try:
            # Added to avoid systemd error:
            # 'snap.charmed-mongodb.pbm-agent.service: Start request repeated too quickly'
            time.sleep(1)
            pbm_snap.start(services=[Config.Backup.SERVICE_NAME], enable=True)
        except snap.SnapError as e:
            logger.error(f"Failed to restart {Config.Backup.SERVICE_NAME}: {str(e)}")
            self._get_service_status(Config.Backup.SERVICE_NAME)
            raise e

    def _get_service_status(self, service_name) -> None:
        logger.error(f"Getting status of {service_name} service:")
        self._run_diagnostic_command(
            f"systemctl status snap.charmed-mongodb.{service_name}.service"
        )
        self._run_diagnostic_command(
            f"journalctl -xeu snap.charmed-mongodb.{service_name}.service"
        )

    def _run_diagnostic_command(self, cmd) -> None:
        logger.error("Running diagnostic command: %s", cmd)
        try:
            output = subprocess.check_output(cmd, shell=True, text=True)
            logger.error(output)
        except subprocess.CalledProcessError as e:
            logger.error(f"Exception occurred running '{cmd}'\n {e}")

    def _initialise_replica_set(self, event: StartEvent) -> None:
        if self.db_initialised:
            # The replica set should be initialised only once. Check should be
            # external (e.g., check initialisation inside peer relation). We
            # shouldn't rely on MongoDB response because the data directory
            # can be corrupted.
            return

        with MongoDBConnection(self.mongodb_config, "localhost", direct=True) as direct_mongo:
            try:
                logger.info("Replica Set initialization")
                direct_mongo.init_replset()
                self.peers.data[self.app]["replica_set_hosts"] = json.dumps(
                    [self.unit_host(self.unit)]
                )

                logger.info("User initialization")
                self._init_operator_user()
                self._init_backup_user()
                self._init_monitor_user()

                # Bare replicas can create users or config-servers for related mongos apps
                if not self.is_role(Config.Role.SHARD):
                    logger.info("Manage user")
                    self.client_relations.oversee_users(None, None)

            except subprocess.CalledProcessError as e:
                logger.error(
                    "Deferring on_start: exit code: %i, stderr: %s", e.exit_code, e.stderr
                )
                event.defer()
                self.status.set_and_share_status(
                    WaitingStatus("waiting to initialise replica set")
                )
                return
            except PyMongoError as e:
                logger.error("Deferring on_start since: error=%r", e)
                event.defer()
                self.status.set_and_share_status(
                    WaitingStatus("waiting to initialise replica set")
                )
                return

            # replica set initialised properly and ready to go
            self.db_initialised = True
            self.status.set_and_share_status(ActiveStatus())

    def unit_host(self, unit: Unit) -> str:
        """Returns the ip address of a given unit."""
        # check if host is current host
        if unit == self.unit:
            return str(self.model.get_binding(Config.Relations.PEERS).network.bind_address)
        # check if host is a peer
        elif unit in self.peers.data:
            return str(self.peers.data[unit].get("private-address"))
        # raise exception if host not found
        else:
            raise ApplicationHostNotFoundError

    def get_secret(self, scope: str, key: str) -> Optional[str]:
        """Get secret from the secret storage."""
        label = generate_secret_label(self, scope)
        secret = self.secrets.get(label)
        if not secret:
            return

        value = secret.get_content().get(key)
        if value != Config.Secrets.SECRET_DELETED_LABEL:
            return value

    def set_secret(self, scope: str, key: str, value: Optional[str]) -> Optional[str]:
        """Set secret in the secret storage.

        Juju versions > 3.0 use `juju secrets`, this function first checks
          which secret store is being used before setting the secret.
        """
        if not value:
            return self.remove_secret(scope, key)

        label = generate_secret_label(self, scope)
        secret = self.secrets.get(label)
        if not secret:
            self.secrets.add(label, {key: value}, scope)
        else:
            content = secret.get_content()
            content.update({key: value})
            secret.set_content(content)
        return label

    def remove_secret(self, scope, key) -> None:
        """Removing a secret."""
        label = generate_secret_label(self, scope)
        secret = self.secrets.get(label)

        if not secret:
            return

        content = secret.get_content()

        if not content.get(key) or content[key] == Config.Secrets.SECRET_DELETED_LABEL:
            logger.error(f"Non-existing secret {scope}:{key} was attempted to be removed.")
            return

        content[key] = Config.Secrets.SECRET_DELETED_LABEL
        secret.set_content(content)

    def start_charm_services(self):
        """Starts the mongod service and if necessary starts mongos.

        Raises:
            snap.SnapError
        """
        snap_cache = snap.SnapCache()
        mongodb_snap = snap_cache["charmed-mongodb"]
        mongodb_snap.start(services=["mongod"], enable=True)

        # charms running as config server are responsible for maintaining a server side mongos
        if self.is_role(Config.Role.CONFIG_SERVER):
            mongodb_snap.start(services=["mongos"], enable=True)

    def stop_charm_services(self):
        """Stops the mongod service and if necessary stops mongos.

        Raises:
            snap.SnapError
        """
        snap_cache = snap.SnapCache()
        mongodb_snap = snap_cache["charmed-mongodb"]
        mongodb_snap.stop(services=["mongod"])

        # charms running as config server are responsible for maintaining a server side mongos
        if self.is_role(Config.Role.CONFIG_SERVER):
            mongodb_snap.stop(services=["mongos"])

    def restart_charm_services(self):
        """Restarts the mongod service with its associated configuration."""
        try:
            self.stop_charm_services()
            update_mongod_service(
                self.unit_host(self.unit),
                config=self.mongodb_config,
                role=self.role,
            )
            self.start_charm_services()
        except snap.SnapError as e:
            logger.error("An exception occurred when starting mongod agent, error: %s.", str(e))
            self.status.set_and_share_status(BlockedStatus("couldn't start MongoDB"))
            return

    def auth_enabled(self) -> bool:
        """Returns true is a mongod service has the auth configuration."""
        with open(Config.ENV_VAR_PATH, "r") as env_vars_file:
            env_vars = env_vars_file.readlines()

        return any("MONGOD_ARGS" in line and "--auth" in line for line in env_vars)

    def has_backup_service(self):
        """Verifies the backup service is available."""
        snap_cache = snap.SnapCache()
        mongodb_snap = snap_cache["charmed-mongodb"]
        if mongodb_snap.present:
            return True

        return False

    def clear_pbm_config_file(self) -> None:
        """Overwrites existing config file with the default file provided by snap."""
        subprocess.check_output(
            f"charmed-mongodb.pbm config --file {Config.MONGODB_SNAP_DATA_DIR}/etc/pbm/pbm_config.yaml",
            shell=True,
        )

    def run_pbm_command(self, cmd: List[str]) -> str:
        """Executes the provided pbm command.

        Raises:
            subprocess.CalledProcessError
        """
        pbm_response = subprocess.check_output(f"charmed-mongodb.pbm {' '.join(cmd)}", shell=True)
        return pbm_response.decode("utf-8")

    def start_backup_service(self) -> None:
        """Starts the pbm agent.

        Raises:
            snap.SnapError
        """
        snap_cache = snap.SnapCache()
        charmed_mongodb_snap = snap_cache["charmed-mongodb"]
        charmed_mongodb_snap.start(services=["pbm-agent"], enable=True)

    def restart_backup_service(self) -> None:
        """Restarts the pbm agent.

        Raises:
            snap.SnapError
        """
        snap_cache = snap.SnapCache()
        charmed_mongodb_snap = snap_cache["charmed-mongodb"]
        charmed_mongodb_snap.restart(services=["pbm-agent"])

    def _scope_obj(self, scope: Scopes) -> Application | Unit:
        if scope == APP_SCOPE:
            return self.app
        if scope == UNIT_SCOPE:
            return self.unit

    def _peer_data(self, scope: Scopes):
        if not self.peers:
            return {}.setdefault(scope, {})
        scope_obj = self._scope_obj(scope)
        return self.peers.data[scope_obj]

    def check_relation_broken_or_scale_down(self, event: RelationDepartedEvent) -> None:
        """Checks relation departed event is the result of removed relation or scale down.

        Relation departed and relation broken events occur during scaling down or during relation
        removal, only relation departed events have access to metadata to determine which case.
        """
        scaling_down = self.set_scaling_down(event)

        if scaling_down:
            logger.info(
                "Scaling down the application, no need to process removed relation in broken hook."
            )

    def is_scaling_down(self, rel_id: int) -> bool:
        """Returns True if the application is scaling down."""
        rel_departed_key = self._generate_relation_departed_key(rel_id)
        return json.loads(self.unit_peer_data[rel_departed_key])

    def has_departed_run(self, rel_id: int) -> bool:
        """Returns True if the relation departed event has run."""
        rel_departed_key = self._generate_relation_departed_key(rel_id)
        return rel_departed_key in self.unit_peer_data

    def set_scaling_down(self, event: RelationDepartedEvent) -> bool:
        """Sets whether or not the current unit is scaling down."""
        # check if relation departed is due to current unit being removed. (i.e. scaling down the
        # application.)
        rel_departed_key = self._generate_relation_departed_key(event.relation.id)
        scaling_down = event.departing_unit == self.unit
        self.unit_peer_data[rel_departed_key] = json.dumps(scaling_down)
        return scaling_down

    def proceed_on_broken_event(self, event) -> bool:
        """Returns relation_id if relation broken event occurred due to a removed relation."""
        # Only relation_deparated events can check if scaling down
        departed_relation_id = event.relation.id
        if not self.has_departed_run(departed_relation_id):
            logger.info(
                "Deferring, must wait for relation departed hook to decide if relation should be removed."
            )
            event.defer()
            return False

        # check if were scaling down and add a log message
        if self.is_scaling_down(departed_relation_id):
            logger.info(
                "Relation broken event occurring due to scale down, do not proceed to remove users."
            )
            return False

        return True

    @staticmethod
    def _generate_relation_departed_key(rel_id: int) -> str:
        """Generates the relation departed key for a specified relation id."""
        return f"relation_{rel_id}_departed"

    @property
    def _is_removing_last_replica(self) -> bool:
        """Returns True if the last replica (juju unit) is getting removed."""
        return self.app.planned_units() == 0 and len(self.peers_units) == 0

    def is_relation_feasible(self, rel_interface) -> bool:
        """Returns true if the proposed relation is feasible."""
        if self.is_sharding_component() and rel_interface in Config.Relations.DB_RELATIONS:
            self.status.set_and_share_status(
                BlockedStatus(f"Sharding roles do not support {rel_interface} interface.")
            )
            logger.error(
                "Charm is in sharding role: %s. Does not support %s interface.",
                self.role,
                rel_interface,
            )
            return False

        if (
            not self.is_sharding_component()
            and rel_interface == Config.Relations.SHARDING_RELATIONS_NAME
        ):
            self.status.set_and_share_status(
                BlockedStatus("sharding interface cannot be used by replicas")
            )
            logger.error(
                "Charm is in sharding role: %s. Does not support %s interface.",
                self.role,
                rel_interface,
            )
            return False

        if revision_mismatch_status := self.status.get_cluster_mismatched_revision_status():
            self.status.set_and_share_status(revision_mismatch_status)
            return False

        return True

    def is_sharding_component(self) -> bool:
        """Returns true if charm is running as a sharded component."""
        return self.is_role(Config.Role.SHARD) or self.is_role(Config.Role.CONFIG_SERVER)

    def is_cluster_on_same_revision(self) -> bool:
        """Returns True if the cluster is using the same charm revision.

        Note: This can only be determined by the config-server since shards are not integrated to
        each other.
        """
        if not self.is_role(Config.Role.CONFIG_SERVER):
            raise NotConfigServerError("This check can only be ran by the config-server.")

        return self.version_checker.are_related_apps_valid()

    def get_config_server_name(self) -> Optional[str]:
        """Returns the name of the Juju Application that the shard is using as a config server."""
        if not self.is_role(Config.Role.SHARD):
            logger.info(
                "Component %s is not a shard, cannot be integrated to a config-server.", self.role
            )
            return None

        return self.shard.get_config_server_name()

    def is_db_service_ready(self) -> bool:
        """Returns True if the underlying database service is ready."""
        with MongoDBConnection(self.mongodb_config) as mongod:
            mongod_ready = mongod.is_ready

        if not self.is_role(Config.Role.CONFIG_SERVER):
            return mongod_ready

        with MongoDBConnection(self.mongos_config) as mongos:
            return mongod_ready and mongos.is_ready

    # END: helper functions


class ShardingMigrationError(Exception):
    """Raised when there is an attempt to change the role of a sharding component."""


class SetPasswordError(Exception):
    """Raised on failure to set password for MongoDB user."""


class EarlyRemovalOfConfigServerError(Exception):
    """Raised when there is an attempt to remove a config-server, while related to a shard."""


if __name__ == "__main__":
    main(MongodbOperatorCharm)
