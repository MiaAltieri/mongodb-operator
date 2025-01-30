#!/usr/bin/env python3
"""Code for handing statuses in the app and unit."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
from typing import Optional, Tuple

from charms.mongodb.v1.mongodb import MongoConfiguration, MongoDBConnection
from data_platform_helpers.version_check import NoVersionError, get_charm_revision
from ops.charm import CharmBase
from ops.framework import Object
from ops.model import ActiveStatus, BlockedStatus, StatusBase, WaitingStatus
from pymongo.errors import AutoReconnect, OperationFailure, ServerSelectionTimeoutError

from config import Config

# The unique Charmhub library identifier, never change it
LIBID = "9b0b9fac53244229aed5ffc5e62141eb"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 7

AUTH_FAILED_CODE = 18
UNAUTHORISED_CODE = 13
TLS_CANNOT_FIND_PRIMARY = 133


logger = logging.getLogger(__name__)


class MongoDBStatusHandler(Object):
    """Verifies versions across multiple integrated applications."""

    def __init__(
        self,
        charm: CharmBase,
    ) -> None:
        """Constructor for CrossAppVersionChecker.

        Args:
            charm: charm to inherit from.
        """
        super().__init__(charm, None)
        self.charm = charm

        # TODO Future PR: handle update_status

    # BEGIN Helpers

    def set_and_share_status(self, status: StatusBase):
        """Sets the charm status and shares to app status and config-server if applicable."""
        # TODO Future Feature/Epic: process other statuses, i.e. only set provided status if its
        # appropriate.
        self.charm.unit.status = status

        self.set_app_status()

        if self.charm.is_role(Config.Role.SHARD):
            self.share_status_to_config_server()

    def set_app_status(self):
        """TODO Future Feature/Epic: parse statuses and set a status for the entire app."""

    def is_current_unit_ready(self, ignore_unhealthy_upgrade: bool = False) -> bool:
        """Returns True if the current unit status shows that the unit is ready.

        Note: we allow the use of ignore_unhealthy_upgrade, to avoid infinite loops due to this
        function returning False and preventing the status from being reset.
        """
        if isinstance(self.charm.unit.status, ActiveStatus):
            return True

        if ignore_unhealthy_upgrade and self.charm.unit.status == Config.Status.UNHEALTHY_UPGRADE:
            return True

        return self.is_status_related_to_mismatched_revision(
            type(self.charm.unit.status).__name__.lower()
        )

    def is_status_related_to_mismatched_revision(self, status_type: str) -> bool:
        """Returns True if the current status is related to a mimsatch in revision.

        Note: A few functions calling this method receive states differently. One receives them by
        "goal state" which processes data differently and the other via the ".status" property.
        Hence we have to be flexible to handle each.
        """
        if not self.get_cluster_mismatched_revision_status():
            return False

        if "waiting" in status_type and self.charm.is_role(Config.Role.CONFIG_SERVER):
            return True

        if "blocked" in status_type and self.charm.is_role(Config.Role.SHARD):
            return True

        return False

    def are_all_units_ready_for_upgrade(self, unit_to_ignore: str = "") -> bool:
        """Returns True if all charm units status's show that they are ready for upgrade."""
        goal_state = self.charm.model._backend._run(
            "goal-state", return_output=True, use_json=True
        )
        for unit_name, unit_state in goal_state["units"].items():
            if unit_name == unit_to_ignore:
                continue
            if unit_state["status"] == "active":
                continue
            if not self.is_status_related_to_mismatched_revision(unit_state["status"]):
                return False

        return True

    def are_shards_status_ready_for_upgrade(self) -> bool:
        """Returns True if all integrated shards status's show that they are ready for upgrade.

        A shard is ready for upgrade if it is either in the waiting for upgrade status or active
        status.
        """
        if not self.charm.is_role(Config.Role.CONFIG_SERVER):
            return False

        for sharding_relation in self.charm.config_server.get_all_sharding_relations():
            for unit in sharding_relation.units:
                unit_data = sharding_relation.data[unit]
                status_ready_for_upgrade = json.loads(
                    unit_data.get(Config.Status.STATUS_READY_FOR_UPGRADE, None)
                )
                if not status_ready_for_upgrade:
                    return False

        return True

    def share_status_to_config_server(self):
        """Shares this shards status info to the config server."""
        if not self.charm.is_role(Config.Role.SHARD):
            return

        if not (config_relation := self.charm.shard.get_config_server_relation()):
            return

        config_relation.data[self.charm.unit][Config.Status.STATUS_READY_FOR_UPGRADE] = json.dumps(
            self.is_unit_status_ready_for_upgrade()
        )

    def is_unit_status_ready_for_upgrade(self) -> bool:
        """Returns True if the status of the current unit reflects that it is ready for upgrade."""
        current_status = self.charm.unit.status
        status_message = current_status.message
        if isinstance(current_status, ActiveStatus):
            return True

        if not isinstance(current_status, BlockedStatus):
            return False

        if status_message and "is not up-to date with config-server" in status_message:
            return True

        return False

    def process_statuses(self) -> StatusBase:
        """Retrieves statuses from processes inside charm and returns the highest priority status.

        When a non-fatal error occurs while processing statuses, the error is processed and
        returned as a statuses.

        TODO: add more status handling here for other cases: i.e. TLS, or resetting a status that
        should not be reset
        """
        # retrieve statuses of different services running on Charmed MongoDB
        deployment_mode = (
            "replica set" if self.charm.is_role(Config.Role.REPLICATION) else "cluster"
        )
        waiting_status = None
        try:
            statuses = self.get_statuses()
        except OperationFailure as e:
            if e.code in [UNAUTHORISED_CODE, AUTH_FAILED_CODE]:
                waiting_status = f"Waiting to sync passwords across the {deployment_mode}"
            elif e.code == TLS_CANNOT_FIND_PRIMARY:
                waiting_status = (
                    f"Waiting to sync internal membership across the {deployment_mode}"
                )
            else:
                raise
        except ServerSelectionTimeoutError:
            waiting_status = f"Waiting to sync internal membership across the {deployment_mode}"

        if waiting_status:
            return WaitingStatus(waiting_status)

        return self.prioritize_statuses(statuses)

    def get_statuses(self) -> Tuple:
        """Retrieves statuses for the different processes running inside the unit."""
        mongodb_status = build_unit_status(
            self.charm.mongodb_config, self.charm.unit_host(self.charm.unit)
        )
        shard_status = self.charm.shard.get_shard_status()
        config_server_status = self.charm.config_server.get_config_server_status()
        pbm_status = self.charm.backups.get_pbm_status()
        return (mongodb_status, shard_status, config_server_status, pbm_status)

    def prioritize_statuses(self, statuses: Tuple) -> StatusBase:
        """Returns the status with the highest priority from backups, sharding, and mongod."""
        mongodb_status, shard_status, config_server_status, pbm_status = statuses
        # failure in mongodb takes precedence over sharding and config server
        if not isinstance(mongodb_status, ActiveStatus):
            return mongodb_status

        if shard_status and not isinstance(shard_status, ActiveStatus):
            return shard_status

        if config_server_status and not isinstance(config_server_status, ActiveStatus):
            return config_server_status

        if pbm_status and not isinstance(pbm_status, ActiveStatus):
            return pbm_status

        # if all statuses are active report mongodb status over sharding status
        return mongodb_status

    def get_invalid_integration_status(self) -> Optional[StatusBase]:
        """Returns a status if an invalid integration is present."""
        has_sharding_integration = (
            self.model.relations[Config.Relations.CONFIG_SERVER_RELATIONS_NAME]
            or self.model.relations[Config.Relations.SHARDING_RELATIONS_NAME]
        )
        if self.charm.is_role(Config.Role.REPLICATION) and has_sharding_integration:
            return BlockedStatus("sharding interface cannot be used by replicas")

        if not self.charm.cluster.is_valid_mongos_integration():
            return BlockedStatus(
                "Relation to mongos not supported, config role must be config-server"
            )

        if not self.charm.backups.is_valid_s3_integration():
            return BlockedStatus(
                "Relation to s3-integrator is not supported, config role must be config-server"
            )

        return self.get_cluster_mismatched_revision_status()

    def get_cluster_mismatched_revision_status(self) -> Optional[StatusBase]:
        """Returns a Status if the cluster has mismatched revisions."""
        # check for invalid versions in sharding integrations, i.e. a shard running on
        # revision  88 and a config-server running on revision 110
        current_charms_version = get_charm_revision(
            self.charm.unit, local_version=self.charm.get_charm_internal_revision
        )
        local_identifier = (
            "-locally built"
            if self.charm.version_checker.is_local_charm(self.charm.app.name)
            else ""
        )
        try:
            if self.charm.version_checker.are_related_apps_valid():
                return
        except NoVersionError as e:
            # relations to shards/config-server are expected to provide a version number. If they
            # do not, it is because they are from an earlier charm revision, i.e. pre-revison X.
            logger.debug(e)
            if self.charm.is_role(Config.Role.SHARD):
                return BlockedStatus(
                    f"Charm revision ({current_charms_version}{local_identifier}) is not up-to date with config-server."
                )

        if self.charm.is_role(Config.Role.SHARD):
            config_server_revision = self.charm.version_checker.get_version_of_related_app(
                self.charm.get_config_server_name()
            )
            remote_local_identifier = (
                "-locally built"
                if self.charm.version_checker.is_local_charm(self.charm.get_config_server_name())
                else ""
            )
            return BlockedStatus(
                f"Charm revision ({current_charms_version}{local_identifier}) is not up-to date with config-server ({config_server_revision}{remote_local_identifier})."
            )

        if self.charm.is_role(Config.Role.CONFIG_SERVER):
            return WaitingStatus(
                f"Waiting for shards to upgrade/downgrade to revision {current_charms_version}{local_identifier}."
            )


def build_unit_status(mongodb_config: MongoConfiguration, unit_host: str) -> StatusBase:
    """Generates the status of a unit based on its status reported by mongod."""
    try:
        with MongoDBConnection(mongodb_config) as mongo:
            replset_status = mongo.get_replset_status()

            if unit_host not in replset_status:
                return WaitingStatus("Member being added..")

            replica_status = replset_status[unit_host]

            match replica_status:
                case "PRIMARY":
                    return ActiveStatus("Primary")
                case "SECONDARY":
                    return ActiveStatus("")
                case "STARTUP" | "STARTUP2" | "ROLLBACK" | "RECOVERING":
                    return WaitingStatus("Member is syncing...")
                case "REMOVED":
                    return WaitingStatus("Member is removing...")
                case _:
                    return BlockedStatus(replica_status)
    except ServerSelectionTimeoutError as e:
        # ServerSelectionTimeoutError is commonly due to ReplicaSetNoPrimary
        logger.debug("Got error: %s, while checking replica set status", str(e))
        return WaitingStatus("Waiting for primary re-election..")
    except AutoReconnect as e:
        # AutoReconnect is raised when a connection to the database is lost and an attempt to
        # auto-reconnect will be made by pymongo.
        logger.debug("Got error: %s, while checking replica set status", str(e))
        return WaitingStatus("Waiting to reconnect to unit..")

    # END: Helpers
