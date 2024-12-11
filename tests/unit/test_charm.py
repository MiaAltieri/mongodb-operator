# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import re
import unittest
from unittest import mock
from unittest.mock import MagicMock, call, patch

import pytest
from charms.operator_libs_linux.v2 import snap
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus
from ops.testing import Harness
from parameterized import parameterized
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure
from tenacity import stop_after_attempt

from charm import MongodbOperatorCharm, NotReadyError, subprocess
from config import Config


from .helpers import patch_network_get

REPO_NAME = "deb-https://repo.mongodb.org/apt/ubuntu-focal/mongodb-org/5.0"
REPO_ENTRY = (
    "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse"
)
REPO_MAP = {"deb-https://repo.mongodb.org/apt/ubuntu-focal/mongodb-org/5.0"}
PEER_ADDR = {"private-address": "127.4.5.6"}

PYMONGO_EXCEPTIONS = [
    ConnectionFailure("error message"),
    ConfigurationError("error message"),
    OperationFailure("error message"),
]

S3_RELATION_NAME = "s3-credentials"


class TestCharm(unittest.TestCase):
    @patch("charm.get_charm_revision")
    def setUp(self, *unused):
        self.harness = Harness(MongodbOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()
        self.peer_rel_id = self.harness.add_relation("database-peers", "database-peers")
        self.peer_rel_id = self.harness.add_relation("upgrade-version-a", "upgrade-version-a")

    @pytest.fixture
    def use_caplog(self, caplog):
        self._caplog = caplog

    def _setup_secrets(self):
        self.harness.set_leader(True)
        self.harness.charm._generate_secrets()
        self.harness.set_leader(False)

    @patch("charm.MongodbOperatorCharm.get_secret")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._init_operator_user")
    @patch("charm.MongodbOperatorCharm._open_ports_tcp")
    @patch("charm.snap.SnapCache")
    @patch("charm.MongodbOperatorCharm.push_file_to_unit")
    @patch("builtins.open")
    def test_on_start_not_leader_doesnt_initialise_replica_set(
        self, open, path, snap, _open_ports_tcp, init_admin, connection, get_secret
    ):
        """Tests that a non leader unit does not initialise the replica set."""
        # set snap data
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}
        get_secret.return_value = "pass123"
        self.harness.charm.app_peer_data["monitor-password"] = "pass123"

        self.harness.set_leader(False)
        self.harness.charm.on.start.emit()

        mock_mongodb_snap.start.assert_called()
        _open_ports_tcp.assert_called()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus())
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_admin.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._init_operator_user")
    @patch("charm.MongodbOperatorCharm._open_ports_tcp")
    @patch("charm.MongodbOperatorCharm.push_file_to_unit")
    @patch("builtins.open")
    def test_on_start_snap_failure_leads_to_blocked_status(
        self,
        open,
        path,
        _open_ports_tcp,
        init_admin,
        connection,
    ):
        """Test failures on systemd result in blocked status."""
        self.harness.set_leader(True)
        self.harness.charm.on.start.emit()
        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))
        _open_ports_tcp.assert_not_called()

        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_admin.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongodbOperatorCharm._open_ports_tcp")
    @patch("charm.MongodbOperatorCharm._initialise_replica_set")
    @patch("charm.snap.SnapCache")
    @patch("charm.MongodbOperatorCharm.push_file_to_unit")
    @patch("builtins.open")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._init_operator_user")
    def test_on_start_mongod_not_ready_defer(
        self,
        init_admin,
        connection,
        open,
        path,
        snap,
        initialise_replica_set,
        _open_ports_tcp,
    ):
        """Test verifies that we wait to initialise replica set when mongod is not running."""
        # set snap data
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}

        self.harness.set_leader(True)
        connection.return_value.__enter__.return_value.is_ready = False

        self.harness.charm.on.start.emit()
        self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))
        connection.return_value.__enter__.return_value.init_replset.assert_not_called()
        init_admin.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongodbOperatorCharm._open_ports_tcp")
    @patch("charm.snap.SnapCache")
    @patch("charm.MongodbOperatorCharm.push_file_to_unit")
    @patch("builtins.open")
    def test_start_unable_to_open_tcp_moves_to_blocked(self, open, path, snap, _open_ports_tcp):
        """Test verifies that if TCP port cannot be opened we go to the blocked state."""
        # set snap data
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}

        self.harness.set_leader(True)
        _open_ports_tcp.side_effect = subprocess.CalledProcessError(
            cmd="open-port 27017/TCP", returncode=1
        )
        self.harness.charm.on.start.emit()

        self.assertEqual(
            self.harness.charm.unit.status, BlockedStatus("failed to open TCP port for MongoDB")
        )

    @patch("subprocess.check_call")
    def test_set_port(self, _call):
        """Test verifies operation of set port."""
        self.harness.charm._open_ports_tcp([27017])
        # Make sure the port is opened and the service is started
        self.assertEqual(_call.call_args_list, [call(["open-port", "27017/TCP"])])

    @patch("subprocess.check_call")
    def test_set_port_failure(self, _call):
        """Test verifies that we raise the correct errors when we fail to open a port."""
        _call.side_effect = subprocess.CalledProcessError(cmd="open-port 27017/TCP", returncode=1)

        with self.assertRaises(subprocess.CalledProcessError):
            with self.assertLogs("charm", "ERROR") as logs:
                self.harness.charm._open_ports_tcp([27017])
                self.assertTrue("failed opening port 27017" in "".join(logs.output))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.update_mongod_service")
    @patch("charm.snap.SnapCache")
    @patch("subprocess.check_call")
    def test_install_snap_packages_failure(self, _call, snap_cache, update_mongod_service):
        """Test verifies the correct functions get called when installing apt packages."""
        snap_cache.side_effect = snap.SnapError
        self.harness.charm.on.install.emit()
        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.update_mongod_service")
    @patch("charm.snap.SnapCache")
    @patch("charm.setup_logrotate_and_cron")
    @patch("charm.copy_licenses_to_unit")
    @patch("subprocess.check_call")
    @patch("builtins.open")
    @patch("charm.sysctl.Config.configure")
    def test_install(self, patched_os_config, *unused):
        """Test verifies the correct functions get called when installing apt packages."""
        self.harness.charm.on.install.emit()
        patched_os_config.assert_called_once_with(Config.Sysctl.OS_REQUIREMENTS)

    @patch_network_get(private_address="1.1.1.1")
    def test_app_hosts(self):
        rel_id = self.harness.charm.model.get_relation("database-peers").id
        self.harness.add_relation_unit(rel_id, "mongodb/1")
        self.harness.update_relation_data(rel_id, "mongodb/1", PEER_ADDR)

        resulting_ips = self.harness.charm.app_hosts
        expected_ips = ["127.4.5.6", "1.1.1.1"]
        self.assertEqual(resulting_ips, expected_ips)

    @patch("charm.MongoDBConnection")
    def test_mongodb_relation_joined_non_leader_does_nothing(self, connection):
        """Test verifies that non-leader units don't reconfigure the replica set on joined."""
        rel = self.harness.charm.model.get_relation("database-peers")
        self.harness.set_leader(False)
        self.harness.charm.on.database_peers_relation_joined.emit(relation=rel)
        connection.return_value.__enter__.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_mongodb_relation_joined_all_replicas_not_ready(
        self, _, rev, local, is_local, connection
    ):
        """Tests that we go into waiting when current ReplicaSet hosts are not ready.

        Tests the scenario that if current replica set hosts are not ready, the leader goes into
        WaitingStatus and no attempt to reconfigure is made.
        """
        # preset values
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        connection.return_value.__enter__.return_value.is_ready = False
        connection.return_value.__enter__.return_value.get_replset_members.return_value = {
            "1.1.1.1"
        }

        # simulate 2nd MongoDB unit
        rel = self.harness.charm.model.get_relation("database-peers")
        self.harness.add_relation_unit(rel.id, "mongodb/1")
        self.harness.update_relation_data(rel.id, "mongodb/1", PEER_ADDR)

        # verify we go into waiting and don't reconfigure
        self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))
        connection.return_value.__enter__.return_value.add_replset_member.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBConnection")
    @patch("charms.mongodb.v0.mongo.MongoClient")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_relation_joined_get_members_failure(
        self, _, rev, local, is_local, client, connection, defer
    ):
        """Tests reconfigure does not execute when unable to get the replica set members.

        Verifies in case of relation_joined and relation departed, that when the the database
        cannot retrieve the replica set members that no attempts to remove/add units are made and
        that the the event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        rel = self.harness.charm.model.get_relation("database-peers")

        for exception in PYMONGO_EXCEPTIONS:
            connection.return_value.__enter__.return_value.get_replset_members.side_effect = (
                exception
            )

            # test both relation events
            for departed in [False, True]:
                if departed:
                    # simulate removing 2nd MongoDB unit
                    self.harness.remove_relation_unit(rel.id, "mongodb/1")
                    connection.return_value.__enter__.return_value.add_replset_member.assert_not_called()
                else:
                    # simulate 2nd MongoDB unit joining
                    self.harness.add_relation_unit(rel.id, "mongodb/1")
                    self.harness.update_relation_data(rel.id, "mongodb/1", PEER_ADDR)
                    connection.return_value.__enter__.return_value.remove_replset_member.assert_not_called()

                defer.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBConnection")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_reconfigure_add_member_failure(self, _, rev, local, is_local, connection, defer):
        """Tests reconfigure does not proceed when unable to add a member.

        Verifies in relation joined events, that when the database cannot add a member that the
        event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        connection.return_value.__enter__.return_value.get_replset_members.return_value = {
            "1.1.1.1"
        }
        rel = self.harness.charm.model.get_relation("database-peers")

        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append(NotReadyError)
        for exception in exceptions:
            connection.return_value.__enter__.return_value.add_replset_member.side_effect = (
                exception
            )

            # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
            self.harness.add_relation_unit(rel.id, "mongodb/1")
            self.harness.update_relation_data(rel.id, "mongodb/1", PEER_ADDR)

            connection.return_value.__enter__.return_value.add_replset_member.assert_called()
            defer.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongodbOperatorCharm._open_ports_tcp")
    @patch("charm.snap.SnapCache")
    @patch("charm.MongodbOperatorCharm.push_file_to_unit")
    @patch("builtins.open")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._init_operator_user")
    def test_initialise_replica_failure_leads_to_waiting_state(
        self,
        init_admin,
        connection,
        open,
        path,
        snap_cache,
        _,
    ):
        """Tests that failure to initialise replica set goes into Waiting Status."""
        # set snap data
        mock_mongodb_snap = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}

        # set peer data so that leader doesn't reconfigure set on set_leader

        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["_new_leader_must_reconfigure"] = "False"
        connection.return_value.__enter__.return_value.is_ready = True

        for exception in PYMONGO_EXCEPTIONS:
            connection.return_value.__enter__.return_value.init_replset.side_effect = exception
            self.harness.charm.on.start.emit()
            connection.return_value.__enter__.return_value.init_replset.assert_called()
            init_admin.assert_not_called()
            self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v0.set_status.MongoDBConnection")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charms.mongodb.v0.set_status.build_unit_status")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_update_status_mongodb_error(
        self,
        _,
        get_mongodb_status,
        get_pbm_status,
        connection,
        status_connection,
        get_charm_revision,
        is_local,
        is_integrated_to_local,
    ):
        """Tests that when MongoDB is not active, that is reported instead of pbm."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        connection.return_value.__enter__.return_value.is_ready = True

        pbm_statuses = [
            ActiveStatus("pbm"),
            BlockedStatus("pbm"),
            MaintenanceStatus("pbm"),
            WaitingStatus("pbm"),
        ]
        mongodb_statuses = [
            BlockedStatus("mongodb"),
            MaintenanceStatus("mongodb"),
            WaitingStatus("mongodb"),
        ]
        self.harness.add_relation(S3_RELATION_NAME, "s3-integrator")

        for pbm_status in pbm_statuses:
            for mongodb_status in mongodb_statuses:
                get_pbm_status.return_value = pbm_status
                get_mongodb_status.return_value = mongodb_status
                self.harness.charm.on.update_status.emit()
                self.assertEqual(self.harness.charm.unit.status, mongodb_status)

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v0.set_status.MongoDBConnection")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charms.mongodb.v0.set_status.build_unit_status")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_update_status_pbm_error(
        self,
        _,
        get_mongodb_status,
        get_pbm_status,
        connection,
        status_connection,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Tests when MongoDB is active and pbm is in the error state, pbm status is reported."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        connection.return_value.__enter__.return_value.is_ready = True

        pbm_statuses = [
            BlockedStatus("pbm"),
            MaintenanceStatus("pbm"),
            WaitingStatus("pbm"),
        ]
        mongodb_statuses = [ActiveStatus("mongodb")]
        self.harness.add_relation(S3_RELATION_NAME, "s3-integrator")

        for pbm_status in pbm_statuses:
            for mongodb_status in mongodb_statuses:
                get_pbm_status.return_value = pbm_status
                get_mongodb_status.return_value = mongodb_status
                self.harness.charm.on.update_status.emit()
                self.assertEqual(self.harness.charm.unit.status, pbm_status)

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v0.set_status.MongoDBConnection")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charms.mongodb.v0.set_status.build_unit_status")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_update_status_pbm_and_mongodb_ready(
        self,
        _,
        get_mongodb_status,
        get_pbm_status,
        connection,
        status_connection,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Tests when both Mongodb and pbm are ready that MongoDB status is reported."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        connection.return_value.__enter__.return_value.is_ready = True

        self.harness.add_relation(S3_RELATION_NAME, "s3-integrator")

        get_pbm_status.return_value = ActiveStatus("pbm")
        get_mongodb_status.return_value = ActiveStatus("mongodb")
        self.harness.charm.on.update_status.emit()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus("mongodb"))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v0.set_status.MongoDBConnection")
    @patch("charm.MongoDBConnection")
    @patch("charms.mongodb.v0.set_status.build_unit_status")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_update_status_no_s3(
        self,
        _,
        has_backup_service,
        get_mongodb_status,
        connection,
        status_connection,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Tests when the s3 relation isn't present that the MongoDB status is reported."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        connection.return_value.__enter__.return_value.is_ready = True
        has_backup_service.return_value = True

        get_mongodb_status.return_value = ActiveStatus("mongodb")
        self.harness.charm.on.update_status.emit()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus("mongodb"))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v0.set_status.MongoDBConnection")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_update_status_primary(
        self,
        _,
        pbm_status,
        connection,
        status_connection,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Tests that update status identifies the primary unit and updates status."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        pbm_status.return_value = ActiveStatus("")

        self.harness.set_leader(False)
        connection.return_value.__enter__.return_value.is_ready = True
        status_connection.return_value.__enter__.return_value.get_replset_status.return_value = {
            "1.1.1.1": "PRIMARY"
        }
        self.harness.charm.on.update_status.emit()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus("Primary"))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v0.set_status.MongoDBConnection")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_update_status_secondary(
        self,
        _,
        pbm_status,
        connection,
        status_connection,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Tests that update status identifies secondary units and doesn't update status."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        pbm_status.return_value = ActiveStatus("")

        self.harness.set_leader(False)
        connection.return_value.__enter__.return_value.is_ready = True
        status_connection.return_value.__enter__.return_value.get_replset_status.return_value = {
            "1.1.1.1": "SECONDARY"
        }
        self.harness.charm.on.update_status.emit()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus(""))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v0.set_status.MongoDBConnection")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_update_status_additional_messages(
        self,
        _,
        pbm_status,
        connection,
        status_connection,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Tests status updates are correct for non-primary and non-secondary cases."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        pbm_status.return_value = ActiveStatus("")

        # Case 1: Unit has not been added to replica set yet
        self.harness.set_leader(False)
        connection.return_value.__enter__.return_value.is_ready = True
        status_connection.return_value.__enter__.return_value.get_replset_status.return_value = {}
        self.harness.charm.on.update_status.emit()
        self.assertEqual(self.harness.charm.unit.status, WaitingStatus("Member being added.."))

        # Case 2: Unit is being removed from replica set
        status_connection.return_value.__enter__.return_value.get_replset_status.return_value = {
            "1.1.1.1": "REMOVED"
        }
        self.harness.charm.on.update_status.emit()
        self.assertEqual(self.harness.charm.unit.status, WaitingStatus("Member is removing..."))

        # Case 3: Member is syncing to replica set
        for syncing_status in ["STARTUP", "STARTUP2", "ROLLBACK", "RECOVERING"]:
            status_connection.return_value.__enter__.return_value.get_replset_status.return_value = {
                "1.1.1.1": syncing_status
            }
            self.harness.charm.on.update_status.emit()
            self.assertEqual(self.harness.charm.unit.status, WaitingStatus("Member is syncing..."))

        # Case 4: Unknown status
        status_connection.return_value.__enter__.return_value.get_replset_status.return_value = {
            "1.1.1.1": "unknown"
        }
        self.harness.charm.on.update_status.emit()
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("unknown"))

    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charm.MongodbOperatorCharm.get_secret")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    def test_update_status_not_ready(
        self, connection, get_secret, get_rev, is_local, is_integrated_to_local
    ):
        """Tests that if mongod is not running on this unit it restarts it."""
        get_secret.return_value = "pass123"
        connection.return_value.__enter__.return_value.is_ready = False
        self.harness.charm.app_peer_data["db_initialised"] = "true"

        self.harness.charm.on.update_status.emit()
        self.assertEqual(
            self.harness.charm.unit.status, WaitingStatus("Waiting for MongoDB to start")
        )

    @patch("charm.MongodbOperatorCharm.get_secret")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    def test_get_primary_current_unit_primary(self, connection, get_secret):
        """Tests get primary outputs correct primary when called on a primary replica."""
        mock_event = mock.Mock()
        connection.return_value.__enter__.return_value.primary.return_value = "1.1.1.1"
        get_secret.return_value = "pass123"
        self.harness.charm._on_get_primary_action(mock_event)
        mock_event.set_results.assert_called_with({"replica-set-primary": "mongodb/0"})

    @patch("charm.MongodbOperatorCharm.get_secret")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    def test_get_primary_peer_unit_primary(self, connection, get_secret):
        """Tests get primary outputs correct primary when called on a secondary replica."""
        # add peer unit
        rel_id = self.harness.charm.model.get_relation("database-peers").id
        get_secret.return_value = "pass123"
        self.harness.add_relation_unit(rel_id, "mongodb/1")
        self.harness.update_relation_data(rel_id, "mongodb/1", {"private-address": "2.2.2.2"})

        # mock out the self unit not being primary but its peer being primary
        connection.return_value.__enter__.return_value.primary.return_value = "2.2.2.2"

        mock_event = mock.Mock()

        self.harness.charm._on_get_primary_action(mock_event)
        mock_event.set_results.assert_called_with({"replica-set-primary": "mongodb/1"})

    @patch("charm.MongodbOperatorCharm.get_secret")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    def test_primary_no_primary(self, connection, get_secret):
        """Test that that the primary property can handle the case when there is no primary.

        Verifies that when there is no primary, the property _primary returns None.
        """
        get_secret.return_value = "pass123"
        # add peer unit
        rel_id = self.harness.charm.model.get_relation("database-peers").id
        self.harness.add_relation_unit(rel_id, "mongodb/1")
        self.harness.update_relation_data(rel_id, "mongodb/1", {"private-address": "2.2.2.2"})

        # mock out no units being primary
        connection.return_value.__enter__.return_value.primary.return_value = None

        # verify no primary identified
        primary = self.harness.charm.primary
        self.assertEqual(primary, None)

    @patch("charm.MongodbOperatorCharm.get_secret")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    def test_primary_failure(self, connection, get_secret):
        """Tests that when getting the primary fails that no replica is reported as primary."""
        # verify that we raise the correct exception
        get_secret.return_value = "pass123"
        for exception in PYMONGO_EXCEPTIONS:
            connection.return_value.__enter__.return_value.primary.side_effect = exception
            self.assertEqual(self.harness.charm.primary, None)

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.stop_after_attempt")
    def test_storage_detaching_failure_does_not_defer(self, retry_stop, connection):
        """Test that failure in removing replica does not defer the hook.

        Deferring Storage Detached hooks can result in un-predicable behavior and while it is
        technically possible to defer the event, it shouldn't be. This test verifies that no
        attempt to defer storage detached as made.
        """
        retry_stop.return_value = stop_after_attempt(1)
        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append(NotReadyError)
        for exception in exceptions:
            connection.return_value.__enter__.return_value.remove_replset_member.side_effect = (
                exception
            )
            event = mock.Mock()
            self.harness.charm.on.mongodb_storage_detaching.emit(mock.Mock())
            event.defer.assert_not_called()

    @patch("charm.MongodbOperatorCharm.get_secret")
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongodbOperatorCharm.app_hosts")
    @patch("charm.MongoDBConnection")
    def test_process_unremoved_units_handles_errors(self, connection, app_hosts, get_secret):
        """Test failures in process_unremoved_units are handled and not raised."""
        get_secret.return_value = "pass123"
        connection.return_value.__enter__.return_value.get_replset_members.return_value = {
            "1.1.1.1",
            "2.2.2.2",
        }
        self.harness.charm.app_hosts = ["2.2.2.2"]

        for exception in [PYMONGO_EXCEPTIONS, NotReadyError]:
            connection.return_value.__enter__.return_value.remove_replset_member.side_effect = (
                exception
            )
            self.harness.charm.process_unremoved_units(mock.Mock())
            connection.return_value.__enter__.return_value.remove_replset_member.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoConfiguration")
    @patch("charm.subprocess.run")
    def test_start_init_user_after_second_call(self, run, config):
        """Tests that the creation of the admin user is only performed once.

        Verifies that if the user is already set up, that no attempts to set it up again are
        made.
        """
        self.harness.set_leader(True)

        self.harness.charm._init_operator_user()
        self.assertEqual("operator-user-created" in self.harness.charm.app_peer_data, True)

        self.harness.charm._init_operator_user()
        run.assert_called_once()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_set_password(self, pbm_status, connection):
        """Tests that a new admin password is generated and is returned to the user."""
        self.harness.set_leader(True)
        pbm_status.return_value = ActiveStatus("pbm")
        original_password = self.harness.charm.get_secret("app", "operator-password")
        action_event = mock.Mock()
        action_event.params = {}
        self.harness.charm._on_set_password(action_event)
        new_password = self.harness.charm.get_secret("app", "operator-password")

        # verify app data is updated and results are reported to user
        self.assertNotEqual(original_password, new_password)

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_set_password_provided(self, pbm_status, connection):
        """Tests that a given password is set as the new mongodb password."""
        self.harness.set_leader(True)
        pbm_status.return_value = ActiveStatus("pbm")
        action_event = mock.Mock()
        action_event.params = {"password": "canonical123"}
        self.harness.charm._on_set_password(action_event)
        new_password = self.harness.charm.get_secret("app", "operator-password")

        # verify app data is updated and results are reported to user
        self.assertEqual("canonical123", new_password)
        action_event.set_results.assert_called_with(
            {"password": "canonical123", "secret-id": mock.ANY}
        )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_set_password_failure(self, pbm_status, connection):
        """Tests failure to reset password does not update app data and failure is reported."""
        self.harness.set_leader(True)
        pbm_status.return_value = ActiveStatus("pbm")
        original_password = self.harness.charm.get_secret("app", "operator-password")
        action_event = mock.Mock()
        action_event.params = {}

        for exception in [PYMONGO_EXCEPTIONS, NotReadyError]:
            connection.return_value.__enter__.return_value.set_user_password.side_effect = (
                exception
            )
            self.harness.charm._on_set_password(action_event)
            current_password = self.harness.charm.get_secret("app", "operator-password")

            # verify passwords are not updated.
            self.assertEqual(current_password, original_password)
            action_event.fail.assert_called()

    def test_get_password(self):
        self._setup_secrets()
        assert isinstance(self.harness.charm.get_secret("app", "monitor-password"), str)
        self.harness.charm.get_secret("app", "non-existing-secret") is None

        self.harness.charm.set_secret("unit", "somekey", "bla")
        assert isinstance(self.harness.charm.get_secret("unit", "somekey"), str)
        self.harness.charm.get_secret("unit", "non-existing-secret") is None

    def test_set_reset_existing_password_app(self):
        self._setup_secrets()
        self.harness.set_leader(True)

        # Getting current password
        self.harness.charm.set_secret("app", "monitor-password", "bla")
        assert self.harness.charm.get_secret("app", "monitor-password") == "bla"

        self.harness.charm.set_secret("app", "monitor-password", "blablabla")
        assert self.harness.charm.get_secret("app", "monitor-password") == "blablabla"

    def test_set_reset_existing_password_app_nonleader(self):
        self._setup_secrets()
        self.harness.set_leader(False)

        # Getting current password
        with self.assertRaises(RuntimeError):
            self.harness.charm.set_secret("app", "monitor-password", "bla")

    @parameterized.expand([("app"), ("unit")])
    def test_set_secret_returning_secret_id(self, scope):
        secret_id = self.harness.charm.set_secret(scope, "somekey", "bla")
        assert re.match(f"mongodb.{scope}", secret_id)

    @parameterized.expand([("app"), ("unit")])
    def test_set_reset_new_secret(self, scope):
        if scope == "app":
            self.harness.set_leader(True)

        # Getting current password
        self.harness.charm.set_secret(scope, "new-secret", "bla")
        assert self.harness.charm.get_secret(scope, "new-secret") == "bla"

        # Reset new secret
        self.harness.charm.set_secret(scope, "new-secret", "blablabla")
        assert self.harness.charm.get_secret(scope, "new-secret") == "blablabla"

        # Set another new secret
        self.harness.charm.set_secret(scope, "new-secret2", "blablabla")
        assert self.harness.charm.get_secret(scope, "new-secret2") == "blablabla"

    def test_set_reset_new_secret_non_leader(self):
        self.harness.set_leader(True)

        # Getting current password
        self.harness.charm.set_secret("app", "new-secret", "bla")
        assert self.harness.charm.get_secret("app", "new-secret") == "bla"

        # Reset new secret
        self.harness.set_leader(False)
        with self.assertRaises(RuntimeError):
            self.harness.charm.set_secret("app", "new-secret", "blablabla")

        # Set another new secret
        with self.assertRaises(RuntimeError):
            self.harness.charm.set_secret("app", "new-secret2", "blablabla")

    @parameterized.expand([("app"), ("unit")])
    def test_invalid_secret(self, scope):
        with self.assertRaises(TypeError):
            self.harness.charm.set_secret("unit", "somekey", 1)

        self.harness.charm.set_secret("unit", "somekey", "")
        assert self.harness.charm.get_secret(scope, "somekey") is None

    @pytest.mark.usefixtures("use_caplog")
    def test_delete_password(self):
        self._setup_secrets()
        self.harness.set_leader(True)

        assert self.harness.charm.get_secret("app", "monitor-password")
        self.harness.charm.remove_secret("app", "monitor-password")
        assert self.harness.charm.get_secret("app", "monitor-password") is None

        assert self.harness.charm.set_secret("unit", "somekey", "somesecret")
        self.harness.charm.remove_secret("unit", "somekey")
        assert self.harness.charm.get_secret("unit", "somekey") is None

        with self._caplog.at_level(logging.ERROR):
            self.harness.charm.remove_secret("app", "monitor-password")
            assert (
                "Non-existing secret app:monitor-password was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.remove_secret("unit", "somekey")
            assert (
                "Non-existing secret unit:somekey was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.remove_secret("app", "non-existing-secret")
            assert (
                "Non-existing secret app:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.remove_secret("unit", "non-existing-secret")
            assert (
                "Non-existing secret unit:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

    def test_delete_password_non_leader(self):
        self._setup_secrets()
        self.harness.set_leader(False)
        assert self.harness.charm.get_secret("app", "monitor-password")
        with self.assertRaises(RuntimeError):
            self.harness.charm.remove_secret("app", "monitor-password")

    @parameterized.expand([("app"), ("unit")])
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_on_secret_changed(self, scope, connect_exporter):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        secret_label = self.harness.charm.set_secret(scope, "new-secret", "bla")
        secret = self.harness.charm.model.get_secret(label=secret_label)

        event = mock.Mock()
        event.secret = secret
        secret_label = self.harness.charm._on_secret_changed(event)
        connect_exporter.assert_called()

    @parameterized.expand([("app"), ("unit")])
    @pytest.mark.usefixtures("use_caplog")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_on_other_secret_changed(self, scope, connect_exporter):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        # "Hack": creating a secret outside of the normal MongodbOperatorCharm.set_secret workflow
        scope_obj = self.harness.charm._scope_obj(scope)
        secret = scope_obj.add_secret({"key": "value"})

        event = mock.Mock()
        event.secret = secret

        with self._caplog.at_level(logging.DEBUG):
            self.harness.charm._on_secret_changed(event)
            assert f"Secret {secret.id} changed, but it's unknown" in self._caplog.text

        connect_exporter.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_connect_to_mongo_exporter_on_set_password(self, connect_exporter, connection):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user."""
        # container = self.harness.model.unit.get_container("mongod")
        # self.harness.set_can_connect(container, True)
        # self.harness.charm.on.mongod_pebble_ready.emit(container)
        self.harness.set_leader(True)

        action_event = mock.Mock()
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_set_password(action_event)
        connect_exporter.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_event_set_password_secrets(
        self, connect_exporter, connection, has_backup_service, get_pbm_status
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        pw = "bla"
        has_backup_service.return_value = True
        get_pbm_status.return_value = ActiveStatus()
        self.harness.set_leader(True)

        action_event = mock.Mock()
        action_event.set_results = MagicMock()
        action_event.params = {"username": "monitor", "password": pw}
        self.harness.charm._on_set_password(action_event)
        connect_exporter.assert_called()

        action_event.set_results.assert_called()
        args_pw_set = action_event.set_results.call_args.args[0]
        assert "secret-id" in args_pw_set

        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        assert args_pw["password"] == pw

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_event_auto_reset_password_secrets_when_no_pw_value_shipped(
        self, connect_exporter, connection, has_backup_service, get_pbm_status
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        has_backup_service.return_value = True
        get_pbm_status.return_value = ActiveStatus()
        self._setup_secrets()
        self.harness.set_leader(True)

        action_event = mock.Mock()
        action_event.set_results = MagicMock()

        # Getting current password
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        pw1 = args_pw["password"]

        # No password value was shipped
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_set_password(action_event)
        connect_exporter.assert_called()

        # New password was generated
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        pw2 = args_pw["password"]

        # a new password was created
        assert pw1 != pw2

    @patch("charm.MongoDBConnection")
    @patch("charm.MongodbOperatorCharm._connect_mongodb_exporter")
    def test_event_any_unit_can_get_password_secrets(self, connect_exporter, connection):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        self._setup_secrets()

        action_event = mock.Mock()
        action_event.set_results = MagicMock()

        # Getting current password
        action_event.params = {"username": "monitor"}
        self.harness.charm._on_get_password(action_event)
        args_pw = action_event.set_results.call_args.args[0]
        assert "password" in args_pw
        assert args_pw["password"]

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_set_backup_password_pbm_busy(self, pbm_status):
        """Tests changes to passwords fail when pbm is restoring/backing up."""
        self.harness.set_leader(True)
        original_password = "pass123"
        action_event = mock.Mock()

        for username in ["backup", "monitor", "operator"]:
            self.harness.charm.app_peer_data[f"{username}-password"] = original_password
            action_event.params = {"username": username}
            pbm_status.return_value = MaintenanceStatus("pbm")
            self.harness.charm._on_set_password(action_event)
            current_password = self.harness.charm.app_peer_data[f"{username}-password"]
            action_event.fail.assert_called()
            self.assertEqual(current_password, original_password)

    @patch("config.Config.ENV_VAR_PATH", "tests/unit/data/env.txt")
    def test_auth_not_enabled(self):
        self.assertEqual(self.harness.charm.auth_enabled(), False)

    @patch("config.Config.ENV_VAR_PATH", "tests/unit/data/env_auth.txt")
    def test_auth_enabled(self):
        self.assertEqual(self.harness.charm.auth_enabled(), True)

    @patch("charm.snap.SnapCache")
    def test_connect_mongodb_exporter_no_pass(
        self,
        snap_cache,
    ):
        """Verifies that mongodb exporter is not started without password."""
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap_cache.return_value = {"charmed-mongodb": mock_mongodb_snap}

        self.harness.charm._connect_mongodb_exporter()
        mock_mongodb_snap.restart.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    def test_unit_host(self):
        """Tests that get hosts returns the current unit hosts."""
        assert self.harness.charm.unit_host(self.harness.charm.unit) == "1.1.1.1"
