# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import unittest
from unittest import mock
from unittest.mock import PropertyMock, patch

import pytest

# from charms.operator_libs_linux.v2 import snap
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus
from ops.testing import ActionFailed, Harness
from parameterized import parameterized
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure
from single_kernel_mongo.config.literals import OS_REQUIREMENTS, Scope
from single_kernel_mongo.exceptions import WorkloadExecError, WorkloadServiceError
from single_kernel_mongo.utils.mongo_connection import NotReadyError
from single_kernel_mongo.utils.mongodb_users import (
    BackupUser,
    MonitorUser,
    OperatorUser,
)

from charm import MongoDBVMCharm

from .helpers import patch_network_get

logger = logging.getLogger()

PEER_ADDR = {"private-address": "127.4.5.6"}

PYMONGO_EXCEPTIONS = [
    ConnectionFailure("error message"),
    ConfigurationError("error message"),
    OperationFailure("error message"),
]

S3_RELATION_NAME = "s3-credentials"


@pytest.fixture(autouse=True)
def tenacity_wait(mocker):
    mocker.patch("tenacity.nap.time")


@pytest.fixture(autouse=True)
def mongod_ready(mocker):
    mocker.patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
        return_value=True,
    )


class TestCharm(unittest.TestCase):
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def setUp(self, *unused):
        self.harness = Harness(MongoDBVMCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()
        with self.harness.hooks_disabled():
            self.harness.add_storage(storage_name="mongodb", count=1, attach=True)
        self.peer_rel_id = self.harness.add_relation("database-peers", "database-peers")
        self.peer_rel_id = self.harness.add_relation("upgrade-version-a", "upgrade-version-a")

    @pytest.fixture
    def use_caplog(self, caplog):
        self._caplog = caplog

    def _setup_secrets(self):
        self.harness.set_leader(True)
        self.harness.set_leader(False)

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    def test_on_start_not_leader_doesnt_initialise_replica_set(self):
        """Tests that a non leader unit does not initialise the replica set."""
        with (
            patch(
                "single_kernel_mongo.managers.mongo.MongoManager.initialise_replica_set"
            ) as patched_mongo_initialise,
            patch("single_kernel_mongo.core.vm_workload.VMWorkload.start") as patched_start,
        ):
            self._setup_secrets()

            self.harness.charm.on.start.emit()
            patched_start.assert_called()
            patched_mongo_initialise.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    def test_on_start_snap_failure_leads_to_blocked_status(
        self,
    ):
        """Test failures on systemd result in blocked status."""
        self.harness.set_leader(True)
        with patch(
            "single_kernel_mongo.core.vm_workload.VMWorkload.start",
            side_effect=WorkloadServiceError,
        ):
            self.harness.charm.on.start.emit()
            self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    def test_on_start_mongod_not_ready_defer(
        self,
    ):
        """Test verifies that we wait to initialise replica set when mongod is not running."""
        self.harness.set_leader(True)

        with patch(
            "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
            new_callable=PropertyMock(return_value=False),
        ):
            self.harness.charm.on.start.emit()
            self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    def test_start_unable_to_open_tcp_moves_to_blocked(
        self,
    ):
        """Test verifies that if TCP port cannot be opened we go to the blocked state."""

        def mock_exec(command, *_, **__):
            if command[0] == "open-port":
                raise WorkloadExecError("open-port", 1, None, None)

        self.harness.set_leader(True)

        self.harness.charm.workload.exec = mock_exec
        self.harness.charm.on.start.emit()

        self.assertEqual(
            self.harness.charm.unit.status,
            BlockedStatus("failed to open TCP port for MongoDB"),
        )

    @patch_network_get(private_address="1.1.1.1")
    def test_install_snap_packages_failure(
        self,
    ):
        """Test verifies the correct functions get called when installing apt packages."""
        with patch(
            "single_kernel_mongo.core.vm_workload.VMWorkload.install",
            return_value=False,
        ):
            self.harness.charm.on.install.emit()
            self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.lib.charms.operator_libs_linux.v0.sysctl.Config.configure")
    @patch("single_kernel_mongo.core.vm_workload.VMWorkload.install", return_value=True)
    @pytest.mark.usefixtures("mock_fs_interactions")
    def test_install_sysctl(self, _install, patched_os_config):
        """Test verifies the correct functions get called when installing apt packages."""
        self.harness.charm.on.install.emit()
        patched_os_config.assert_called_once_with(OS_REQUIREMENTS)

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.status.StatusManager.process_and_share_statuses")
    def test_app_hosts(self, *unused):
        rel_id = self.harness.charm.model.get_relation("database-peers").id
        self.harness.add_relation_unit(rel_id, "mongodb/1")
        self.harness.update_relation_data(rel_id, "mongodb/1", PEER_ADDR)

        resulting_ips = self.harness.charm.operator.state.app_hosts
        expected_ips = {"127.4.5.6", "1.1.1.1"}
        self.assertEqual(resulting_ips, expected_ips)

    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection")
    def test_mongodb_relation_joined_non_leader_does_nothing(self, connection):
        """Test verifies that non-leader units don't reconfigure the replica set on joined."""
        rel = self.harness.charm.model.get_relation("database-peers")
        self.harness.set_leader(False)
        self.harness.charm.on.database_peers_relation_joined.emit(relation=rel)
        connection.return_value.__enter__.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
        new_callable=PropertyMock(return_value=False),
    )
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_members",
        return_value={"1.1.1.1"},
    )
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.add_replset_member")
    @patch("ops.framework.EventBase.defer")
    def test_mongodb_relation_joined_all_replicas_not_ready(
        self, defer, add_replset_member, *unused
    ):
        """Tests that we go into waiting when current ReplicaSet hosts are not ready.

        Tests the scenario that if current replica set hosts are not ready, the leader goes into
        WaitingStatus and no attempt to reconfigure is made.
        """
        # preset values
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True

        # simulate 2nd MongoDB unit
        rel = self.harness.charm.model.get_relation("database-peers")
        self.harness.add_relation_unit(rel.id, "mongodb/1")
        self.harness.update_relation_data(rel.id, "mongodb/1", PEER_ADDR)

        # verify we go into waiting and don't reconfigure
        self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))
        add_replset_member.assert_not_called()
        defer.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.add_replset_member",
    )
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.remove_replset_member",
    )
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_members",
    )
    @patch("ops.framework.EventBase.defer")
    def test_relation_joined_get_members_failure(
        self,
        defer,
        get_replset_members,
        rm_replset_member,
        add_replset_member,
    ):
        """Tests reconfigure does not execute when unable to get the replica set members.

        Verifies in case of relation_joined and relation departed, that when the the database
        cannot retrieve the replica set members that no attempts to remove/add units are made and
        that the the event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        rel = self.harness.charm.model.get_relation("database-peers")

        for exception in PYMONGO_EXCEPTIONS:
            get_replset_members.side_effect = exception

            # test both relation events
            for departed in [False, True]:
                if departed:
                    # simulate removing 2nd MongoDB unit
                    self.harness.remove_relation_unit(rel.id, "mongodb/1")
                    add_replset_member.assert_not_called()
                else:
                    # simulate 2nd MongoDB unit joining
                    self.harness.add_relation_unit(rel.id, "mongodb/1")
                    self.harness.update_relation_data(rel.id, "mongodb/1", PEER_ADDR)
                    rm_replset_member.assert_not_called()
            defer.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.add_replset_member",
    )
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_members",
    )
    @patch("ops.framework.EventBase.defer")
    def test_reconfigure_add_member_failure(self, defer, get_members, add_member):
        """Tests reconfigure does not proceed when unable to add a member.

        Verifies in relation joined events, that when the database cannot add a member that the
        event is deferred.
        """
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        get_members.return_value = {"1.1.1.1"}
        rel = self.harness.charm.model.get_relation("database-peers")

        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append(NotReadyError)
        for exception in exceptions:
            add_member.side_effect = exception

            # simulate 2nd MongoDB unit joining( need a unit to join before removing a unit)
            self.harness.add_relation_unit(rel.id, "mongodb/1")
            self.harness.update_relation_data(rel.id, "mongodb/1", PEER_ADDR)

            add_member.assert_called()
            defer.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.init_replset",
    )
    @patch(
        "single_kernel_mongo.managers.mongo.MongoManager.initialise_operator_user",
    )
    def test_initialise_replica_failure_leads_to_waiting_state(
        self,
        init_admin,
        init_replset,
        *unused,
    ):
        """Tests that failure to initialise replica set goes into Waiting Status."""
        self.harness.set_leader(True)

        for exception in PYMONGO_EXCEPTIONS:
            init_replset.side_effect = exception
            self.harness.charm.on.start.emit()
            init_replset.assert_called()
            init_admin.assert_not_called()
            assert isinstance(self.harness.charm.unit.status, WaitingStatus)
            self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.get_status")
    def test_update_status_mongodb_error(
        self,
        get_mongodb_status,
        get_pbm_status,
    ):
        """Tests that when MongoDB is not active, that is reported instead of pbm."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True

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
                self.harness.charm.status_manager.process_and_share_statuses()
                self.assertEqual(self.harness.charm.unit.status, mongodb_status)

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.get_status")
    def test_update_status_pbm_error(
        self,
        get_mongodb_status,
        get_pbm_status,
    ):
        """Tests when MongoDB is active and pbm is in the error state, pbm status is reported."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True

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
                self.harness.charm.status_manager.process_and_share_statuses()
                self.assertEqual(self.harness.charm.unit.status, pbm_status)

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.get_status")
    def test_update_status_pbm_and_mongodb_ready(
        self,
        get_mongodb_status,
        get_pbm_status,
    ):
        """Tests when both Mongodb and pbm are ready that MongoDB status is reported."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True

        self.harness.add_relation(S3_RELATION_NAME, "s3-integrator")

        get_pbm_status.return_value = ActiveStatus("pbm")
        get_mongodb_status.return_value = ActiveStatus("mongodb")
        self.harness.charm.status_manager.process_and_share_statuses()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus("mongodb"))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.get_status")
    def test_update_status_no_s3(
        self,
        get_mongodb_status,
    ):
        """Tests when the s3 relation isn't present that the MongoDB status is reported."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True

        get_mongodb_status.return_value = ActiveStatus("mongodb")
        self.harness.charm.status_manager.process_and_share_statuses()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus("mongodb"))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_status")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    def test_update_status_primary(
        self,
        get_pbm_status,
        get_replset_status,
    ):
        """Tests that update status identifies the primary unit and updates status."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True

        get_pbm_status.return_value = ActiveStatus("")

        self.harness.set_leader(False)
        get_replset_status.return_value = {"1.1.1.1": "PRIMARY"}
        self.harness.charm.status_manager.process_and_share_statuses()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus("Primary"))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_status")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    def test_update_status_secondary(
        self,
        get_pbm_status,
        get_replset_status,
    ):
        """Tests that update status identifies secondary units and doesn't update status."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        get_pbm_status.return_value = ActiveStatus("")

        self.harness.set_leader(False)
        get_replset_status.return_value = {"1.1.1.1": "SECONDARY"}
        self.harness.charm.status_manager.process_and_share_statuses()
        self.assertEqual(self.harness.charm.unit.status, ActiveStatus(""))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_status")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    def test_update_status_additional_messages(
        self,
        get_pbm_status,
        get_replset_status,
    ):
        """Tests status updates are correct for non-primary and non-secondary cases."""
        # assume leader has already initialised the replica set
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        get_pbm_status.return_value = ActiveStatus("")

        # Case 1: Unit has not been added to replica set yet
        self.harness.set_leader(False)
        get_replset_status.return_value = {}
        self.harness.charm.status_manager.process_and_share_statuses()
        self.assertEqual(self.harness.charm.unit.status, WaitingStatus("Member being added."))

        # Case 2: Unit is being removed from replica set
        get_replset_status.return_value = {"1.1.1.1": "REMOVED"}
        self.harness.charm.status_manager.process_and_share_statuses()
        self.assertEqual(self.harness.charm.unit.status, WaitingStatus("Member is removing..."))

        # Case 3: Member is syncing to replica set
        for syncing_status in ["STARTUP", "STARTUP2", "ROLLBACK", "RECOVERING"]:
            get_replset_status.return_value = {"1.1.1.1": syncing_status}
            self.harness.charm.status_manager.process_and_share_statuses()
            self.assertEqual(self.harness.charm.unit.status, WaitingStatus("Member is syncing..."))

        # Case 4: Unknown status
        get_replset_status.return_value = {"1.1.1.1": "unknown"}
        self.harness.charm.status_manager.process_and_share_statuses()
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("unknown"))

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
        new_callable=PropertyMock(return_value=False),
    )
    def test_update_status_not_ready(self, *unused):
        """Tests that if mongod is not running on this unit it restarts it."""
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True

        self.harness.charm.on.update_status.emit()
        self.assertEqual(
            self.harness.charm.unit.status,
            WaitingStatus("Waiting for MongoDB to start"),
        )

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.primary",
    )
    def test_get_primary_current_unit_primary(self, mock_primary):
        """Tests get primary outputs correct primary when called on a primary replica."""
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        mock_primary.return_value = "1.1.1.1"
        output = self.harness.run_action("get-primary")
        assert output.results["replica-set-primary"] == "mongodb/0"

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.primary",
    )
    def test_get_primary_peer_unit_primary(self, mock_primary):
        """Tests get primary outputs correct primary when called on a secondary replica."""
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        # add peer unit
        rel_id = self.harness.charm.model.get_relation("database-peers").id
        self.harness.add_relation_unit(rel_id, "mongodb/1")
        self.harness.update_relation_data(rel_id, "mongodb/1", {"private-address": "2.2.2.2"})

        # mock out the self unit not being primary but its peer being primary
        mock_primary.return_value = "2.2.2.2"

        output = self.harness.run_action("get-primary")
        assert output.results["replica-set-primary"] == "mongodb/1"

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.primary",
    )
    def test_primary_no_primary(self, mock_primary):
        """Test that that the primary property can handle the case when there is no primary.

        Verifies that when there is no primary, the property _primary returns None.
        """
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        # add peer unit
        rel_id = self.harness.charm.model.get_relation("database-peers").id
        self.harness.add_relation_unit(rel_id, "mongodb/1")
        self.harness.update_relation_data(rel_id, "mongodb/1", {"private-address": "2.2.2.2"})

        # mock out no units being primary
        mock_primary.return_value = None

        # verify no primary identified
        primary = self.harness.charm.operator.primary_unit_name
        self.assertEqual(primary, None)

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.primary",
    )
    def test_primary_failure(self, mock_primary):
        """Tests that when getting the primary fails that no replica is reported as primary."""
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        # verify that we raise the correct exception
        for exception in PYMONGO_EXCEPTIONS:
            mock_primary.side_effect = exception
            self.assertEqual(self.harness.charm.operator.primary_unit_name, None)

    @patch_network_get(private_address="1.1.1.1")
    @pytest.mark.usefixtures("mock_fs_interactions")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.remove_replset_member",
    )
    def test_storage_detaching_failure_does_not_defer(self, remove_replset_member):
        """Test that failure in removing replica does not defer the hook.

        Deferring Storage Detached hooks can result in un-predicable behavior and while it is
        technically possible to defer the event, it shouldn't be. This test verifies that no
        attempt to defer storage detached as made.
        """
        exceptions = PYMONGO_EXCEPTIONS
        exceptions.append(NotReadyError)
        for exception in exceptions:
            remove_replset_member.side_effect = exception
            event = mock.Mock()
            self.harness.charm.on.mongodb_storage_detaching.emit(mock.Mock())
            event.defer.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.core.vm_workload.VMWorkload.run_bin_command")
    def test_start_init_user_after_second_call(self, run):
        """Tests that the creation of the admin user is only performed once.

        Verifies that if the user is already set up, that no attempts to set it up again are
        made.
        """
        self.harness.set_leader(True)

        self.harness.charm.operator.mongo_manager.initialise_operator_user()
        self.assertEqual(
            "operator-user-created"
            in self.harness.charm.operator.state.app_peer_data.relation_data.keys(),
            True,
        )

        self.harness.charm.operator.mongo_manager.initialise_operator_user()
        run.assert_called_once()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    def test_set_password(self, pbm_status, *unused):
        """Tests that a new admin password is generated and is returned to the user."""
        self.harness.set_leader(True)
        pbm_status.return_value = ActiveStatus("pbm")
        original_password = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.APP, "operator-password"
        )
        self.harness.run_action("set-password")
        new_password = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.APP, "operator-password"
        )

        # verify app data is updated and results are reported to user
        self.assertNotEqual(original_password, new_password)

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    def test_set_password_provided(self, pbm_status, *unused):
        """Tests that a given password is set as the new mongodb password."""
        self.harness.set_leader(True)
        pbm_status.return_value = ActiveStatus("pbm")
        output = self.harness.run_action("set-password", {"password": "canonical123"})
        new_password = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.APP, "operator-password"
        )

        # verify app data is updated and results are reported to user
        self.assertEqual("canonical123", new_password)
        assert output.results["password"] == "canonical123"
        assert output.results["secret-id"]

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    def test_set_password_failure(self, pbm_status, set_user_password):
        """Tests failure to reset password does not update app data and failure is reported."""
        self.harness.set_leader(True)
        pbm_status.return_value = ActiveStatus("pbm")
        original_password = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.APP, "operator-password"
        )

        for exception in [PYMONGO_EXCEPTIONS, NotReadyError]:
            set_user_password.side_effect = exception
            with pytest.raises(ActionFailed):
                self.harness.run_action("set-password")
            current_password = self.harness.charm.operator.state.secrets.get_for_key(
                Scope.APP, "operator-password"
            )

            # verify passwords are not updated.
            self.assertEqual(current_password, original_password)

    def test_get_password(self):
        self.harness.set_leader(True)

        assert isinstance(
            self.harness.charm.operator.state.secrets.get_for_key(Scope.APP, "monitor-password"),
            str,
        )
        assert (
            self.harness.charm.operator.state.secrets.get_for_key(Scope.APP, "non-existing")
            is None
        )

        self.harness.charm.operator.state.secrets.set("somekey", "bla", Scope.UNIT)
        assert isinstance(
            self.harness.charm.operator.state.secrets.get_for_key(Scope.UNIT, "somekey"),
            str,
        )
        assert (
            self.harness.charm.operator.state.secrets.get_for_key(Scope.APP, "non-existing")
            is None
        )

    @parameterized.expand([(Scope.APP), (Scope.UNIT)])
    def test_invalid_secret(self, scope):
        with self.assertRaises(TypeError):
            self.harness.charm.operator.state.secrets.set("somekey", 1, Scope.UNIT)

        self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "somekey")
        assert self.harness.charm.operator.state.secrets.get_for_key(scope, "somekey") is None

    @pytest.mark.usefixtures("use_caplog")
    def test_delete_password(self):
        self.harness.set_leader(True)

        assert self.harness.charm.operator.state.get_user_password(MonitorUser)
        self.harness.charm.operator.state.secrets.remove(Scope.APP, "monitor-password")
        assert self.harness.charm.operator.state.get_user_password(MonitorUser) == ""

        assert self.harness.charm.operator.state.secrets.set("somekey", "somesecret", Scope.UNIT)
        self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "somekey")
        assert self.harness.charm.operator.state.secrets.get_for_key(Scope.UNIT, "somekey") is None

        with self._caplog.at_level(logging.ERROR):
            self.harness.charm.operator.state.secrets.remove(Scope.APP, "monitor-password")
            assert (
                "Non-existing secret app:monitor-password was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "somekey")
            assert (
                "Non-existing secret unit:somekey was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.operator.state.secrets.remove(Scope.APP, "non-existing-secret")
            assert (
                "Non-existing secret app:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

            self.harness.charm.operator.state.secrets.remove(Scope.UNIT, "non-existing-secret")
            assert (
                "Non-existing secret unit:non-existing-secret was attempted to be removed."
                in self._caplog.text
            )

    def test_delete_password_non_leader(self):
        self._setup_secrets()
        self.harness.set_leader(False)
        assert self.harness.charm.operator.state.get_user_password(MonitorUser)
        with self.assertRaises(RuntimeError):
            self.harness.charm.operator.state.secrets.remove(Scope.APP, "monitor-password")

    @parameterized.expand([(Scope.APP), (Scope.UNIT)])
    @patch("single_kernel_mongo.status.StatusManager.process_and_share_statuses")
    @patch("single_kernel_mongo.managers.config.BackupConfigManager.configure_and_restart")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_on_secret_changed(self, scope, connect_exporter, connect_backup, *unused):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        secret = self.harness.charm.operator.state.secrets.set("new-secret", "bla", scope)
        secret = self.harness.charm.model.get_secret(label=secret.label)

        self.harness.charm.on.secret_changed.emit(label=secret.label, id=secret.id)
        connect_exporter.assert_called()
        connect_backup.assert_called()

    @parameterized.expand([(Scope.APP), (Scope.UNIT)])
    @pytest.mark.usefixtures("use_caplog")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_on_other_secret_changed(self, scope, connect_exporter):
        """NOTE: currently ops.testing seems to allow for non-leader to set secrets too!"""
        # "Hack": creating a secret outside of the normal MongodbOperatorCharm.set_secret workflow
        scope_obj = self.harness.charm.app if scope == Scope.APP else self.harness.charm.unit
        secret = scope_obj.add_secret({"key": "value"})

        with self._caplog.at_level(logging.DEBUG):
            self.harness.charm.on.secret_changed.emit(label=secret.label, id=secret.id)
            assert f"Secret {secret.id} changed, but it's unknown" in self._caplog.text

        connect_exporter.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_connect_to_mongo_exporter_on_set_password(
        self, connect_exporter, mock_set_user_password
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'monitor' user."""
        self.harness.set_leader(True)

        self.harness.run_action("set-password", {"username": "monitor"})
        connect_exporter.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_event_auto_reset_password_secrets_when_no_pw_value_shipped(
        self, connect_exporter, set_user_password
    ):
        """Test _connect_mongodb_exporter is called when the password is set for 'montior' user.

        Furthermore: in Juju 3.x we want to use secrets
        """
        self.harness.set_leader(True)

        # Getting current password
        params = {"username": "monitor"}
        output = self.harness.run_action("set-password", params)
        assert output.results["password"]
        pw1 = output.results["password"]

        connect_exporter.assert_called()

        # New password was generated
        params = {"username": "monitor"}
        output = self.harness.run_action("set-password", params)
        assert output.results["password"]
        pw2 = output.results["password"]

        # a new password was created
        assert pw1 != pw2

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.set_user_password")
    @patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    def test_event_any_unit_can_get_password_secrets(self, connect_exporter, set_user_password):
        """Test that a non leader unit can get the password."""
        self._setup_secrets()

        # Getting current password
        output = self.harness.run_action("get-password", {"username": "monitor"})
        assert output.results["password"]

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.managers.backups.BackupManager.get_status")
    def test_set_backup_password_pbm_busy(self, pbm_status):
        """Tests changes to passwords fail when pbm is restoring/backing up."""
        self.harness.set_leader(True)

        pbm_status.return_value = MaintenanceStatus("pbm")
        for user in [BackupUser, MonitorUser, OperatorUser]:
            original_password = self.harness.charm.operator.state.get_user_password(user)
            with pytest.raises(ActionFailed):
                self.harness.run_action("set-password", {"username": user.username})
            current_password = self.harness.charm.operator.state.get_user_password(user)
            self.assertEqual(current_password, original_password)

    @patch_network_get(private_address="1.1.1.1")
    def test_unit_host(self):
        """Tests that get hosts returns the current unit hosts."""
        assert self.harness.charm.operator.state.unit_peer_data.internal_address == "1.1.1.1"
