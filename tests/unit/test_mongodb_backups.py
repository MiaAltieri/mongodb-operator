# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import subprocess
import unittest
from unittest import mock
from unittest.mock import patch

import tenacity
from charms.mongodb.v1.helpers import current_pbm_op
from charms.mongodb.v1.mongodb_backups import (
    INVALID_INTEGRATION_STATUS,
    PBMBusyError,
    ResyncError,
    SetPBMConfigError,
    stop_after_attempt,
    wait_fixed,
)
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    ModelError,
    WaitingStatus,
)
from ops.pebble import ExecError
from ops.testing import Harness

from charm import MongodbOperatorCharm

from .helpers import patch_network_get

RELATION_NAME = "s3-credentials"


class TestMongoBackups(unittest.TestCase):
    @patch("charm.get_charm_revision")
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self, *unused):
        self.harness = Harness(MongodbOperatorCharm)
        self.harness.begin()
        self.harness.add_relation("database-peers", "database-peers")
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    def test_relation_joined_to_blocked_if_shard(
        self,
    ):
        def is_shard_mock_call(role_name: str):
            return role_name == "shard"

        self.harness.charm.is_role = is_shard_mock_call
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        relation = self.harness.charm.model.get_relation(RELATION_NAME)
        self.harness.charm.on[RELATION_NAME].relation_joined.emit(relation=relation)
        assert self.harness.charm.unit.status == INVALID_INTEGRATION_STATUS

    def test_credentials_changed_to_blocked_if_shard(self):
        def is_shard_mock_call(role_name: str):
            return role_name == "shard"

        self.harness.charm.is_role = is_shard_mock_call
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        relation = self.harness.charm.model.get_relation(RELATION_NAME)
        self.harness.charm.backups.s3_client.on.credentials_changed.emit(relation=relation)
        assert self.harness.charm.unit.status == INVALID_INTEGRATION_STATUS

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_get_pbm_status_snap_not_present(self, pbm_command, service):
        """Tests that when the snap is not present pbm is in blocked state."""
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")

        pbm_command.side_effect = ModelError("service pbm-agent not found")
        self.assertTrue(isinstance(self.harness.charm.backups.get_pbm_status(), BlockedStatus))

    @patch("charm.MongoDBBackups.are_s3_configurations_provided", return_value=True)
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_get_pbm_status_resync(self, pbm_command, service, *unused):
        """Tests that when pbm is resyncing that pbm is in waiting state."""
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")

        service.return_value = True
        pbm_command.return_value = (
            '{"running":{"type":"resync","opID":"64f5cc22a73b330c3880e3b2"}}'
        )
        self.assertTrue(isinstance(self.harness.charm.backups.get_pbm_status(), WaitingStatus))

    @patch("charm.MongoDBBackups.are_s3_configurations_provided", return_value=True)
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_get_pbm_status_running(self, pbm_command, service, *unused):
        """Tests that when pbm not running an op that pbm is in active state."""
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")

        service.return_value = True
        pbm_command.return_value = '{"running":{}}'
        self.assertTrue(isinstance(self.harness.charm.backups.get_pbm_status(), ActiveStatus))

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_get_pbm_status_incorrect_cred(self, pbm_command, service):
        """Tests that when pbm has incorrect credentials that pbm is in blocked state."""
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")

        service.return_value = True
        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "status"],
            exit_code=1,
            stdout="status code: 403",
            stderr="",
        )
        self.assertTrue(isinstance(self.harness.charm.backups.get_pbm_status(), BlockedStatus))

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_get_pbm_status_incorrect_conf(self, pbm_command, service):
        """Tests that when pbm has incorrect configs that pbm is in blocked state."""
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")

        service.return_value = True
        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "status"],
            exit_code=1,
            stdout="status code: 404",
            stderr="",
        )
        self.assertTrue(isinstance(self.harness.charm.backups.get_pbm_status(), BlockedStatus))

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_get_pbm_status_no_config(self, pbm_command, service):
        """Tests when configurations for pbm are not given through S3 there is no status."""
        self.assertTrue(self.harness.charm.backups.get_pbm_status() is None)

    @patch("charm.snap.SnapCache")
    @patch("charms.mongodb.v1.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v1.mongodb_backups.stop_after_attempt")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_verify_resync_config_error(self, pbm_command, service, retry_wait, retry_stop, snap):
        """Tests that when pbm cannot perform the resync command it raises an error."""
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}
        service.return_value = True
        pbm_command.side_effect = ExecError(
            command=["pbm status"], exit_code=1, stdout="", stderr=""
        )

        retry_stop.return_value = stop_after_attempt(1)
        retry_wait.return_value = wait_fixed(1)

        with self.assertRaises(ExecError):
            self.harness.charm.backups._resync_config_options()

    @patch("charm.snap.SnapCache")
    @patch("charms.mongodb.v1.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v1.mongodb_backups.stop_after_attempt")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_verify_resync_cred_error(self, pbm_command, service, retry_wait, retry_stop, snap):
        """Tests that when pbm cannot resync due to creds that it raises an error."""
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}
        service.return_value = True
        retry_stop.return_value = stop_after_attempt(1)
        retry_wait.return_value = wait_fixed(1)
        pbm_command.side_effect = ExecError(
            command=["pbm status"], exit_code=1, stdout="status code: 403", stderr=""
        )

        with self.assertRaises(ExecError):
            self.harness.charm.backups._resync_config_options()

    @patch("charm.snap.SnapCache")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_verify_resync_syncing(self, pbm_status, run_pbm_command, service, snap):
        """Tests that when pbm is syncing that it raises an error."""
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}
        service.return_value = True
        pbm_status.return_value = MaintenanceStatus()
        run_pbm_command.return_value = (
            '{"running":{"type":"resync","opID":"64f5cc22a73b330c3880e3b2"}}'
        )

        # disable retry
        self.harness.charm.backups._wait_pbm_status.retry.retry = tenacity.retry_if_not_result(
            lambda x: True
        )

        with self.assertRaises(PBMBusyError):
            self.harness.charm.backups._resync_config_options()

    @patch("charm.snap.SnapCache")
    @patch("charms.mongodb.v1.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v1.mongodb_backups.stop_after_attempt")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_resync_config_options_failure(
        self, pbm_status, service, retry_stop, retry_wait, snap
    ):
        """Verifies _resync_config_options raises an error when a resync cannot be performed."""
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}
        service.return_value = True
        pbm_status.return_value = MaintenanceStatus()

        with self.assertRaises(PBMBusyError):
            self.harness.charm.backups._resync_config_options()

    @patch("charm.snap.SnapCache")
    @patch("charms.mongodb.v1.mongodb_backups.wait_fixed")
    @patch("charms.mongodb.v1.mongodb_backups.stop_after_attempt")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_resync_config_restart(self, pbm_status, service, retry_stop, retry_wait, snap):
        """Verifies _resync_config_options restarts that snap if alreaady resyncing."""
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        mock_mongodb_snap.restart = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}
        service.return_value = True
        retry_stop.return_value = stop_after_attempt(1)
        retry_stop.return_value = wait_fixed(1)
        pbm_status.return_value = WaitingStatus()

        with self.assertRaises(PBMBusyError):
            self.harness.charm.backups._resync_config_options()

        mock_mongodb_snap.restart.assert_called()

    @patch("charm.snap.SnapCache")
    @patch("charm.MongoDBBackups._get_pbm_configs")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    @patch("charm.MongodbOperatorCharm.clear_pbm_config_file")
    def test_set_config_options(self, clear_config, run_pbm_command, pbm_configs, snap):
        """Verifies _set_config_options failure raises SetPBMConfigError."""
        mock_mongodb_snap = mock.Mock()
        mock_mongodb_snap.present = True
        mock_mongodb_snap.start = mock.Mock()
        snap.return_value = {"charmed-mongodb": mock_mongodb_snap}
        run_pbm_command.side_effect = subprocess.CalledProcessError(
            cmd=["/usr/bin/pbm config --set this_key=doesnt_exist"],
            returncode=42,
        )
        pbm_configs.return_value = {"this_key": "doesnt_exist"}
        with self.assertRaises(SetPBMConfigError):
            self.harness.charm.backups._set_config_options()

    def test_backup_without_rel(self):
        """Verifies no backups are attempted without s3 relation."""
        action_event = mock.Mock()
        action_event.params = {}

        self.harness.charm.backups._on_create_backup_action(action_event)
        action_event.fail.assert_called()

    @patch("ops.framework.EventBase.defer")
    def test_s3_credentials_no_db(self, defer):
        """Verifies that when there is no DB that setting credentials is deferred."""
        del self.harness.charm.app_peer_data["db_initialised"]

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBBackups._set_config_options")
    def test_s3_credentials_set_pbm_failure(self, _set_config_options, service):
        """Test charm goes into blocked state when setting pbm configs fail."""
        service.return_value = True

        _set_config_options.side_effect = SetPBMConfigError
        self.harness.charm.app_peer_data["db_initialised"] = "true"

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_s3_credentials_config_error(
        self, pbm_status, service, defer, resync, _set_config_options
    ):
        """Test charm defers when more time is needed to sync pbm."""
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        service.return_value = True
        pbm_status.return_value = ActiveStatus()
        resync.side_effect = SetPBMConfigError

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )
        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_s3_credentials_syncing(self, pbm_status, service, defer, resync, _set_config_options):
        """Test charm defers when more time is needed to sync pbm credentials."""
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        service.return_value = True
        resync.side_effect = ResyncError

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_called()
        self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_s3_credentials_pbm_busy(
        self, pbm_status, service, defer, resync, _set_config_options
    ):
        """Test charm defers when more time is needed to sync pbm."""
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        service.return_value = True

        resync.side_effect = PBMBusyError

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_called()
        self.assertTrue(isinstance(self.harness.charm.unit.status, WaitingStatus))

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBBackups._set_config_options")
    @patch("charm.MongoDBBackups._resync_config_options")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_s3_credentials_pbm_error(
        self, pbm_command, service, defer, resync, _set_config_options
    ):
        """Test charm defers when more time is needed to sync pbm."""
        service.return_value = True
        self.harness.charm.app_peer_data["db_initialised"] = "true"
        resync.side_effect = ExecError(
            command=["/usr/bin/pbm status"],
            exit_code=1,
            stdout="status code: 403",
            stderr="",
        )
        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm status"],
            exit_code=1,
            stdout="status code: 403",
            stderr="",
        )

        # triggering s3 event with correct fields
        mock_s3_info = mock.Mock()
        mock_s3_info.return_value = {"access-key": "noneya", "secret-key": "business"}
        self.harness.charm.backups.s3_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator/0",
            {"bucket": "hat"},
        )

        defer.assert_not_called()
        self.assertTrue(isinstance(self.harness.charm.unit.status, BlockedStatus))

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_backup_failed(self, pbm_status, pbm_command, service):
        """Verifies backup is fails if the pbm command failed."""
        service.return_value = True

        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "status"],
            exit_code=1,
            stdout="status code: 42",
            stderr="",
        )

        action_event = mock.Mock()
        action_event.params = {}
        pbm_status.return_value = ActiveStatus("")

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)

        action_event.fail.assert_called()

    def test_backup_list_without_rel(self):
        """Verifies no backup lists are attempted without s3 relation."""
        action_event = mock.Mock()
        action_event.params = {}

        self.harness.charm.backups._on_list_backups_action(action_event)
        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_backup_list_syncing(self, pbm_command, service):
        """Verifies backup list is deferred if more time is needed to resync."""
        service.return_value = True

        action_event = mock.Mock()
        action_event.params = {}

        pbm_command.return_value = (
            '{"running":{"type":"resync","opID":"64f5cc22a73b330c3880e3b2"}}'
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_list_backups_action(action_event)

        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_backup_list_wrong_cred(self, pbm_command, service):
        """Verifies backup list fails with wrong credentials."""
        action_event = mock.Mock()
        action_event.params = {}

        service.return_value = True
        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "status"],
            exit_code=1,
            stdout="status code: 403",
            stderr="",
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_list_backups_action(action_event)
        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_backup_list_failed(self, pbm_status, pbm_command, service):
        """Verifies backup list fails if the pbm command fails."""
        mock_pbm_snap = mock.Mock()
        mock_pbm_snap.present = True

        action_event = mock.Mock()
        action_event.params = {}
        pbm_status.return_value = ActiveStatus("")

        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "list"],
            exit_code=1,
            stdout="status code: 403",
            stderr="",
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_list_backups_action(action_event)

        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_generate_backup_list_output(self, run_pbm_command):
        """Tests correct formation of backup list output.

        Specifically the spacing of the backups, the header, the backup order, and the backup
        contents.
        """
        # case 1: running backup is listed in error state
        with open("tests/unit/data/pbm_status_duplicate_running.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        run_pbm_command.return_value = output_contents.encode("utf-8")
        formatted_output = self.harness.charm.backups._generate_backup_list_output()
        formatted_output = formatted_output.split("\n")
        header = formatted_output[0]
        self.assertEqual(header, "backup-id             | backup-type  | backup-status")
        divider = formatted_output[1]
        self.assertEqual(divider, "-" * len(header))
        eariest_backup = formatted_output[2]
        self.assertEqual(
            eariest_backup,
            "1900-02-14T13:59:14Z  | physical     | failed: internet not invented yet",
        )
        failed_backup = formatted_output[3]
        self.assertEqual(failed_backup, "2000-02-14T14:09:43Z  | logical      | finished")
        inprogress_backup = formatted_output[4]
        self.assertEqual(inprogress_backup, "2023-02-14T17:06:38Z  | logical      | in progress")

        # case 2: running backup is not listed in error state
        with open("tests/unit/data/pbm_status.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        run_pbm_command.return_value = output_contents.encode("utf-8")
        formatted_output = self.harness.charm.backups._generate_backup_list_output()
        formatted_output = formatted_output.split("\n")
        header = formatted_output[0]
        self.assertEqual(header, "backup-id             | backup-type  | backup-status")
        divider = formatted_output[1]
        self.assertEqual(
            divider, "-" * len("backup-id             | backup-type  | backup-status")
        )
        eariest_backup = formatted_output[2]
        self.assertEqual(
            eariest_backup,
            "1900-02-14T13:59:14Z  | physical     | failed: internet not invented yet",
        )
        failed_backup = formatted_output[3]
        self.assertEqual(failed_backup, "2000-02-14T14:09:43Z  | logical      | finished")
        inprogress_backup = formatted_output[4]
        self.assertEqual(inprogress_backup, "2023-02-14T17:06:38Z  | logical      | in progress")

    def test_restore_without_rel(self):
        """Verifies no restores are attempted without s3 relation."""
        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}

        self.harness.charm.backups._on_restore_action(action_event)
        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_restore_syncing(self, pbm_command, service):
        """Verifies restore is deferred if more time is needed to resync."""
        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        service.return_value = "pbm"
        pbm_command.return_value = (
            '{"running":{"type":"resync","opID":"64f5cc22a73b330c3880e3b2"}}'
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)

        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_restore_running_backup(self, pbm_command, service):
        """Verifies restore is fails if another backup is already running."""
        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        service.return_value = "pbm"
        pbm_command.return_value = (
            'Currently running:\n====\nSnapshot backup "2023-08-21T13:08:22Z"'
        )
        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)

        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    @patch("charm.MongoDBBackups.get_pbm_status")
    def test_restore_wrong_cred(self, pbm_status, pbm_command, service):
        """Verifies restore is fails if the credentials are incorrect."""
        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        action_event = mock.Mock()
        action_event.params = {}
        pbm_status.return_value = ActiveStatus("")

        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "list"],
            exit_code=1,
            stdout="status code: 403",
            stderr="",
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)
        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    @patch("charm.MongoDBBackups.get_pbm_status")
    @patch("charm.MongoDBBackups._needs_provided_remap_arguments")
    def test_restore_failed(self, remap, pbm_status, pbm_command, service):
        """Verifies restore is fails if the pbm command failed."""
        action_event = mock.Mock()
        action_event.params = {"backup-id": "back-me-up"}
        pbm_status.return_value = ActiveStatus("")

        pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm", "restore"], exit_code=1, stdout="failed", stderr=""
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_restore_action(action_event)

        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_remap_replicaset_no_backup(self, run_pbm_command):
        """Test verifies that no remapping is given if the backup_id doesn't exist."""
        with open("tests/unit/data/pbm_status.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        run_pbm_command.return_value = output_contents.encode("utf-8")
        remap = self.harness.charm.backups._remap_replicaset("this-id-doesnt-exist")
        self.assertEqual(remap, "")

    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_remap_replicaset_no_remap_necessary(self, run_pbm_command):
        """Test verifies that no remapping is given if no remapping is necessary."""
        with open("tests/unit/data/pbm_status_error_remap.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        run_pbm_command.return_value = output_contents.encode("utf-8")

        # first case is that the backup is not in the error state
        remap = self.harness.charm.backups._remap_replicaset("2000-02-14T14:09:43Z")
        self.assertEqual(remap, "")

        # second case is that the backup has an error not related to remapping
        remap = self.harness.charm.backups._remap_replicaset("1900-02-14T13:59:14Z")
        self.assertEqual(remap, "")

        # third case is that the backup has two errors one related to remapping and another
        # related to something else
        remap = self.harness.charm.backups._remap_replicaset("2001-02-14T13:59:14Z")
        self.assertEqual(remap, "")

    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_remap_replicaset_remap_necessary(self, run_pbm_command):
        """Test verifies that remapping is provided and correct when necessary."""
        with open("tests/unit/data/pbm_status_error_remap.txt") as f:
            output_contents = f.readlines()
            output_contents = "".join(output_contents)

        run_pbm_command.return_value = output_contents.encode("utf-8")
        self.harness.charm.app.name = "current-app-name"

        # first case is that the backup is not in the error state
        remap = self.harness.charm.backups._remap_replicaset("2002-02-14T13:59:14Z")
        self.assertEqual(remap, "current-app-name=old-cluster-name")

    @patch("charm.MongoDBBackups.are_s3_configurations_provided", return_value=True)
    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_get_pbm_status_backup(self, run_pbm_command, service, *unused):
        """Tests that when pbm running a backup that pbm is in maintenance state."""
        relation_id = self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")

        service.return_value = "pbm"
        run_pbm_command.return_value = '{"running":{"type":"backup","name":"2023-09-04T12:15:58Z","startTS":1693829759,"status":"oplog backup","opID":"64f5ca7e777e294530289465"}}'
        self.assertTrue(isinstance(self.harness.charm.backups.get_pbm_status(), MaintenanceStatus))

    def test_current_pbm_op(self):
        """Test if _current_pbm_op can identify the operation pbm is running."""
        action = current_pbm_op('{"running":{"type":"my-action"}}')
        self.assertEqual(action, {"type": "my-action"})

        no_action = current_pbm_op('{"running":{}}')
        self.assertEqual(no_action, {})

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_backup_syncing(self, run_pbm_command, service):
        """Verifies backup is deferred if more time is needed to resync."""
        action_event = mock.Mock()
        action_event.params = {}
        service.return_value = "pbm"
        run_pbm_command.return_value = (
            '{"running":{"type":"resync","opID":"64f5cc22a73b330c3880e3b2"}}'
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)

        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_backup_running_backup(self, run_pbm_command, service):
        """Verifies backup is fails if another backup is already running."""
        action_event = mock.Mock()
        action_event.params = {}
        service.return_value = "pbm"
        run_pbm_command.return_value = (
            'Currently running:\n====\nSnapshot backup "2023-08-21T13:08:22Z"'
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)
        action_event.fail.assert_called()

    @patch("charm.MongodbOperatorCharm.has_backup_service")
    @patch("charm.MongodbOperatorCharm.run_pbm_command")
    def test_backup_wrong_cred(self, run_pbm_command, service):
        """Verifies backup is fails if the credentials are incorrect."""
        action_event = mock.Mock()
        action_event.params = {}
        service.return_value = True
        run_pbm_command.side_effect = ExecError(
            command=["/usr/bin/pbm config --set this_key=doesnt_exist"],
            exit_code=403,
            stdout="status code: 403",
            stderr="",
        )

        self.harness.add_relation(RELATION_NAME, "s3-integrator")
        self.harness.charm.backups._on_create_backup_action(action_event)
        action_event.fail.assert_called()

    def test_get_backup_restore_operation_result(self):
        backup_id = "2023-08-21T13:08:22Z"
        current_pbm_status = ActiveStatus("")
        previous_pbm_status = MaintenanceStatus(f"backup started/running, backup id:'{backup_id}'")
        operation_result = self.harness.charm.backups._get_backup_restore_operation_result(
            current_pbm_status, previous_pbm_status
        )
        assert operation_result == f"Backup '{backup_id}' completed successfully"
        previous_pbm_status = MaintenanceStatus(
            f"restore started/running, backup id:'{backup_id}'"
        )
        operation_result = self.harness.charm.backups._get_backup_restore_operation_result(
            current_pbm_status, previous_pbm_status
        )
        assert operation_result == f"Restore from backup '{backup_id}' completed successfully"
