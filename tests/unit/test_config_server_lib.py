# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest import mock
from unittest.mock import patch

import pytest
from ops import BlockedStatus, WaitingStatus
from ops.testing import Harness
from single_kernel_mongo.exceptions import (
    DeferrableFailedHookChecksError,
    NonDeferrableFailedHookChecksError,
)

from charm import MongoDBVMCharm

RELATION_NAME = "s3-credentials"


class TestConfigServerInterface(unittest.TestCase):
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def setUp(self, *unused):
        self.harness = Harness(MongoDBVMCharm)
        self.harness.begin()
        self.harness.add_relation("database-peers", "database-peers")
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    def test_on_relation_joined_failed_hook_checks(self):
        """Tests that no relation data is set when cluster joining conditions are not met."""

        def is_not_config_mock_call(*args):
            assert args == ("config-server",)
            return False

        self.harness.charm.operator.state.db_initialised = True

        # fails due to being run on non-config-server
        self.harness.charm.operator.state.is_role = is_not_config_mock_call
        relation_id = self.harness.add_relation("cluster", "mongos")
        self.harness.add_relation_unit(relation_id, "mongos/0")
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data = (
            mock.Mock()
        )
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_not_called()

        # fails because db has not been initialized
        self.harness.charm.operator.state.db_initialised = False

        def is_config_mock_call(*args):
            assert args == ("config-server",)
            return True

        self.harness.charm.is_role = is_config_mock_call
        self.harness.add_relation_unit(relation_id, "mongos/1")
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_not_called()

        # fails because not leader
        self.harness.set_leader(False)
        self.harness.add_relation_unit(relation_id, "mongos/2")
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_not_called()

    @mock.patch("single_kernel_mongo.managers.mongo.MongoManager.reconcile_mongo_users_and_dbs")
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @mock.patch("ops.framework.EventBase.defer")
    def test_relation_changed_fail_if_no_database_field(self, defer, get_charm_rev, oversee):
        def is_config_mock_call(*args):
            return args == ("config-server",)

        self.harness.charm.operator.state.db_initialised = True
        self.harness.set_leader(True)
        self.harness.charm.operator.state.is_role = is_config_mock_call
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data = (
            mock.Mock()
        )

        relation_id = self.harness.add_relation("cluster", "mongos")
        self.harness.add_relation_unit(relation_id, "mongos/0")

        # Fails because there is no `database` field in the relation data
        relation = self.harness.charm.model.get_relation("cluster")
        self.harness.charm.on["cluster"].relation_changed.emit(relation=relation)
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_not_called()

        # Success, we have the for the database field.
        # Note: updating the relation data here triggers a relation_changed event.
        self.harness.update_relation_data(relation_id, "mongos", {"database": "test-database"})
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_called()

    def test_update_rel_data_failed_hook_checks(self):
        """Tests that no relation data is set when the cluster is not ready."""

        def is_not_config_mock_call(*args):
            return args != ("config-server",)

        self.harness.charm.operator.state.db_initialised = True
        self.harness.add_relation("cluster", "mongos")

        # fails due to being run on non-config-server
        self.harness.charm.operator.state.is_role = is_not_config_mock_call
        with pytest.raises(NonDeferrableFailedHookChecksError):
            self.harness.charm.operator.cluster_manager.update_config_server_db()
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data = (
            mock.Mock()
        )
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_not_called()

        def is_config_mock_call(*args):
            return args == ("config-server",)

        # fails because not leader
        self.harness.set_leader(False)
        with pytest.raises(NonDeferrableFailedHookChecksError):
            self.harness.charm.operator.cluster_manager.update_config_server_db()
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_not_called()

        # fails because db has not been initialized
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = False
        self.harness.charm.operator.state.is_role = is_config_mock_call
        with pytest.raises(DeferrableFailedHookChecksError):
            self.harness.charm.operator.cluster_manager.update_config_server_db()
        self.harness.charm.operator.cluster_manager.data_interface.update_relation_data.assert_not_called()

    @patch("data_platform_helpers.version_check.CrossAppVersionChecker.set_version_on_related_app")
    @patch("data_platform_helpers.version_check.get_charm_revision", return_value="1")
    @patch(
        "single_kernel_mongo.core.version_checker.VersionChecker.get_cluster_mismatched_revision_status",
        return_value=None,
    )
    def test_pass_hooks_check_waits_for_start_config_server(
        self, mismatch, get_rev, set_version_on
    ):
        """Ensure that pass_hooks defers until the database is initialized.

        Note: in some cases sharding related hooks execute before config and leader elected hooks,
        therefore it is important that the `pass_hooks_check` defers an event until the database
        has been started
        """

        def is_shard_mock_call(*args):
            return args == ("shard",)

        self.harness.charm.operator.state.is_role = is_shard_mock_call
        relation_id = self.harness.add_relation("sharding", "config-server")
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="sharding"
        )

        self.harness.set_leader(False)
        with pytest.raises(DeferrableFailedHookChecksError):
            self.harness.charm.operator.shard_manager.assert_pass_hook_checks(relation)

        # once the database has been initialised, pass hooks check should no longer defer if the
        # unit is not the leader nor is the wrong wrole
        self.harness.set_leader(True)
        self.harness.charm.operator.state.app_peer_data.db_initialised = True
        self.harness.charm.operator.shard_manager.assert_pass_hook_checks(relation)

    @patch("data_platform_helpers.version_check.CrossAppVersionChecker.set_version_on_related_app")
    @patch("data_platform_helpers.version_check.get_charm_revision", return_value="1")
    @patch(
        "single_kernel_mongo.core.version_checker.VersionChecker.get_cluster_mismatched_revision_status",
        return_value=None,
    )
    def test_pass_hooks_check_waits_for_start_shard(self, mismatch, get_rev, set_rev):
        """Ensure that pass_hooks defers until the database is initialized.

        Note: in some cases sharding related hooks execute before config and leader elected hooks,
        therefore it is important that the `pass_hooks_check` defers an event until the database
        has been started
        """

        def is_config_mock_call(*args):
            return args == ("config-server",)

        self.harness.charm.operator.state.is_role = is_config_mock_call
        relation_id = self.harness.add_relation("config-server", "shard")
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="sharding"
        )

        self.harness.set_leader(False)
        with pytest.raises(DeferrableFailedHookChecksError):
            self.harness.charm.operator.config_server_manager.assert_pass_hook_checks(relation)

        # once the database has been initialised, pass hooks check should no longer defer if the
        # unit is not the leader nor is the wrong wrole
        self.harness.set_leader(True)
        self.harness.charm.operator.state.app_peer_data.db_initialised = True
        self.harness.charm.operator.config_server_manager.assert_pass_hook_checks(relation)

    @patch("data_platform_helpers.version_check.CrossAppVersionChecker.set_version_on_related_app")
    def test_defer_if_no_version_config_server(self, set_rel):
        """Ensure that pass_hooks defers until we have matching versions."""

        def is_config_mock_call(*args):
            return args == ("config-server",)

        def get_cluster_mismatched_revision_status_mock_fail(*unused):
            return WaitingStatus("No info")

        def get_cluster_mismatched_revision_status_mock_success(*unused):
            return None

        self.harness.charm.operator.state.is_role = is_config_mock_call

        self.harness.charm.operator.cluster_version_checker.get_cluster_mismatched_revision_status = (
            get_cluster_mismatched_revision_status_mock_fail
        )

        self.harness.charm.operator.state.app_peer_data.db_initialised = True

        relation_id = self.harness.add_relation("config-server", "shard")
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="config-server"
        )

        with pytest.raises(DeferrableFailedHookChecksError):
            self.harness.charm.operator.config_server_manager.assert_pass_hook_checks(relation)

        # If we return a matching revision status, then we won't defer anymore.
        self.harness.charm.operator.cluster_version_checker.get_cluster_mismatched_revision_status = (
            get_cluster_mismatched_revision_status_mock_success
        )

        self.harness.charm.operator.config_server_manager.assert_pass_hook_checks(relation)

    @patch("data_platform_helpers.version_check.CrossAppVersionChecker.set_version_on_related_app")
    def test_defer_if_no_version_shard(self, set_rel):
        """Ensure that pass_hooks defers until we have matching versions."""

        def is_config_mock_call(*args):
            return args == ("shard",)

        def get_cluster_mismatched_revision_status_mock_fail(*unused):
            return BlockedStatus("No info")

        def get_cluster_mismatched_revision_status_mock_success(*unused):
            return None

        self.harness.charm.operator.state.is_role = is_config_mock_call

        relation_id = self.harness.add_relation("sharding", "config-server")
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="sharding"
        )

        # First, we'll return a blocked status because it should defer.
        self.harness.charm.operator.cluster_version_checker.get_cluster_mismatched_revision_status = (
            get_cluster_mismatched_revision_status_mock_fail
        )

        self.harness.charm.operator.state.app_peer_data.db_initialised = True

        with pytest.raises(DeferrableFailedHookChecksError):
            self.harness.charm.operator.shard_manager.assert_pass_hook_checks(relation)

        # Then, if we return a matching revision status, then we won't defer anymore.
        self.harness.charm.operator.cluster_version_checker.get_cluster_mismatched_revision_status = (
            get_cluster_mismatched_revision_status_mock_success
        )

        self.harness.charm.operator.shard_manager.assert_pass_hook_checks(relation)
