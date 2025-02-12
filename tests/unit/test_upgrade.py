# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest import mock
from unittest.mock import patch

from ops.model import ActiveStatus, BlockedStatus
from ops.testing import Harness

from charm import MongoDBVMCharm

from .helpers import patch_network_get


class TestCharm(unittest.TestCase):
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self, *unused):
        self.harness = Harness(MongoDBVMCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()
        self.peer_rel_id = self.harness.add_relation("database-peers", "database-peers")
        self.peer_rel_id = self.harness.add_relation("upgrade-version-a", "upgrade-version-a")

    @patch_network_get(private_address="1.1.1.1")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.get_replset_status",
    )
    @patch("single_kernel_mongo.utils.mongo_connection.MongoClient")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.is_ready",
    )
    @patch(
        "single_kernel_mongo.core.version_checker.VersionChecker.is_status_related_to_mismatched_revision"
    )
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch(
        "single_kernel_mongo.core.abstract_upgrades.GenericMongoDBUpgradeManager.are_all_units_ready_for_upgrade"
    )
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.is_any_sync")
    def test_is_cluster_healthy(
        self,
        is_any_sync,
        are_all_units_active,
        get_rev,
        is_status_related_to_mismatched_revision,
        is_ready,
        mock_client,
        *unused,
    ):
        """Test is_cluster_healthy function."""

        def is_replication_mock_call(*args):
            return args == ("replication",)

        self.harness.set_leader(True)
        is_status_related_to_mismatched_revision.return_value = False

        mock_units_ready = mock.Mock()
        mock_units_ready.return_value = True
        self.harness.charm.operator.upgrade_manager.are_all_units_ready_for_upgrade = (
            mock_units_ready
        )

        # case 1: unit is not ready after restarting
        is_ready.return_value = False
        assert not self.harness.charm.operator.upgrade_manager.is_cluster_healthy()

        # case 2: cluster is still syncing
        is_ready.return_value = True

        self.harness.charm.operator.state.is_role = is_replication_mock_call
        self.harness.charm.unit.status = ActiveStatus()
        is_any_sync.return_value = True
        assert not self.harness.charm.operator.upgrade_manager.is_cluster_healthy()

        # case 3: unit is not active
        self.harness.charm.unit.status = BlockedStatus()
        is_any_sync.return_value = False
        assert not self.harness.charm.operator.upgrade_manager.is_cluster_healthy()

        # case 4: cluster is healthy
        self.harness.charm.unit.status = ActiveStatus()
        is_any_sync.return_value = False
        mock_client.return_value.admin.command.return_value = mock.Mock()
        result = self.harness.charm.operator.upgrade_manager.is_cluster_healthy()
        assert result

        # case 5: not all units are active
        self.harness.charm.operator.upgrade_manager.are_all_units_ready_for_upgrade.return_value = (
            False
        )
        assert not self.harness.charm.operator.upgrade_manager.is_cluster_healthy()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoClient")
    @patch(
        "single_kernel_mongo.core.abstract_upgrades.GenericMongoDBUpgradeManager.is_write_on_secondaries"
    )
    def test_is_replica_set_able_read_write(self, is_write_on_secondaries, client, connection):
        """Test test_is_replica_set_able_read_write function."""
        self.harness.set_leader(True)
        # case 1: writes are not present on secondaries
        is_write_on_secondaries.return_value = False
        assert not self.harness.charm.operator.upgrade_manager.is_replica_set_able_read_write()

        # case 2: writes are present on secondaries
        is_write_on_secondaries.return_value = True
        assert self.harness.charm.operator.upgrade_manager.is_replica_set_able_read_write()
