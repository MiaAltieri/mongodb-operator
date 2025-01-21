# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from dataclasses import asdict
from unittest import mock
from unittest.mock import patch

from ops.model import ActiveStatus, BlockedStatus, StatusBase, WaitingStatus
from ops.testing import Harness
from parameterized import parameterized
from single_kernel_mongo.status import Statuses

from charm import MongodbOperatorCharm

from .helpers import patch_network_get

CHARM_VERSION = "127"


class TestCharm(unittest.TestCase):
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self, get_charm_revision):
        get_charm_revision.return_value = CHARM_VERSION
        self.harness = Harness(MongodbOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_are_all_units_ready_for_upgrade(self) -> None:
        """Verify that status handler returns the correct status."""
        # case 1: all juju units are ready for upgrade
        goal_state = {"units": {"unit_0": {"status": "active"}}}
        get_mismatched_revsion = mock.Mock()
        get_mismatched_revsion.return_value = None
        run_mock = mock.Mock()
        run_mock._run.return_value = goal_state
        self.harness.charm.model._backend = run_mock
        self.harness.charm.operator.cluster_version_checker.get_cluster_mismatched_revision_status = (
            get_mismatched_revsion
        )

        assert self.harness.charm.operator.upgrade_manager.are_all_units_ready_for_upgrade()

        # case 2: not all juju units are ready for upgrade
        goal_state = {"units": {"unit_0": {"status": "active"}, "unit_1": {"status": "blocked"}}}
        run_mock = mock.Mock()
        run_mock._run.return_value = goal_state
        self.harness.charm.model._backend = run_mock

        assert not self.harness.charm.operator.upgrade_manager.are_all_units_ready_for_upgrade()

    @parameterized.expand(
        [
            [
                BlockedStatus("Invalid"),
                ActiveStatus(),
                ActiveStatus(),
                ActiveStatus(),
                "mongodb",
            ],
            [
                WaitingStatus("Waiting"),
                ActiveStatus(),
                ActiveStatus(),
                ActiveStatus(),
                "mongodb",
            ],
            [
                ActiveStatus(),
                BlockedStatus("Invalid"),
                ActiveStatus(),
                ActiveStatus(),
                "shard",
            ],
            [
                ActiveStatus(),
                WaitingStatus("Waiting"),
                ActiveStatus(),
                ActiveStatus(),
                "shard",
            ],
            [
                ActiveStatus(),
                None,
                BlockedStatus("Invalid"),
                ActiveStatus(),
                "config_server",
            ],
            [
                ActiveStatus(),
                None,
                WaitingStatus("Waiting"),
                ActiveStatus(),
                "config_server",
            ],
            [ActiveStatus(), None, None, BlockedStatus("Invalid"), "pbm"],
            [ActiveStatus(), None, None, WaitingStatus("Waiting"), "pbm"],
            [ActiveStatus(), None, None, None, "mongodb"],
            [ActiveStatus(), ActiveStatus(), ActiveStatus(), ActiveStatus(), "mongodb"],
        ]
    )
    def test_prioritize_status(
        self,
        mongodb_status: StatusBase,
        shard_status: StatusBase | None,
        config_server_status: StatusBase | None,
        pbm_status: StatusBase | None,
        expected_index: int,
    ):
        """Tests different cases of statuses for prioritize_status."""
        statuses = Statuses(mongodb_status, shard_status, config_server_status, pbm_status)
        assert (
            self.harness.charm.status_manager.prioritize_statuses(statuses)
            == asdict(statuses)[expected_index]
        )

    @parameterized.expand(
        [
            [
                False,
                True,
                None,
                BlockedStatus(
                    "Relation to mongos not supported, config role must be config-server"
                ),
            ],
            [
                False,
                False,
                None,
                BlockedStatus(
                    "Relation to mongos not supported, config role must be config-server"
                ),
            ],
            [
                True,
                False,
                None,
                BlockedStatus(
                    "Relation to s3-integrator is not supported, config role must be config-server"
                ),
            ],
            [True, True, None, None],
            [True, True, ActiveStatus(), ActiveStatus()],
            [True, True, BlockedStatus(""), BlockedStatus("")],
            [True, True, WaitingStatus(""), WaitingStatus("")],
        ]
    )
    def test_get_invalid_integration_status(
        self,
        mongos_integration: bool,
        valid_s3_integration: bool,
        mismatched_revision_status: StatusBase | None,
        expected_status: StatusBase | None,
    ):
        """Tests different cases of statuses for get_invalid_integration_status."""
        get_mismatched_revision_mock = mock.Mock()
        get_mismatched_revision_mock.return_value = mismatched_revision_status

        mongos_integration_mock = mock.Mock()
        mongos_integration_mock.return_value = mongos_integration

        valid_s3_integration_mock = mock.Mock()
        valid_s3_integration_mock.return_value = valid_s3_integration

        self.harness.charm.operator.cluster_version_checker.get_cluster_mismatched_revision_status = (
            get_mismatched_revision_mock
        )
        self.harness.charm.operator.cluster_manager.is_valid_mongos_integration = (
            mongos_integration_mock
        )
        self.harness.charm.operator.backup_manager.is_valid_s3_integration = (
            valid_s3_integration_mock
        )

        self.harness.charm.operator.pass_status_basic_checks()

        assert self.harness.charm.unit.status == expected_status or ActiveStatus("")
