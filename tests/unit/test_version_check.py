# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest.mock import patch

from data_platform_helpers.version_check import (
    DEPLOYMENT_TYPE,
    VERSION_CONST,
    NoVersionError,
)
from ops.testing import Harness

from charm import MongoDBVMCharm

from .helpers import patch_network_get

CHARMHUB_DEPLOYMENT = "ch"
LOCAL_DEPLOYMENT = "local"
RELATION_TO_CHECK_VERSION = "sharding"
CHARM_VERSION = "123"
VALID_VERSION = CHARM_VERSION
INVALID_VERSION = "456"
APP_0 = "APP_0"
APP_1 = "APP_1"
APP_2 = "APP_2"


class TestCharm(unittest.TestCase):
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self, get_charm_revision):
        get_charm_revision.return_value = CHARM_VERSION
        self.harness = Harness(MongoDBVMCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def add_invalid_relation(self, deployment=LOCAL_DEPLOYMENT):
        rel_id = self.harness.add_relation(RELATION_TO_CHECK_VERSION, APP_0)
        self.harness.add_relation_unit(rel_id, f"{APP_0}/0")
        self.harness.update_relation_data(
            rel_id,
            f"{APP_0}",
            {VERSION_CONST: INVALID_VERSION, DEPLOYMENT_TYPE: deployment},
        )

    def add_valid_relation(self, deployment=LOCAL_DEPLOYMENT):
        rel_id = self.harness.add_relation(RELATION_TO_CHECK_VERSION, APP_1)
        self.harness.add_relation_unit(rel_id, f"{APP_1}/0")
        self.harness.update_relation_data(
            rel_id,
            f"{APP_1}",
            {VERSION_CONST: VALID_VERSION, DEPLOYMENT_TYPE: deployment},
        )

    def add_relation_with_no_version(self):
        rel_id = self.harness.add_relation(RELATION_TO_CHECK_VERSION, RELATION_TO_CHECK_VERSION)
        self.harness.add_relation_unit(rel_id, f"{APP_2}/0")

    def test_get_invalid_versions(self):
        """Verifies that get invalid versions returns the expected content."""
        # case one: retrieves invalid versions + valid
        self.add_invalid_relation()
        self.add_valid_relation()
        invalid_version = (
            self.harness.charm.operator.cross_app_version_checker.get_invalid_versions()
        )
        assert invalid_version == [(APP_0, INVALID_VERSION)]

        # case two: missing version info
        self.add_relation_with_no_version()
        with self.assertRaises(NoVersionError):
            self.harness.charm.operator.cross_app_version_checker.get_invalid_versions()

    def test_get_version_of_related_app(self):
        """Verifies that version checker can retrieve integrated application versions."""
        # case one: get version
        self.add_invalid_relation()
        version = self.harness.charm.operator.cross_app_version_checker.get_version_of_related_app(
            APP_0
        )
        assert version == INVALID_VERSION

        # case two: missing version info
        self.add_relation_with_no_version()
        with self.assertRaises(NoVersionError):
            self.harness.charm.operator.cross_app_version_checker.get_version_of_related_app(APP_2)

    def test_is_related_app_locally_built_charm(self):
        """Verifies that version checker can retrieve integrated application deployment types."""
        # case one: local deployment
        self.add_valid_relation(deployment=LOCAL_DEPLOYMENT)
        assert self.harness.charm.operator.cross_app_version_checker.is_local_charm(APP_1)

        # case two: charmhub deployment
        self.add_invalid_relation(deployment=CHARMHUB_DEPLOYMENT)
        assert not self.harness.charm.operator.cross_app_version_checker.is_local_charm(APP_0)

        # case three: missing version info
        self.add_relation_with_no_version()
        with self.assertRaises(NoVersionError):
            self.harness.charm.operator.cross_app_version_checker.is_local_charm(APP_2)
