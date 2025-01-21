# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import unittest
from unittest.mock import patch

from ops import BlockedStatus
from ops.testing import Harness
from parameterized import parameterized
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure

from charm import MongodbOperatorCharm

from .helpers import patch_network_get

PYMONGO_EXCEPTIONS = [
    (ConnectionFailure("error message"), ConnectionFailure),
    (ConfigurationError("error message"), ConfigurationError),
    (OperationFailure("error message"), OperationFailure),
]
PEER_ADDR = {"private-address": "127.4.5.6"}
RELATION_EVENTS = ["joined", "changed", "departed"]
DEPARTED_IDS = [None, 0]


class TestMongoProvider(unittest.TestCase):
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self, *unused):
        self.harness = Harness(MongodbOperatorCharm)
        self.harness.begin()
        self.harness.add_relation("database-peers", "mongodb-peers")
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    @parameterized.expand([["config-server"], ["shard"]])
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.reconcile_mongo_users_and_dbs")
    def test_relation_event_relation_not_feasible(self, role: str, oversee_users, *unused):
        """Tests that relating with a wrong role sets a blocked status."""

        def is_config_server_role(role_name: str):
            return role_name == role

        self.harness.charm.operator.state.is_role = is_config_server_role

        relation_id = self.harness.add_relation("database", "consumer")
        self.harness.add_relation_unit(relation_id, "consumer/0")
        self.harness.update_relation_data(relation_id, "consumer/0", PEER_ADDR)

        assert self.harness.charm.unit.status == BlockedStatus(
            "Sharding roles do not support database interface."
        )
        oversee_users.assert_not_called()

    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.reconcile_mongo_users_and_dbs")
    def test_relation_event_db_not_initialised(self, oversee_users, defer, *unused):
        """Tests no database relations are handled until the database is initialised.

        Users should not be "overseen" until the database has been initialised, no matter the
        event hook (departed, joined, updated)
        """
        # presets
        self.harness.set_leader(True)
        relation_id = self.harness.add_relation("database", "consumer")

        for relation_event in RELATION_EVENTS:
            if relation_event == "joined":
                self.harness.add_relation_unit(relation_id, "consumer/0")
            elif relation_event == "changed":
                self.harness.update_relation_data(relation_id, "consumer/0", PEER_ADDR)
            else:
                self.harness.remove_relation_unit(relation_id, "consumer/0")

        oversee_users.assert_not_called()
        defer.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.reconcile_mongo_users_and_dbs")
    def test_relation_event_oversee_users_mongo_failure(
        self,
        oversee_users,
        defer,
        get_rev,
    ):
        """Tests the errors related to pymongo when overseeing users result in a defer."""
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        relation_id = self.harness.add_relation("database", "consumer")

        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            oversee_users.side_effect = exception

            for relation_event in RELATION_EVENTS:
                if relation_event == "joined":
                    self.harness.add_relation_unit(relation_id, "consumer/0")
                elif relation_event == "changed":
                    self.harness.update_relation_data(relation_id, "consumer/0", PEER_ADDR)
                else:
                    self.harness.remove_relation_unit(relation_id, "consumer/0")

            defer.assert_called()

    # oversee_users raises AssertionError when unable to attain users from relation
    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    @patch("ops.framework.EventBase.defer")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.reconcile_mongo_users_and_dbs")
    def test_relation_event_oversee_users_fails_to_get_relation(
        self,
        oversee_users,
        defer,
        get_rev,
    ):
        """Verifies that when users are formatted incorrectly an assertion error is raised."""
        # presets
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        relation_id = self.harness.add_relation("database", "consumer")

        # AssertionError is raised when unable to attain users from relation (due to name
        # formatting)
        oversee_users.side_effect = AssertionError
        with self.assertRaises(AssertionError):
            for relation_event in RELATION_EVENTS:
                if relation_event == "joined":
                    self.harness.add_relation_unit(relation_id, "consumer/0")
                elif relation_event == "changed":
                    self.harness.update_relation_data(relation_id, "consumer/0", PEER_ADDR)
                else:
                    self.harness.remove_relation_unit(relation_id, "consumer/0")

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.user_exists")
    def test_oversee_users_get_users_failure(self, mock_user_exists):
        """Verifies that when unable to retrieve users from mongod an exception is raised."""
        relation_id = self.harness.add_relation("database", "consumer")
        self.harness.add_relation_unit(relation_id=relation_id, remote_unit_name="consumer/0")
        self.harness.update_relation_data(
            relation_id, "consumer", PEER_ADDR | {"database": "test"}
        )
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="database"
        )
        for dep_id in [True, False]:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                mock_user_exists.side_effect = exception
                with self.assertRaises(expected_raise):
                    self.harness.charm.operator.mongo_manager.reconcile_mongo_users_and_dbs(
                        relation=relation,
                        relation_departing=dep_id,
                        relation_changed=True,
                    )

    @patch_network_get(private_address="1.1.1.1")
    @patch(
        "single_kernel_mongo.utils.mongo_connection.MongoConnection.user_exists",
        return_value=False,
    )
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.create_user")
    @patch(
        "single_kernel_mongo.lib.charms.data_platform_libs.v0.data_interfaces.DatabaseProviderData.set_credentials"
    )
    def test_oversee_users_create_user_failure(self, set_credentials, create_user, user_exists):
        """Verifies when user creation fails an exception is raised and no relations are set."""
        # presets, such that the need to create user relations is triggered
        relation_id = self.harness.add_relation("database", "consumer")
        self.harness.add_relation_unit(relation_id=relation_id, remote_unit_name="consumer/0")
        self.harness.update_relation_data(
            relation_id, "consumer", PEER_ADDR | {"database": "test"}
        )
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="database"
        )
        for dep_id in [True, False]:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                create_user.side_effect = exception
                with self.assertRaises(expected_raise):
                    self.harness.charm.operator.mongo_manager.reconcile_mongo_users_and_dbs(
                        relation=relation,
                        relation_departing=dep_id,
                        relation_changed=True,
                    )
                set_credentials.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.add_user")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.update_user")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.remove_user")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.drop_database")
    def test_oversee_users_no_auto_delete(self, drop_db, *unused):
        """Verifies when no-auto delete is specified databases are not dropped.."""
        # presets, such that the need to drop a database
        relation_id = self.harness.add_relation("database", "consumer")
        self.harness.add_relation_unit(relation_id=relation_id, remote_unit_name="consumer/0")
        self.harness.update_relation_data(
            relation_id, "consumer", PEER_ADDR | {"database": "test"}
        )
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="database"
        )

        self.harness.charm.operator.mongo_manager.reconcile_mongo_users_and_dbs(
            relation, relation_departing=True
        )
        drop_db.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.add_user")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.update_user")
    @patch("single_kernel_mongo.managers.mongo.MongoManager.remove_user")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.get_databases")
    @patch("single_kernel_mongo.utils.mongo_connection.MongoConnection.drop_database")
    def test_oversee_users_mongo_databases_failure(self, drop_db, get_db, *unused):
        """Verifies failures in checking for databases with mongod result in raised exceptions."""
        self.harness.set_leader(True)
        self.harness.charm.operator.state.db_initialised = True
        self.harness.update_config({"auto-delete": True})

        relation_id = self.harness.add_relation("database", "consumer")
        self.harness.add_relation_unit(relation_id=relation_id, remote_unit_name="consumer/0")
        self.harness.update_relation_data(
            relation_id, "consumer", PEER_ADDR | {"database": "test"}
        )
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="database"
        )

        get_db.return_value = {"test"}

        for exception, expected_raise in PYMONGO_EXCEPTIONS:
            drop_db.side_effect = exception
            with self.assertRaises(expected_raise):
                self.harness.charm.operator.mongo_manager.reconcile_mongo_users_and_dbs(
                    relation, relation_departing=True
                )

    @parameterized.expand(
        [
            ["config-server", True, True],
            ["shard", True, True],
            ["database", False, True],
            ["database", True, False],
        ]
    )
    @patch_network_get(private_address="1.1.1.1")
    @patch(
        "single_kernel_mongo.lib.charms.data_platform_libs.v0.data_interfaces.DatabaseProviderData.set_credentials"
    )
    @patch("single_kernel_mongo.managers.mongodb_operator.get_charm_revision")
    def test_update_app_relation_data_protected(
        self, role: str, db_init: str, is_leader: bool, charm_rev, set_creds
    ):
        def mock_role_call(*args):
            return args == (role,)

        self.harness.set_leader(is_leader)
        self.harness.charm.operator.state.db_initialised = db_init
        self.harness.update_config({"auto-delete": True})

        self.harness.charm.operator.state.is_role = mock_role_call

        relation_id = self.harness.add_relation("database", "consumer")
        relation = self.harness.model.get_relation(
            relation_id=relation_id, relation_name="database"
        )

        self.harness.charm.operator.mongo_manager.update_app_relation_data(relation)
        set_creds.assert_not_called()
