# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import unittest
from unittest import mock
from unittest.mock import patch

from ops import BlockedStatus
from ops.charm import RelationEvent
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
    @patch("charm.get_charm_revision")
    @patch_network_get(private_address="1.1.1.1")
    def setUp(self, *unused):
        self.harness = Harness(MongodbOperatorCharm)
        self.harness.begin()
        self.harness.add_relation("database-peers", "mongodb-peers")
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    @parameterized.expand([["config-server"], ["shard"]])
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charm.MongoDBProvider.oversee_users")
    def test_relation_event_relation_not_feasible(self, role: str, oversee_users, *unused):
        """Tests that relating with a wrong role sets a blocked status."""

        def is_config_server_role(role_name: str):
            return role_name == role

        self.harness.charm.is_role = is_config_server_role

        relation_id = self.harness.add_relation("database", "consumer")
        self.harness.add_relation_unit(relation_id, "consumer/0")
        self.harness.update_relation_data(relation_id, "consumer/0", PEER_ADDR)

        assert self.harness.charm.unit.status == BlockedStatus(
            "Sharding roles do not support database interface."
        )
        oversee_users.assert_not_called()

    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider.oversee_users")
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
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider.oversee_users")
    @patch("charm.MongodbOperatorCharm.auth_enabled", return_value=True)
    def test_relation_event_oversee_users_mongo_failure(
        self,
        auth_enabled,
        oversee_users,
        defer,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Tests the errors related to pymongo when overseeing users result in a defer."""
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
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
    @patch("data_platform_helpers.version_check.CrossAppVersionChecker.is_local_charm")
    @patch(
        "data_platform_helpers.version_check.CrossAppVersionChecker.is_integrated_to_locally_built_charm"
    )
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("ops.framework.EventBase.defer")
    @patch("charm.MongoDBProvider.oversee_users")
    @patch("charm.MongodbOperatorCharm.auth_enabled", return_value=True)
    def test_relation_event_oversee_users_fails_to_get_relation(
        self,
        auth_enabled,
        oversee_users,
        defer,
        get_rev,
        is_local,
        is_integrated_to_local,
    ):
        """Verifies that when users are formatted incorrectly an assertion error is raised."""
        # presets
        self.harness.set_leader(True)
        self.harness.charm.app_peer_data["db_initialised"] = "true"
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
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_get_users_failure(self, connection):
        """Verifies that when unable to retrieve users from mongod an exception is raised."""
        for dep_id in DEPARTED_IDS:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                connection.return_value.__enter__.return_value.get_users.side_effect = exception
                with self.assertRaises(expected_raise):
                    self.harness.charm.client_relations.oversee_users(
                        dep_id, RelationEvent(mock.Mock(), mock.Mock())
                    )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_drop_user_failure(self, connection, relation_users):
        """Verifies that when unable to drop users from mongod an exception is raised."""
        # presets, such that there is a need to drop users.
        self.harness.charm.app_peer_data["managed-users-key"] = json.dumps(
            ["relation-user1", "relation-user2"]
        )
        relation_users.return_value = {"relation-user1"}
        connection.return_value.__enter__.return_value.get_users.return_value = {
            "relation-user1",
            "relation-user2",
        }

        for dep_id in DEPARTED_IDS:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                connection.return_value.__enter__.return_value.drop_user.side_effect = exception
                with self.assertRaises(expected_raise):
                    self.harness.charm.client_relations.oversee_users(
                        dep_id, RelationEvent(mock.Mock(), mock.Mock())
                    )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_get_config_failure(self, connection, relation_users):
        """Verifies that when users do not match necessary schema an AssertionError is raised."""
        # presets, such that the need to create user relations is triggered. Further presets
        # designed such that relation users will not match due to not following schema
        # "relation-username"
        relation_users.return_value = {"user1", "user2"}
        connection.return_value.__enter__.return_value.get_users.return_value = {"user1"}

        for dep_id in DEPARTED_IDS:
            with self.assertRaises(AssertionError):
                self.harness.charm.client_relations.oversee_users(
                    dep_id, RelationEvent(mock.Mock(), mock.Mock())
                )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._set_relation")
    @patch("charm.MongoDBProvider._get_config")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    @patch("charm.MongoDBProvider._diff")
    def test_oversee_users_no_config_database(
        self, diff, connection, relation_users, get_config, set_relation
    ):
        """Verifies when the config for a user has no database that they are not created."""
        # presets, such that the need to create user relations is triggered
        relation_users.return_value = {"relation-user1", "relation-user2"}
        connection.return_value.__enter__.return_value.get_users.return_value = {"relation-user1"}

        get_config.return_value.database = None

        for dep_id in DEPARTED_IDS:
            self.harness.charm.client_relations.oversee_users(
                dep_id, RelationEvent(mock.Mock(), mock.Mock())
            )
            connection.return_value.__enter__.return_value.create_user.assert_not_called()
            set_relation.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._set_relation")
    @patch("charm.MongoDBProvider._get_config")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_create_user_failure(
        self, connection, relation_users, get_config, set_relation
    ):
        """Verifies when user creation fails an exception is raised and no relations are set."""
        # presets, such that the need to create user relations is triggered
        relation_users.return_value = {"relation-user1", "relation-user2"}
        connection.return_value.__enter__.return_value.get_users.return_value = {"relation-user1"}

        for dep_id in DEPARTED_IDS:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                connection.return_value.__enter__.return_value.create_user.side_effect = exception
                with self.assertRaises(expected_raise):
                    self.harness.charm.client_relations.oversee_users(
                        dep_id, RelationEvent(mock.Mock(), mock.Mock())
                    )
                set_relation.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_config")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_set_relation_failure(self, connection, relation_users, get_config):
        """Verifies that when adding a user with an invalid name that an exception is raised."""
        # presets, such that the need to create user relations is triggered and user naming such
        # that setting relation users will fail since they do not follow the schema
        # "relation-username"
        relation_users.return_value = {"user1", "user2"}
        connection.return_value.__enter__.return_value.get_users.return_value = {"user1"}
        get_config.return_value.username = "user1"

        for dep_id in DEPARTED_IDS:
            # getting usernames raises AssertionError when usernames do not follow correct format
            with self.assertRaises(AssertionError):
                self.harness.charm.client_relations.oversee_users(
                    dep_id, RelationEvent(mock.Mock(), mock.Mock())
                )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_update_get_config_failure(self, connection, relation_users):
        """Verifies that when updating a user with an invalid name that an exception is raised."""
        # presets, such that the need to update user relations is triggered and user naming such
        # that setting relation users will fail since they do not follow the schema
        # "relation-username"
        relation_users.return_value = {"user1"}
        connection.return_value.__enter__.return_value.get_users.return_value = {"user1"}

        for dep_id in DEPARTED_IDS:
            with self.assertRaises(AssertionError):
                self.harness.charm.client_relations.oversee_users(
                    dep_id, RelationEvent(mock.Mock(), mock.Mock())
                )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_config")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_update_user_failure(self, connection, relation_users, get_config):
        """Verifies that when updating users fails an exception is raised."""
        # presets, such that the need to update user relations is triggered
        self.harness.charm.app_peer_data["managed-users-key"] = json.dumps(["relation-user1"])
        relation_users.return_value = {"relation-user1"}
        connection.return_value.__enter__.return_value.get_users.return_value = {"relation-user1"}

        for dep_id in DEPARTED_IDS:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                connection.return_value.__enter__.return_value.update_user.side_effect = exception

                with self.assertRaises(expected_raise):
                    self.harness.charm.client_relations.oversee_users(
                        dep_id, RelationEvent(mock.Mock(), mock.Mock())
                    )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_databases_from_relations")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_no_auto_delete(
        self, connection, relation_users, databases_from_relations
    ):
        """Verifies when no-auto delete is specified databases are not dropped.."""
        # presets, such that the need to drop a database
        connection.return_value.__enter__.return_value.get_databases.return_value = {"db1", "db2"}
        databases_from_relations.return_value = {"d1"}

        for dep_id in DEPARTED_IDS:
            self.harness.charm.client_relations.oversee_users(
                dep_id, RelationEvent(mock.Mock(), mock.Mock())
            )
            connection.return_value.__enter__.return_value.drop_database.assert_not_called()

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_mongo_databases_failure(self, connection, relation_users):
        """Verifies failures in checking for databases with mongod result in raised exceptions."""
        self.harness.update_config({"auto-delete": True})
        for dep_id in DEPARTED_IDS:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                connection.return_value.__enter__.return_value.get_databases.side_effect = (
                    exception
                )

                with self.assertRaises(expected_raise):
                    self.harness.charm.client_relations.oversee_users(
                        dep_id, RelationEvent(mock.Mock(), mock.Mock())
                    )

    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.MongoDBProvider._get_databases_from_relations")
    @patch("charm.MongoDBProvider._get_users_from_relations")
    @patch("charms.mongodb.v1.mongodb_provider.MongoConnection")
    def test_oversee_users_drop_database_failure(
        self, connection, relation_users, databases_from_relations
    ):
        """Verifies failures in dropping database result in raised exception."""
        # presets, such that the need to drop a database
        connection.return_value.__enter__.return_value.get_databases.return_value = {"db1", "db2"}
        databases_from_relations.return_value = {"d1"}
        self.harness.update_config({"auto-delete": True})

        # verify operations across different inputs to oversee_users
        for dep_id in DEPARTED_IDS:
            for exception, expected_raise in PYMONGO_EXCEPTIONS:
                connection.return_value.__enter__.return_value.drop_database.side_effect = (
                    exception
                )

                with self.assertRaises(expected_raise):
                    # verify behaviour across relation event
                    self.harness.charm.client_relations.oversee_users(
                        dep_id, RelationEvent(mock.Mock(), mock.Mock())
                    )

    @parameterized.expand(
        [
            ["shard", "true", True],
            ["database", "false", True],
            ["database", "true", False],
        ]
    )
    @patch_network_get(private_address="1.1.1.1")
    @patch("charm.CrossAppVersionChecker.is_local_charm")
    @patch("charm.CrossAppVersionChecker.is_integrated_to_locally_built_charm")
    @patch("charms.mongodb.v0.set_status.get_charm_revision")
    @patch("charms.mongodb.v1.mongodb_provider.MongoDBProvider._get_relations")
    def test_update_app_relation_data_protected(
        self,
        role: str,
        db_init: str,
        is_leader: bool,
        _get_relations_mock,
        charm_rev,
        is_integrater,
        is_local,
    ):
        def mock_role_call(*args):
            return args == (role,)

        self.harness.charm.is_role = mock_role_call
        self.harness.charm.app_peer_data["db_initialised"] = db_init
        self.harness.set_leader(is_leader)

        self.harness.add_relation("database", "consumer")

        # Should fail because the role is "shard"
        assert not self.charm.client_relations.pass_sanity_hook_checks()

        self.harness.charm.client_relations.update_app_relation_data()
        _get_relations_mock.assert_not_called()
