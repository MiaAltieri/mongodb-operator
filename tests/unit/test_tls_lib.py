# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest.mock import PropertyMock, patch

from cryptography import x509
from ops.testing import Harness
from parameterized import parameterized
from single_kernel_mongo.config.literals import Scope

from charm import MongoDBVMCharm

RELATION_NAME = "certificates"


class TestMongoTLS(unittest.TestCase):
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def setUp(self, *unused):
        self.harness = Harness(MongoDBVMCharm)
        self.harness.begin()
        self.harness.add_relation("database-peers", "database-peers")
        self.harness.charm.operator.state.db_initialised = True
        self.harness.set_leader(True)
        self.charm = self.harness.charm
        self.addCleanup(self.harness.cleanup)

    @parameterized.expand([True, False])
    @patch("single_kernel_mongo.managers.tls.TLSManager.get_new_sans")
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def test_set_tls_private_keys(self, leader, get_new_sans, *unused):
        """Tests setting of TLS private key via the leader, ie both internal and external.

        Note: this implicitly tests: _request_certificate & _parse_tls_file
        """
        self.harness.set_leader(True)
        self.harness.add_relation("certificates", "certificates")
        # Tests for leader unit (ie internal certificates and external certificates)
        get_new_sans.return_value = {"sans_dns": [""], "sans_ips": ["1.1.1.1"]}
        self.harness.set_leader(leader)

        # generated rsa key test - leader
        self.harness.run_action("set-tls-private-key")
        self.verify_internal_rsa_csr()
        self.verify_external_rsa_csr()

        with open("tests/unit/data/key.pem") as f:
            key_contents = f.readlines()
            key_contents = "".join(key_contents)

        set_app_rsa_key = key_contents
        # we expect the app rsa key to be parsed such that its trailing newline is removed.
        parsed_app_rsa_key = set_app_rsa_key[:-1]
        params = {"internal-key": set_app_rsa_key}
        self.harness.run_action("set-tls-private-key", params)
        self.verify_internal_rsa_csr(specific_rsa=True, expected_rsa=parsed_app_rsa_key)
        self.verify_external_rsa_csr()

    @parameterized.expand([True, False])
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def test_tls_relation_joined(self, leader, *unused):
        """Test that leader units set both external and internal certificates."""
        self.harness.set_leader(leader)
        self.relate_to_tls_certificates_operator()
        self.verify_internal_rsa_csr()
        self.verify_external_rsa_csr()

    @parameterized.expand([True, False])
    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.restart_charm_services")
    def test_tls_relation_broken(self, leader, restart_charm_services, *unused):
        """Test removes both external and internal certificates."""
        self.harness.charm.operator.state.db_initialised = True
        self.harness.set_leader(leader)
        # set initial certificate values
        rel_id = self.relate_to_tls_certificates_operator()

        self.harness.remove_relation(rel_id)

        # internal certificates and external certificates should be removed
        for scope in [Scope.UNIT, Scope.APP]:
            ca_secret = self.harness.charm.operator.state.secrets.get_for_key(scope, "ca-secret")
            cert_secret = self.harness.charm.operator.state.secrets.get_for_key(
                scope, "cert-secret"
            )
            chain_secret = self.harness.charm.operator.state.secrets.get_for_key(
                scope, "chain-secret"
            )
            self.assertIsNone(ca_secret)
            self.assertIsNone(cert_secret)
            self.assertIsNone(chain_secret)

        # units should be restarted after updating TLS settings
        restart_charm_services.assert_called()

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def test_external_certificate_expiring(self, *unused):
        """Verifies that when an external certificate expires a csr is made."""
        # assume relation exists with a current certificate
        self.relate_to_tls_certificates_operator()
        self.harness.charm.operator.state.secrets.set("int-cert-secret", "int-cert", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set("ext-cert-secret", "ext-cert", Scope.UNIT)

        # simulate current certificate expiring
        old_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-csr-secret"
        )

        self.charm.operator.tls_events.certs_client.on.certificate_expiring.emit(
            certificate="ext-cert", expiry=None
        )

        # verify a new csr was generated

        new_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-csr-secret"
        )
        self.assertNotEqual(old_csr, new_csr)

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def test_internal_certificate_expiring(self, *unused):
        """Verifies that when an internal certificate expires a csr is made."""
        # assume relation exists with a current certificate
        self.relate_to_tls_certificates_operator()
        self.harness.charm.operator.state.secrets.set("int-cert-secret", "int-cert", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set("ext-cert-secret", "ext-cert", Scope.UNIT)

        # verify a new csr was generated when unit receives expiry
        old_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-csr-secret"
        )
        self.charm.operator.tls_events.certs_client.on.certificate_expiring.emit(
            certificate="int-cert", expiry=None
        )
        new_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-csr-secret"
        )
        self.assertNotEqual(old_csr, new_csr)

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    def test_unknown_certificate_expiring(self, *unused):
        """Verifies that when an unknown certificate expires nothing happens."""
        # assume relation exists with a current certificate
        self.relate_to_tls_certificates_operator()
        self.harness.charm.operator.state.secrets.set("int-cert-secret", "int-cert", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set("ext-cert-secret", "ext-cert", Scope.UNIT)

        # simulate unknown certificate expiring on leader
        old_app_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-csr-secret"
        )
        old_unit_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-csr-secret"
        )

        self.charm.operator.tls_events.certs_client.on.certificate_expiring.emit(
            certificate="unknown-cert", expiry=""
        )

        new_app_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-csr-secret"
        )
        new_unit_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-csr-secret"
        )

        self.assertEqual(old_app_csr, new_app_csr)
        self.assertEqual(old_unit_csr, new_unit_csr)

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch("single_kernel_mongo.managers.tls.TLSManager.push_tls_files_to_workload")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.restart_charm_services")
    def test_external_certificate_available(self, restart_charm_services, *unused):
        """Tests behavior when external certificate is made available."""
        # assume relation exists with a current certificate
        self.harness.charm.operator.state.db_initialised = True
        self.harness.set_leader(True)
        self.relate_to_tls_certificates_operator()
        self.harness.charm.operator.state.secrets.set("ext-csr-secret", "csr-secret", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set(
            "ext-cert-secret", "unit-cert-old", Scope.UNIT
        )
        self.harness.charm.operator.state.secrets.set("int-cert-secret", "app-cert", Scope.UNIT)

        self.charm.operator.tls_events.certs_client.on.certificate_available.emit(
            certificate_signing_request="csr-secret",
            chain=["unit-chain"],
            certificate="unit-cert",
            ca="unit-ca",
        )

        chain_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-chain-secret"
        )
        unit_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-cert-secret"
        )
        ca_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-ca-secret"
        )

        self.assertEqual(chain_secret, "unit-chain")
        self.assertEqual(unit_secret, "unit-cert")
        self.assertEqual(ca_secret, "unit-ca")

        restart_charm_services.assert_called()

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch("single_kernel_mongo.managers.tls.TLSManager.push_tls_files_to_workload")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.restart_charm_services")
    def test_internal_certificate_available(self, restart_charm_services, *unused):
        """Tests behavior when internal certificate is made available."""
        self.harness.charm.operator.state.db_initialised = True
        self.harness.set_leader(True)
        # assume relation exists with a current certificate
        self.relate_to_tls_certificates_operator()
        self.harness.charm.operator.state.secrets.set("int-csr-secret", "int-csr", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set(
            "int-cert-secret", "int-cert-old", Scope.UNIT
        )
        self.harness.charm.operator.state.secrets.set(
            "ext-cert-secret", "ext-cert-secret", Scope.UNIT
        )

        self.charm.operator.tls_events.certs_client.on.certificate_available.emit(
            certificate_signing_request="int-csr",
            chain=["int-chain"],
            certificate="int-cert",
            ca="int-ca",
        )

        chain_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-chain-secret"
        )
        unit_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-cert-secret"
        )
        ca_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-ca-secret"
        )

        self.assertEqual(chain_secret, "int-chain")
        self.assertEqual(unit_secret, "int-cert")
        self.assertEqual(ca_secret, "int-ca")

        restart_charm_services.assert_called()

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch("single_kernel_mongo.managers.tls.TLSManager.push_tls_files_to_workload")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.restart_charm_services")
    def test_unknown_certificate_available(self, restart_charm_services, *unused):
        """Tests that when an unknown certificate is available, nothing is updated."""
        # assume relation exists with a current certificate
        self.relate_to_tls_certificates_operator()
        self.harness.charm.operator.state.secrets.set(
            "int-chain-secret", "app-chain-old", Scope.UNIT
        )
        self.harness.charm.operator.state.secrets.set(
            "int-cert-secret", "app-cert-old", Scope.UNIT
        )
        self.harness.charm.operator.state.secrets.set("int-csr-secret", "app-csr-old", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set("int-ca-secret", "app-ca-old", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set("ext-cert-secret", "unit-cert", Scope.UNIT)

        self.charm.operator.tls_events.certs_client.on.certificate_available.emit(
            certificate_signing_request="app-csr",
            chain=["app-chain"],
            certificate="app-cert",
            ca="app-ca",
        )

        chain_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-chain-secret"
        )
        unit_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-cert-secret"
        )
        ca_secret = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-ca-secret"
        )

        self.assertEqual(chain_secret, "app-chain-old")
        self.assertEqual(unit_secret, "app-cert-old")
        self.assertEqual(ca_secret, "app-ca-old")

        restart_charm_services.assert_not_called()

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch("single_kernel_mongo.managers.tls.TLSManager.push_tls_files_to_workload")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.restart_charm_services")
    @patch("ops.framework.EventBase.defer")
    def test_external_certificate_available_deferred(self, defer, *unused):
        """Tests behavior when external certificate is made available."""
        self.harness.charm.operator.state.db_initialised = False

        # assume relation exists with a current certificate
        self.relate_to_tls_certificates_operator()
        self.harness.charm.operator.state.secrets.set("ext-csr-secret", "csr-secret", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set(
            "ext-cert-secret", "unit-cert-old", Scope.UNIT
        )
        self.harness.charm.operator.state.secrets.set("int-cert-secret", "app-cert", Scope.UNIT)

        self.charm.operator.tls_events.certs_client.on.certificate_available.emit(
            certificate_signing_request="csr-secret",
            chain=["unit-chain"],
            certificate="unit-cert",
            ca="unit-ca",
        )
        defer.assert_called()

    @patch(
        "single_kernel_mongo.managers.mongodb_operator.get_charm_revision",
        return_value="1",
    )
    @patch("single_kernel_mongo.managers.tls.TLSManager.push_tls_files_to_workload")
    @patch("single_kernel_mongo.managers.mongodb_operator.MongoDBOperator.restart_charm_services")
    @patch("ops.framework.EventBase.defer")
    def test_external_certificate_broken_deferred(self, defer, *unused):
        """Tests behavior when external certificate is made available."""
        self.harness.charm.operator.state.db_initialised = False

        # assume relation exists with a current certificate
        rel_id = self.relate_to_tls_certificates_operator()
        self.harness.remove_relation(rel_id)

        defer.assert_called()

    def test_get_new_sans_gives_node_port_for_mongos_k8s(self):
        """Tests that get_new_sans only gets node port for external mongos K8s."""
        self.harness.set_leader(True)
        with patch(
            "single_kernel_mongo.state.charm_state.CharmState.unit_host",
            new_callable=PropertyMock(),
        ) as prop_mock:
            prop_mock.return_value = "node_port"
            for substrate in ["k8s", "vm"]:
                for role in ["mongos", "config-server", "shard"]:
                    if role == "mongos" and substrate == "k8s":
                        continue

                    assert (
                        "node-port"
                        not in self.harness.charm.operator.tls_manager.get_new_sans()["sans_ips"]
                    )

    @patch("single_kernel_mongo.state.tls_state.TLSState.is_tls_enabled")
    @patch("single_kernel_mongo.managers.tls.x509.load_pem_x509_certificate")
    def test_get_current_sans_returns_none(self, cert, is_tls_enabled):
        """Tests the different scenarios that get_current_sans returns None.

        1. get_current_sans returns None when TLS is not enabled.
        2. get_current_sans returns None if cert file is wrongly formatted.
        """
        # case 1: get_current_sans returns None when TLS is not enabled.
        is_tls_enabled.return_value = None
        for internal in [True, False]:
            self.assertEqual(
                self.harness.charm.operator.tls_manager.get_current_sans(internal),
                None,
            )

        # case 2: error getting extension
        is_tls_enabled.return_value = True
        cert.side_effect = x509.ExtensionNotFound(msg="error-message", oid=1)
        self.harness.charm.operator.state.secrets.set("ext-cert-secret", "unit-cert", Scope.UNIT)
        self.harness.charm.operator.state.secrets.set("int-cert-secret", "app-cert", Scope.UNIT)

        for internal in [True, False]:
            self.assertEqual(
                self.harness.charm.operator.tls_manager.get_current_sans(internal),
                {"sans_ips": [], "sans_dns": []},
            )

    # Helper functions
    def relate_to_tls_certificates_operator(self) -> int:
        """Relates the charm to the TLS certificates operator."""
        rel_id = self.harness.add_relation(RELATION_NAME, "tls-certificates-operator")
        self.harness.add_relation_unit(rel_id, "tls-certificates-operator/0")
        return rel_id

    def verify_external_rsa_csr(
        self,
        specific_rsa=False,
        expected_rsa=None,
        specific_csr=False,
        expected_csr=None,
    ):
        """Verifies values of external rsa and csr.

        Checks if rsa/csr were randomly generated or if they are a provided value.
        """
        unit_rsa_key = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-key-secret"
        )
        unit_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "ext-csr-secret"
        )

        if specific_rsa:
            self.assertEqual(unit_rsa_key, expected_rsa)
        else:
            self.assertEqual(unit_rsa_key.split("\n")[0], "-----BEGIN RSA PRIVATE KEY-----")

        if specific_csr:
            self.assertEqual(unit_csr, expected_csr)
        else:
            self.assertEqual(unit_csr.split("\n")[0], "-----BEGIN CERTIFICATE REQUEST-----")

    def verify_internal_rsa_csr(
        self,
        specific_rsa=False,
        expected_rsa=None,
        specific_csr=False,
        expected_csr=None,
    ):
        """Verifies values of internal rsa and csr.

        Checks if rsa/csr were randomly generated or if they are a provided value.
        """
        int_rsa_key = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-key-secret"
        )
        int_csr = self.harness.charm.operator.state.secrets.get_for_key(
            Scope.UNIT, "int-csr-secret"
        )

        if specific_rsa:
            self.assertEqual(int_rsa_key, expected_rsa)
        else:
            self.assertEqual(int_rsa_key.split("\n")[0], "-----BEGIN RSA PRIVATE KEY-----")

        if specific_csr:
            self.assertEqual(int_csr, expected_csr)
        else:
            self.assertEqual(int_csr.split("\n")[0], "-----BEGIN CERTIFICATE REQUEST-----")
