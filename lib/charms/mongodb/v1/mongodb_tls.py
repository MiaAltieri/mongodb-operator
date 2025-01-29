# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class we manage client database relations.

This class creates user and database for each application relation
and expose needed information for client connection via fields in
external relation.
"""
import base64
import json
import logging
import re
import socket
from typing import Optional, Tuple

from charms.tls_certificates_interface.v3.tls_certificates import (
    CertificateAvailableEvent,
    CertificateExpiringEvent,
    TLSCertificatesRequiresV3,
    generate_csr,
    generate_private_key,
)
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from ops.charm import ActionEvent, RelationBrokenEvent, RelationJoinedEvent
from ops.framework import Object
from ops.model import ActiveStatus, MaintenanceStatus, WaitingStatus

from config import Config

UNIT_SCOPE = Config.Relations.UNIT_SCOPE
Scopes = Config.Relations.Scopes
SANS_DNS_KEY = "sans_dns"
SANS_IPS_KEY = "sans_ips"

# The unique Charmhub library identifier, never change it
LIBID = "e02a50f0795e4dd292f58e93b4f493dd"

# Increment this major API version when introducing breaking changes
LIBAPI = 1

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 6

WAIT_CERT_UPDATE = "wait-cert-updated"

logger = logging.getLogger(__name__)


class MongoDBTLS(Object):
    """In this class we manage client database relations."""

    def __init__(self, charm, peer_relation, substrate):
        """Manager of MongoDB client relations."""
        super().__init__(charm, "client-relations")
        self.charm = charm
        self.substrate = substrate
        self.peer_relation = peer_relation
        self.certs = TLSCertificatesRequiresV3(self.charm, Config.TLS.TLS_PEER_RELATION)
        self.framework.observe(
            self.charm.on.set_tls_private_key_action, self._on_set_tls_private_key
        )
        self.framework.observe(
            self.charm.on[Config.TLS.TLS_PEER_RELATION].relation_joined,
            self._on_tls_relation_joined,
        )
        self.framework.observe(
            self.charm.on[Config.TLS.TLS_PEER_RELATION].relation_broken,
            self._on_tls_relation_broken,
        )
        self.framework.observe(self.certs.on.certificate_available, self._on_certificate_available)
        self.framework.observe(self.certs.on.certificate_expiring, self._on_certificate_expiring)

    def is_tls_enabled(self, internal: bool):
        """Returns a boolean indicating if TLS for a given internal/external is enabled."""
        return self.get_tls_secret(internal, Config.TLS.SECRET_CERT_LABEL) is not None

    def _on_set_tls_private_key(self, event: ActionEvent) -> None:
        """Set the TLS private key, which will be used for requesting the certificate."""
        logger.debug("Request to set TLS private key received.")
        if self.charm.is_role(Config.Role.MONGOS) and not self.charm.has_config_server():
            logger.error(
                "mongos is not running (not integrated to config-server) deferring renewal of certificates."
            )
            event.fail("Mongos cannot set TLS keys until integrated to config-server.")
            return

        if self.charm.upgrade_in_progress:
            logger.warning("Setting TLS key during an upgrade is not supported.")
            event.fail("Setting TLS key during an upgrade is not supported.")
            return

        try:
            self.request_certificate(event.params.get("external-key", None), internal=False)
            self.request_certificate(event.params.get("internal-key", None), internal=True)
            logger.debug("Successfully set TLS private key.")
        except ValueError as e:
            event.fail(str(e))

    def request_certificate(
        self,
        param: Optional[str],
        internal: bool,
    ):
        """Request TLS certificate."""
        if param is None:
            key = generate_private_key()
        else:
            key = self._parse_tls_file(param)

        sans = self.get_new_sans()
        csr = generate_csr(
            private_key=key,
            subject=self._get_subject_name(),
            organization=self._get_subject_name(),
            sans=sans[SANS_DNS_KEY],
            sans_ip=sans[SANS_IPS_KEY],
        )
        self.set_tls_secret(internal, Config.TLS.SECRET_KEY_LABEL, key.decode("utf-8"))
        self.set_tls_secret(internal, Config.TLS.SECRET_CSR_LABEL, csr.decode("utf-8"))
        self.set_tls_secret(internal, Config.TLS.SECRET_CERT_LABEL, None)

        label = "int" if internal else "ext"
        self.charm.unit_peer_data[f"{label}_certs_subject"] = self._get_subject_name()
        self.charm.unit_peer_data[f"{label}_certs_subject"] = self._get_subject_name()
        self.certs.request_certificate_creation(certificate_signing_request=csr)
        self.set_waiting_for_cert_to_update(internal=internal, waiting=True)

    @staticmethod
    def _parse_tls_file(raw_content: str) -> bytes:
        """Parse TLS files from both plain text or base64 format."""
        if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", raw_content):
            return (
                re.sub(
                    r"(-+(BEGIN|END) [A-Z ]+-+)",
                    "\\1",
                    raw_content,
                )
                .rstrip()
                .encode("utf-8")
            )
        return base64.b64decode(raw_content)

    def _on_tls_relation_joined(self, event: RelationJoinedEvent) -> None:
        """Request certificate when TLS relation joined."""
        if self.charm.is_role(Config.Role.MONGOS) and not self.charm.has_config_server():
            logger.info(
                "mongos is not running (not integrated to config-server) deferring renewal of certificates."
            )
            event.defer()
            return

        if self.charm.upgrade_in_progress:
            logger.warning(
                "Enabling TLS is not supported during an upgrade. The charm may be in a broken, unrecoverable state."
            )
            event.defer()
            return

        self.request_certificate(None, internal=True)
        self.request_certificate(None, internal=False)

    def _on_tls_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Disable TLS when TLS relation broken."""
        if not self.charm.db_initialised:
            logger.info("Deferring %s. db is not initialised.", str(type(event)))
            event.defer()
            return

        if self.charm.upgrade_in_progress:
            logger.warning(
                "Disabling TLS is not supported during an upgrade. The charm may be in a broken, unrecoverable state."
            )

        logger.debug("Disabling external and internal TLS for unit: %s", self.charm.unit.name)

        for internal in [True, False]:
            self.set_tls_secret(internal, Config.TLS.SECRET_CA_LABEL, None)
            self.set_tls_secret(internal, Config.TLS.SECRET_CERT_LABEL, None)
            self.set_tls_secret(internal, Config.TLS.SECRET_CHAIN_LABEL, None)

        if self.charm.is_role(Config.Role.CONFIG_SERVER):
            self.charm.cluster.update_ca_secret(new_ca=None)
            self.charm.config_server.update_ca_secret(new_ca=None)

        logger.info("Restarting mongod with TLS disabled.")
        self.charm.status.set_and_share_status(MaintenanceStatus("disabling TLS"))
        self.charm.delete_tls_certificate_from_workload()
        self.charm.restart_charm_services()
        self.charm.status.set_and_share_status(ActiveStatus())

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Enable TLS when TLS certificate available."""
        if self.charm.is_role(Config.Role.MONGOS) and not self.charm.has_config_server():
            logger.debug(
                "mongos requires config-server in order to start, do not restart with TLS until integrated to config-server"
            )
            event.defer()
            return

        # mongos must receive its certificates in order for it to get initialised
        if not self.charm.db_initialised and not self.charm.is_role(Config.Role.MONGOS):
            logger.info("Deferring %s. db is not initialised.", str(type(event)))
            event.defer()
            return

        int_csr = self.get_tls_secret(internal=True, label_name=Config.TLS.SECRET_CSR_LABEL)
        ext_csr = self.get_tls_secret(internal=False, label_name=Config.TLS.SECRET_CSR_LABEL)

        if ext_csr and event.certificate_signing_request.rstrip() == ext_csr.rstrip():
            logger.debug("The external TLS certificate available.")
            internal = False
        elif int_csr and event.certificate_signing_request.rstrip() == int_csr.rstrip():
            logger.debug("The internal TLS certificate available.")
            internal = True
        else:
            logger.error("An unknown certificate is available -- ignoring.")
            return

        self.set_tls_secret(
            internal,
            Config.TLS.SECRET_CHAIN_LABEL,
            "\n".join(event.chain) if event.chain is not None else None,
        )
        self.set_tls_secret(internal, Config.TLS.SECRET_CERT_LABEL, event.certificate)
        self.set_tls_secret(internal, Config.TLS.SECRET_CA_LABEL, event.ca)
        self.set_waiting_for_cert_to_update(internal=internal, waiting=False)

        if self.charm.is_role(Config.Role.CONFIG_SERVER) and internal:
            self.charm.cluster.update_ca_secret(new_ca=event.ca)
            self.charm.config_server.update_ca_secret(new_ca=event.ca)

        if self.is_waiting_for_both_certs():
            logger.debug(
                "Defer till both internal and external TLS certificates available to avoid second restart."
            )
            event.defer()
            return

        self.charm.delete_tls_certificate_from_workload()
        self.charm.push_tls_certificate_to_workload()

        # if mongos hasn't been started let it be started with the integration to the
        # config-server
        if not self.charm.db_initialised and self.charm.is_role(Config.Role.MONGOS):
            logger.info(
                "Mongos has not yet been initialized, will enable TLS when it is set up with the config-server."
            )
            return

        logger.info("Restarting mongod with TLS enabled.")
        self.charm.status.set_and_share_status(MaintenanceStatus("enabling TLS"))
        self.charm.restart_charm_services()

        if not self.charm.is_db_service_ready():
            self.charm.status.set_and_share_status(WaitingStatus("Waiting for MongoDB to start"))
        elif self.charm.unit.status == WaitingStatus(
            "Waiting for MongoDB to start"
        ) or self.charm.unit.status == MaintenanceStatus("enabling TLS"):
            # clear waiting status if db service is ready
            self.charm.status.set_and_share_status(ActiveStatus())

    def is_waiting_for_both_certs(self) -> bool:
        """Returns a boolean indicating whether additional certs are needed."""
        if not self.get_tls_secret(internal=True, label_name=Config.TLS.SECRET_CERT_LABEL):
            logger.debug("Waiting for internal certificate.")
            return True
        if not self.get_tls_secret(internal=False, label_name=Config.TLS.SECRET_CERT_LABEL):
            logger.debug("Waiting for external certificate.")
            return True

        return False

    def _on_certificate_expiring(self, event: CertificateExpiringEvent) -> None:
        """Request the new certificate when old certificate is expiring."""
        if self.charm.is_role(Config.Role.MONGOS) and not self.charm.has_config_server():
            logger.info(
                "mongos is not running (not integrated to config-server) deferring renewal of certificates."
            )
            event.defer()
            return

        if (
            event.certificate.rstrip()
            == self.get_tls_secret(
                internal=False, label_name=Config.TLS.SECRET_CERT_LABEL
            ).rstrip()
        ):
            logger.debug("The external TLS certificate expiring.")
            internal = False
        elif (
            event.certificate.rstrip()
            == self.get_tls_secret(internal=True, label_name=Config.TLS.SECRET_CERT_LABEL).rstrip()
        ):
            logger.debug("The internal TLS certificate expiring.")
            internal = True
        else:
            logger.error("An unknown certificate expiring.")
            return

        logger.debug("Generating a new Certificate Signing Request.")
        self.request_new_certificates(internal)

    def request_new_certificates(self, internal: bool) -> None:
        """Requests the renewel of a new certificate."""
        key = self.get_tls_secret(internal, Config.TLS.SECRET_KEY_LABEL).encode("utf-8")
        old_csr = self.get_tls_secret(internal, Config.TLS.SECRET_CSR_LABEL).encode("utf-8")
        sans = self.get_new_sans()
        new_csr = generate_csr(
            private_key=key,
            subject=self._get_subject_name(),
            organization=self._get_subject_name(),
            sans=sans[SANS_DNS_KEY],
            sans_ip=sans[SANS_IPS_KEY],
        )
        logger.debug("Requesting a certificate renewal.")

        self.certs.request_certificate_renewal(
            old_certificate_signing_request=old_csr,
            new_certificate_signing_request=new_csr,
        )

        self.set_tls_secret(internal, Config.TLS.SECRET_CSR_LABEL, new_csr.decode("utf-8"))
        self.set_waiting_for_cert_to_update(waiting=True, internal=internal)

    def get_new_sans(self) -> dict[str, list[str]]:
        """Create a list of DNS names for a MongoDB unit.

        Returns:
            A list representing the hostnames of the MongoDB unit.
        """
        unit_id = self.charm.unit.name.split("/")[1]

        sans = {
            SANS_DNS_KEY: [
                f"{self.charm.app.name}-{unit_id}",
                socket.getfqdn(),
                "localhost",
                f"{self.charm.app.name}-{unit_id}.{self.charm.app.name}-endpoints",
            ],
            SANS_IPS_KEY: [
                str(self.charm.model.get_binding(self.peer_relation).network.bind_address)
            ],
        }

        if self.charm.is_role(Config.Role.MONGOS) and self.charm.is_external_client:
            sans[SANS_IPS_KEY].append(
                self.charm.get_ext_mongos_host(self.charm.unit, incl_port=False)
            )

        return sans

    def get_current_sans(self, internal: bool) -> dict[str, list[str]] | None:
        """Gets the current SANs for the unit cert."""
        # if unit has no certificates do not proceed.
        if not self.is_tls_enabled(internal=internal):
            return

        pem_file = self.get_tls_secret(internal, Config.TLS.SECRET_CERT_LABEL)

        try:
            cert = x509.load_pem_x509_certificate(pem_file.encode(), default_backend())
            sans = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
            sans_ip = [str(san) for san in sans.get_values_for_type(x509.IPAddress)]
            sans_dns = [str(san) for san in sans.get_values_for_type(x509.DNSName)]
        except x509.ExtensionNotFound:
            sans_ip = []
            sans_dns = []

        return {SANS_IPS_KEY: sorted(sans_ip), SANS_DNS_KEY: sorted(sans_dns)}

    def get_tls_files(self, internal: bool) -> Tuple[Optional[str], Optional[str]]:
        """Prepare TLS files in special MongoDB way.

        MongoDB needs two files:
        — CA file should have a full chain.
        — PEM file should have private key and certificate without certificate chain.
        """
        scope = "internal" if internal else "external"
        if not self.is_tls_enabled(internal):
            logging.debug(f"TLS disabled for {scope}")
            return None, None
        logging.debug(f"TLS *enabled* for {scope}, fetching data for CA and PEM files ")

        ca = self.get_tls_secret(internal, Config.TLS.SECRET_CA_LABEL)
        chain = self.get_tls_secret(internal, Config.TLS.SECRET_CHAIN_LABEL)
        ca_file = chain if chain else ca

        key = self.get_tls_secret(internal, Config.TLS.SECRET_KEY_LABEL)
        cert = self.get_tls_secret(internal, Config.TLS.SECRET_CERT_LABEL)
        pem_file = key
        if cert:
            pem_file = key + "\n" + cert if key else cert

        return ca_file, pem_file

    def set_tls_secret(self, internal: bool, label_name: str, contents: str) -> None:
        """Sets TLS secret, based on whether or not it is related to internal connections."""
        scope = "int" if internal else "ext"
        label_name = f"{scope}-{label_name}"
        self.charm.set_secret(UNIT_SCOPE, label_name, contents)

    def get_tls_secret(self, internal: bool, label_name: str) -> str:
        """Gets TLS secret, based on whether or not it is related to internal connections."""
        scope = "int" if internal else "ext"
        label_name = f"{scope}-{label_name}"
        return self.charm.get_secret(UNIT_SCOPE, label_name)

    def _get_subject_name(self) -> str:
        """Generate the subject name for CSR."""
        # In sharded MongoDB deployments it is a requirement that all subject names match across
        # all cluster components. The config-server name is the source of truth across mongos and
        # shard deployments.
        if not self.charm.is_role(Config.Role.CONFIG_SERVER):
            # until integrated with config-server use current app name as
            # subject name
            return self.charm.get_config_server_name() or self.charm.app.name

        return self.charm.app.name

    def is_set_waiting_for_cert_to_update(
        self,
        internal: bool = False,
    ) -> bool:
        """Returns True if we are waiting for a cert to update."""
        scope = "int" if internal else "ext"
        label_name = f"{scope}-{WAIT_CERT_UPDATE}"

        return json.loads(self.charm.unit_peer_data.get(label_name, "false"))

    def set_waiting_for_cert_to_update(
        self,
        waiting: bool,
        internal: bool,
    ) -> None:
        """Sets a boolean indicator, for whether or not we are waiting for a cert to update."""
        scope = "int" if internal else "ext"
        label_name = f"{scope}-{WAIT_CERT_UPDATE}"
        self.charm.unit_peer_data[label_name] = json.dumps(waiting)
