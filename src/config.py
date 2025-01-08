"""Configuration for MongoDB Charm."""

# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from typing import Literal, TypeAlias

from ops.model import BlockedStatus

Package: TypeAlias = tuple[str, str, str]


class Config:
    """Configuration for MongoDB Charm."""

    MONGOS_PORT = 27018
    MONGODB_PORT = 27017
    SUBSTRATE = "vm"
    ENV_VAR_PATH = "/etc/environment"
    MONGODB_SNAP_DATA_DIR = "/var/snap/charmed-mongodb/current"
    MONGOD_CONF_DIR = f"{MONGODB_SNAP_DATA_DIR}/etc/mongod"
    MONGOD_CONF_FILE_PATH = f"{MONGOD_CONF_DIR}/mongod.conf"
    CHARM_INTERNAL_VERSION_FILE = "charm_version"
    SNAP_PACKAGES = [("charmed-mongodb", "6/edge", 123)]

    MONGODB_COMMON_PATH = Path("/var/snap/charmed-mongodb/common")

    # This is the snap_daemon user, which does not exist on the VM before the
    # snap install so creating it by UID
    SNAP_USER = 584788

    # Keep these alphabetically sorted
    class Actions:
        """Actions related config for MongoDB Charm."""

        PASSWORD_PARAM_NAME = "password"
        USERNAME_PARAM_NAME = "username"

    class AuditLog:
        """Audit log related configuration."""

        FORMAT = "JSON"
        DESTINATION = "file"
        FILE_NAME = "audit.log"

    class Backup:
        """Backup related config for MongoDB Charm."""

        SERVICE_NAME = "pbm-agent"
        URI_PARAM_NAME = "pbm-uri"

    class LogRotate:
        """Log rotate related constants."""

        MAX_LOG_SIZE = "50M"
        MAX_ROTATIONS_TO_KEEP = 10

    class Monitoring:
        """Monitoring related config for MongoDB Charm."""

        MONGODB_EXPORTER_PORT = 9216
        METRICS_ENDPOINTS = [
            {"path": "/metrics", "port": f"{MONGODB_EXPORTER_PORT}"},
        ]
        METRICS_RULES_DIR = "./src/alert_rules/prometheus"
        LOGS_RULES_DIR = "./src/alert_rules/loki"
        LOG_SLOTS = ["charmed-mongodb:logs"]
        URI_PARAM_NAME = "monitor-uri"
        SERVICE_NAME = "mongodb-exporter"

    class TLS:
        """TLS related config for MongoDB Charm."""

        KEY_FILE_NAME = "keyFile"
        TLS_PEER_RELATION = "certificates"
        SECRET_KEY_LABEL = "key-secret"

        EXT_PEM_FILE = "external-cert.pem"
        EXT_CA_FILE = "external-ca.crt"
        INT_PEM_FILE = "internal-cert.pem"
        INT_CA_FILE = "internal-ca.crt"
        SECRET_CA_LABEL = "ca-secret"
        SECRET_CERT_LABEL = "cert-secret"
        SECRET_CSR_LABEL = "csr-secret"
        SECRET_CHAIN_LABEL = "chain-secret"

    class Relations:
        """Relations related config for MongoDB Charm."""

        NAME = "database"
        PEERS = "database-peers"
        SHARDING_RELATIONS_NAME = "sharding"
        CONFIG_SERVER_RELATIONS_NAME = "config-server"
        CLUSTER_RELATIONS_NAME = "cluster"
        APP_SCOPE = "app"
        UNIT_SCOPE = "unit"
        DB_RELATIONS = [NAME]
        Scopes = Literal[APP_SCOPE, UNIT_SCOPE]

    class Role:
        """Role config names for MongoDB Charm."""

        CONFIG_SERVER = "config-server"
        REPLICATION = "replication"
        SHARD = "shard"
        MONGOS = "mongos"

    class Secrets:
        """Secrets related constants."""

        SECRET_LABEL = "secret"
        SECRET_CACHE_LABEL = "cache"
        SECRET_KEYFILE_NAME = "keyfile"
        SECRET_INTERNAL_LABEL = "internal-secret"
        SECRET_DELETED_LABEL = "None"
        MAX_PASSWORD_LENGTH = 4096

    class Status:
        """Status related constants.

        TODO: move all status messages here.
        """

        STATUS_READY_FOR_UPGRADE = "status-shows-ready-for-upgrade"

        # TODO Future PR add more status messages here as constants
        UNHEALTHY_UPGRADE = BlockedStatus("Unhealthy after refresh.")

    class Substrate:
        """Substrate related constants."""

        VM = "vm"
        K8S = "k8s"

    class Sysctl:
        """Constants for Sysctl."""

        OS_REQUIREMENTS = {
            "vm.max_map_count": "262144",
        }

    class Upgrade:
        """Upgrade related constants."""

        FEATURE_VERSION_6 = "6.0"
