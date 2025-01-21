#!/usr/bin/env python3
"""Charm code for MongoDB service."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from ops.main import main
from single_kernel_mongo.abstract_charm import AbstractMongoCharm
from single_kernel_mongo.config.literals import Substrates
from single_kernel_mongo.config.relations import PeerRelationNames
from single_kernel_mongo.core.structured_config import MongoDBCharmConfig
from single_kernel_mongo.managers.mongodb_operator import MongoDBOperator


class MongodbOperatorCharm(AbstractMongoCharm[MongoDBCharmConfig, MongoDBOperator]):
    """Charm the service."""

    config_type = MongoDBCharmConfig
    operator_type = MongoDBOperator
    substrate = Substrates.VM
    peer_rel_name = PeerRelationNames.PEERS
    name = "mongodb"


if __name__ == "__main__":
    main(MongodbOperatorCharm)
