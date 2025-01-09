#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from juju.errors import JujuAPIError
from pytest_operator.plugin import OpsTest

from ..helpers import DEPLOYMENT_TIMEOUT, wait_for_mongodb_units_blocked

S3_APP_NAME = "s3-integrator"
SHARD_ONE_APP_NAME = "shard"
CONFIG_SERVER_ONE_APP_NAME = "config-server-one"
CONFIG_SERVER_TWO_APP_NAME = "config-server-two"
REPLICATION_APP_NAME = "replication"
APP_CHARM_NAME = "application"
MONGOS_APP_NAME = "mongos"
MONGOS_HOST_APP_NAME = "application-host"

SHARDING_COMPONENTS = [SHARD_ONE_APP_NAME, CONFIG_SERVER_ONE_APP_NAME]

CONFIG_SERVER_REL_NAME = "config-server"
SHARD_REL_NAME = "sharding"
DATABASE_REL_NAME = "first-database"

RELATION_LIMIT_MESSAGE = 'cannot add relation "shard:sharding config-server-two:config-server": establishing a new relation for shard:sharding would exceed its maximum relation limit of 1'
# for now we have a large timeout due to the slow drainage of the `config.system.sessions`
# collection. More info here:
# https://stackoverflow.com/questions/77364840/mongodb-slow-chunk-migration-for-collection-config-system-sessions-with-remov
TIMEOUT = 30 * 60


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest,
    application_charm,
    database_charm,
    mongos_host_application_charm,
) -> None:
    """Build and deploy a sharded cluster."""
    await ops_test.model.deploy(
        database_charm,
        config={"role": "config-server"},
        application_name=CONFIG_SERVER_ONE_APP_NAME,
    )
    await ops_test.model.deploy(
        database_charm,
        config={"role": "config-server"},
        application_name=CONFIG_SERVER_TWO_APP_NAME,
    )
    await ops_test.model.deploy(
        database_charm, config={"role": "shard"}, application_name=SHARD_ONE_APP_NAME
    )
    await ops_test.model.deploy(
        MONGOS_APP_NAME,
        channel="6/edge",
    )
    await ops_test.model.deploy(S3_APP_NAME, channel="edge")

    # TODO: Future PR, once data integrator works with mongos charm deploy that charm instead of
    # packing and deploying the charm in the application dir.
    await ops_test.model.deploy(
        mongos_host_application_charm, application_name=MONGOS_HOST_APP_NAME
    )

    await ops_test.model.deploy(database_charm, application_name=REPLICATION_APP_NAME)
    await ops_test.model.deploy(application_charm, application_name=APP_CHARM_NAME)

    await ops_test.model.wait_for_idle(
        apps=[
            CONFIG_SERVER_ONE_APP_NAME,
            CONFIG_SERVER_TWO_APP_NAME,
            SHARD_ONE_APP_NAME,
        ],
        idle_period=20,
        raise_on_blocked=False,
        timeout=DEPLOYMENT_TIMEOUT,
    )

    await ops_test.model.integrate(
        f"{MONGOS_APP_NAME}",
        f"{MONGOS_HOST_APP_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[MONGOS_HOST_APP_NAME, MONGOS_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,
        timeout=TIMEOUT,
        raise_on_error=False,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_only_one_config_server_relation(ops_test: OpsTest) -> None:
    """Verify that a shard can only be related to one config server."""
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_ONE_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    with pytest.raises(JujuAPIError) as juju_error:
        await ops_test.model.integrate(
            f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
            f"{CONFIG_SERVER_TWO_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
        )

    assert (
        juju_error.value.args[0] == RELATION_LIMIT_MESSAGE
    ), "Shard can relate to multiple config servers."

    # clean up relation
    await ops_test.model.applications[SHARD_ONE_APP_NAME].remove_relation(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_ONE_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[REPLICATION_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,
        timeout=TIMEOUT,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_cannot_use_db_relation(ops_test: OpsTest) -> None:
    """Verify that sharding components cannot use the DB relation."""
    for sharded_component in SHARDING_COMPONENTS:
        await ops_test.model.integrate(f"{APP_CHARM_NAME}:{DATABASE_REL_NAME}", sharded_component)

    for sharded_component in SHARDING_COMPONENTS:
        await wait_for_mongodb_units_blocked(
            ops_test,
            sharded_component,
            status="Sharding roles do not support database interface.",
            timeout=300,
        )

    # clean up relations
    for sharded_component in SHARDING_COMPONENTS:
        await ops_test.model.applications[sharded_component].remove_relation(
            f"{APP_CHARM_NAME}:{DATABASE_REL_NAME}",
            sharded_component,
        )

    await ops_test.model.wait_for_idle(
        apps=SHARDING_COMPONENTS,
        idle_period=20,
        raise_on_blocked=False,
        timeout=TIMEOUT,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_replication_config_server_relation(ops_test: OpsTest):
    """Verifies that using a replica as a shard fails."""
    # attempt to add a replication deployment as a shard to the config server.
    await ops_test.model.integrate(
        f"{REPLICATION_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_ONE_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await wait_for_mongodb_units_blocked(
        ops_test,
        REPLICATION_APP_NAME,
        status="sharding interface cannot be used by replicas",
        timeout=300,
    )

    # clean up relations
    await ops_test.model.applications[REPLICATION_APP_NAME].remove_relation(
        f"{REPLICATION_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_ONE_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_replication_shard_relation(ops_test: OpsTest):
    """Verifies that using a replica as a config-server fails."""
    # attempt to add a shard to a replication deployment as a config server.
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{REPLICATION_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await wait_for_mongodb_units_blocked(
        ops_test,
        REPLICATION_APP_NAME,
        status="sharding interface cannot be used by replicas",
        timeout=300,
    )

    # clean up relation
    await ops_test.model.applications[REPLICATION_APP_NAME].remove_relation(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{REPLICATION_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[REPLICATION_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,
        timeout=TIMEOUT,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_replication_mongos_relation(ops_test: OpsTest) -> None:
    """Verifies connecting a replica to a mongos router fails."""
    # attempt to add a replication deployment as a shard to the config server.
    await ops_test.model.integrate(
        f"{REPLICATION_APP_NAME}",
        f"{MONGOS_APP_NAME}",
    )

    await wait_for_mongodb_units_blocked(
        ops_test,
        REPLICATION_APP_NAME,
        status="Relation to mongos not supported, config role must be config-server",
        timeout=300,
    )

    # clean up relations
    await ops_test.model.applications[REPLICATION_APP_NAME].remove_relation(
        f"{REPLICATION_APP_NAME}:cluster",
        f"{MONGOS_APP_NAME}:cluster",
    )

    await ops_test.model.wait_for_idle(
        apps=[SHARD_ONE_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,
        timeout=TIMEOUT,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_shard_mongos_relation(ops_test: OpsTest) -> None:
    """Verifies connecting a shard to a mongos router fails."""
    # attempt to add a replication deployment as a shard to the config server.
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}",
        f"{MONGOS_APP_NAME}",
    )

    await wait_for_mongodb_units_blocked(
        ops_test,
        SHARD_ONE_APP_NAME,
        status="Relation to mongos not supported, config role must be config-server",
        timeout=300,
    )

    # clean up relations
    await ops_test.model.applications[SHARD_ONE_APP_NAME].remove_relation(
        f"{MONGOS_APP_NAME}:cluster",
        f"{SHARD_ONE_APP_NAME}:cluster",
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_shard_s3_relation(ops_test: OpsTest) -> None:
    """Verifies integrating a shard to s3-integrator fails."""
    # attempt to add a replication deployment as a shard to the config server.
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}",
        f"{S3_APP_NAME}",
    )

    await wait_for_mongodb_units_blocked(
        ops_test,
        SHARD_ONE_APP_NAME,
        status="Relation to s3-integrator is not supported, config role must be config-server",
        timeout=300,
    )

    # clean up relations
    await ops_test.model.applications[SHARD_ONE_APP_NAME].remove_relation(
        f"{S3_APP_NAME}:s3-credentials",
        f"{SHARD_ONE_APP_NAME}:s3-credentials",
    )
