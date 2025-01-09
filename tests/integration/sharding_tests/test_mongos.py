#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


import pytest
from pymongo.errors import OperationFailure
from pytest_operator.plugin import OpsTest

from ..helpers import DEPLOYMENT_TIMEOUT
from .helpers import count_users, generate_mongodb_client, get_username_password

MONGOS_HOST_APP_NAME = "application"
SHARD_ONE_APP_NAME = "shard-one"
MONGOS_APP_NAME = "mongos"
CONFIG_SERVER_APP_NAME = "config-server-one"
SHARD_REL_NAME = "sharding"
CLUSTER_REL_NAME = "cluster"
CONFIG_SERVER_REL_NAME = "config-server"
MONGODB_KEYFILE_PATH = "/var/snap/charmed-mongodb/current/etc/mongod/keyFile"
TIMEOUT = 10 * 60


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, mongos_host_application_charm) -> None:
    """Build and deploy a sharded cluster."""
    mongodb_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(
        mongodb_charm,
        num_units=1,
        config={"role": "config-server"},
        application_name=CONFIG_SERVER_APP_NAME,
    )
    await ops_test.model.deploy(
        mongodb_charm, num_units=1, config={"role": "shard"}, application_name=SHARD_ONE_APP_NAME
    )

    await ops_test.model.deploy(
        MONGOS_APP_NAME,
        channel="6/edge",
        revision=3,
    )

    # TODO: Future PR, once data integrator works with mongos charm deploy that charm instead of
    # packing and deploying the charm in the application dir.
    await ops_test.model.deploy(
        mongos_host_application_charm, application_name=MONGOS_HOST_APP_NAME
    )

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, MONGOS_HOST_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,  # cluster components are blocked waiting for integration.
        timeout=DEPLOYMENT_TIMEOUT,
        raise_on_error=False,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_connect_to_cluster_creates_user(ops_test: OpsTest) -> None:
    """Verifies that when the cluster is formed a new user is created."""
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{SHARD_REL_NAME}",
        f"{CONFIG_SERVER_APP_NAME}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.integrate(
        f"{MONGOS_APP_NAME}",
        f"{MONGOS_HOST_APP_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[MONGOS_HOST_APP_NAME, MONGOS_APP_NAME, SHARD_ONE_APP_NAME, CONFIG_SERVER_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,
        timeout=TIMEOUT,
        raise_on_error=False,
    )

    mongos_client = await generate_mongodb_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )
    num_users = count_users(mongos_client)

    await ops_test.model.integrate(
        f"{MONGOS_APP_NAME}",
        f"{CONFIG_SERVER_APP_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, MONGOS_APP_NAME],
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_error=False,
    )

    num_users_after_integration = count_users(mongos_client)

    assert (
        num_users_after_integration > num_users
    ), "Cluster did not create new users after integration."

    (username, password) = await get_username_password(
        ops_test, app_name=MONGOS_APP_NAME, relation_name=CLUSTER_REL_NAME
    )
    mongos_user_client = await generate_mongodb_client(
        ops_test,
        app_name=CONFIG_SERVER_APP_NAME,
        mongos=True,
        username=username,
        password=password,
    )

    mongos_user_client.admin.command("dbStats")


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_disconnect_from_cluster_removes_user(ops_test: OpsTest) -> None:
    """Verifies that when the cluster is formed a the user is removed."""
    # generate URI for new mongos user
    (username, password) = await get_username_password(
        ops_test, app_name=MONGOS_APP_NAME, relation_name=CLUSTER_REL_NAME
    )
    mongos_user_client = await generate_mongodb_client(
        ops_test,
        app_name=CONFIG_SERVER_APP_NAME,
        mongos=True,
        username=username,
        password=password,
    )

    # generate URI for operator mongos user (i.e. admin)
    mongos_client = await generate_mongodb_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )
    num_users = count_users(mongos_client)
    await ops_test.model.applications[MONGOS_APP_NAME].remove_relation(
        f"{MONGOS_APP_NAME}:cluster",
        f"{CONFIG_SERVER_APP_NAME}:cluster",
    )
    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, MONGOS_APP_NAME],
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_error=False,
    )
    num_users_after_removal = count_users(mongos_client)

    assert (
        num_users > num_users_after_removal
    ), "Cluster did not remove user after integration removal."

    with pytest.raises(OperationFailure) as pymongo_error:
        mongos_user_client.admin.command("dbStats")

    assert pymongo_error.value.code == 18, "User still exists after relation was removed."
