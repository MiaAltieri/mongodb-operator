#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
from pathlib import Path

import pytest
from pytest_operator.plugin import OpsTest

from ..helpers import DEPLOYMENT_TIMEOUT, find_unit, wait_for_mongodb_units_blocked
from ..sharding_tests.helpers import deploy_cluster_components, integrate_cluster
from ..sharding_tests.writes_helpers import (
    SHARD_ONE_DB_NAME,
    SHARD_TWO_DB_NAME,
    count_shard_writes,
    stop_continous_writes,
)

MONGOD_SERVICE = "snap.charmed-mongodb.mongod.service"
MONGOS_SERVICE = "snap.charmed-mongodb.mongos.service"
SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
CONFIG_SERVER_APP_NAME = "config-server"
SHARD_COMPONENTS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME]
CLUSTER_COMPONENTS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, CONFIG_SERVER_APP_NAME]
SHARD_REL_NAME = "sharding"
CONFIG_SERVER_REL_NAME = "config-server"
MEDIAN_REELECTION_TIME = 12


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build deploy, and integrate, a sharded cluster."""
    num_units_cluster_config = {
        CONFIG_SERVER_APP_NAME: 3,
        SHARD_ONE_APP_NAME: 3,
        SHARD_TWO_APP_NAME: 1,
    }
    await deploy_cluster_components(ops_test, num_units_cluster_config, channel="6/edge")

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=DEPLOYMENT_TIMEOUT,
        raise_on_blocked=False,
    )
    await integrate_cluster(ops_test)
    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        status="active",
        idle_period=20,
        timeout=DEPLOYMENT_TIMEOUT,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_rollback_on_config_server(
    ops_test: OpsTest, continuous_writes_to_shard_one, continuous_writes_to_shard_two
) -> None:
    """Verify that the config-server can safely rollback without losing writes."""
    new_charm = await ops_test.build_charm(".")
    config_server_unit = await find_unit(ops_test, leader=True, app_name=CONFIG_SERVER_APP_NAME)
    try:
        action = await config_server_unit.run_action("pre-refresh-check")
        await action.wait()
    # Catch renaming of pre-upgrade-check to pre-refresh-check
    except Exception:
        action = await config_server_unit.run_action("pre-upgrade-check")
        await action.wait()
    assert action.status == "completed", "pre-refresh-check failed, expected to succeed."

    await ops_test.model.applications[CONFIG_SERVER_APP_NAME].refresh(path=new_charm)
    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME], timeout=1000, idle_period=120
    )

    # instead of resuming upgrade refresh with the old version
    # TODO: Use this when https://github.com/juju/python-libjuju/issues/1086 is fixed
    # await ops_test.model.applications[CONFIG_SERVER_APP_NAME].refresh(
    #     channel="6/edge", switch="ch:mongodb"
    # )
    await refresh_with_juju(ops_test, CONFIG_SERVER_APP_NAME, "6/edge")

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME],
        timeout=1000,
        idle_period=30,
    )

    # verify no writes were skipped during upgrade/rollback process
    shard_one_expected_writes = await stop_continous_writes(
        ops_test,
        config_server_name=CONFIG_SERVER_APP_NAME,
        db_name=SHARD_ONE_DB_NAME,
    )
    shard_two_expected_writes = await stop_continous_writes(
        ops_test,
        config_server_name=CONFIG_SERVER_APP_NAME,
        db_name=SHARD_TWO_DB_NAME,
    )

    shard_one_actual_writes = await count_shard_writes(
        ops_test, CONFIG_SERVER_APP_NAME, SHARD_ONE_DB_NAME
    )
    shard_two_actual_writes = await count_shard_writes(
        ops_test, CONFIG_SERVER_APP_NAME, SHARD_TWO_DB_NAME
    )
    assert (
        shard_one_actual_writes == shard_one_expected_writes["number"]
    ), "continuous writes to shard one failed during upgrade"
    assert (
        shard_two_actual_writes == shard_two_expected_writes["number"]
    ), "continuous writes to shard two failed during upgrade"

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS, status="active", timeout=1000, idle_period=20
    )

    # after all shards have upgraded, verify that the balancer has been turned back on
    # TODO implement this check once we have implemented the post-cluster-upgrade code DPE-4143


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_rollback_on_shard_and_config_server(
    ops_test: OpsTest, continuous_writes_to_shard_one, continuous_writes_to_shard_two
) -> None:
    """Verify that a config-server and shard can safely rollback without losing writes."""
    new_charm = await ops_test.build_charm(".")
    await run_upgrade_sequence(ops_test, CONFIG_SERVER_APP_NAME, new_charm=new_charm)

    with open("charm_version", mode="r") as fd:
        revision = fd.read().strip()

    # Wait for statuses to settle down
    asyncio.gather(
        wait_for_mongodb_units_blocked(ops_test, SHARD_ONE_APP_NAME),
        wait_for_mongodb_units_blocked(ops_test, SHARD_TWO_APP_NAME),
        ops_test.model.wait_for_idle(
            apps=[CONFIG_SERVER_APP_NAME],
            timeout=1000,
            idle_period=20,
            status=f"Waiting for shards to upgrade/downgrade to revision {revision}-locally built.",
        ),
    )

    await run_upgrade_sequence(ops_test, SHARD_ONE_APP_NAME, new_charm=new_charm)

    # Wait for statuses to settle down
    asyncio.gather(
        wait_for_mongodb_units_blocked(ops_test, SHARD_TWO_APP_NAME),
        ops_test.model.wait_for_idle(apps=[SHARD_ONE_APP_NAME], timeout=1000, idle_period=20),
        ops_test.model.wait_for_idle(
            apps=[CONFIG_SERVER_APP_NAME],
            timeout=1000,
            idle_period=20,
            status=f"Waiting for shards to upgrade/downgrade to revision {revision}-locally built.",
        ),
    )

    await refresh_with_juju(ops_test, CONFIG_SERVER_APP_NAME, channel="6/edge")

    # verify no writes were skipped during upgrade process
    shard_one_expected_writes = await stop_continous_writes(
        ops_test,
        config_server_name=CONFIG_SERVER_APP_NAME,
        db_name=SHARD_ONE_DB_NAME,
    )
    shard_two_expected_writes = await stop_continous_writes(
        ops_test,
        config_server_name=CONFIG_SERVER_APP_NAME,
        db_name=SHARD_TWO_DB_NAME,
    )

    shard_one_actual_writes = await count_shard_writes(
        ops_test, CONFIG_SERVER_APP_NAME, SHARD_ONE_DB_NAME
    )
    shard_two_actual_writes = await count_shard_writes(
        ops_test, CONFIG_SERVER_APP_NAME, SHARD_TWO_DB_NAME
    )
    assert (
        shard_one_actual_writes >= shard_one_expected_writes["number"]
    ), "continuous writes to shard one failed during upgrade"
    assert (
        shard_two_actual_writes >= shard_two_expected_writes["number"]
    ), "continuous writes to shard two failed during upgrade"

    # after all shards have upgraded, verify that the balancer has been turned back on
    # TODO implement this check once we have implemented the post-cluster-upgrade code DPE-4143


async def refresh_with_juju(ops_test: OpsTest, app_name: str, channel: str) -> None:
    refresh_cmd = f"refresh {app_name} --model {ops_test.model.info.name} --channel {channel} --switch ch:mongodb"
    await ops_test.juju(*refresh_cmd.split())


async def run_upgrade_sequence(
    ops_test: OpsTest,
    app_name: str,
    new_charm: Path | None = None,
    channel: str | None = None,
) -> None:
    """Runs the upgrade sequence on a given app."""
    leader_unit = await find_unit(ops_test, leader=True, app_name=app_name)
    try:
        action = await leader_unit.run_action("pre-refresh-check")
        await action.wait()
    # Catch renaming of pre-upgrade-check to pre-refresh-check
    except Exception:
        action = await leader_unit.run_action("pre-upgrade-check")
        await action.wait()
    assert action.status == "completed", "pre-refresh-check failed, expected to succeed."

    if new_charm is not None:
        await ops_test.model.applications[app_name].refresh(path=new_charm)
    elif channel is not None:
        # TODO: Use this when https://github.com/juju/python-libjuju/issues/1086 is fixed
        # await ops_test.model.applications[app_name].refresh(
        #     channel=channel, switch="ch:mongodb"
        # )
        await refresh_with_juju(ops_test, app_name, channel)
    else:
        raise ValueError("Either new_charm or channel must be provided.")

    await ops_test.model.wait_for_idle(apps=[app_name], timeout=1000, idle_period=120)

    # resume upgrade only needs to be ran when:
    # 1. there are more than one units in the application
    # 2. AND the underlying workload was updated
    if not len(ops_test.model.applications[app_name].units) > 1:
        return

    if (
        "resume-refresh" not in ops_test.model.applications[app_name].status_message
        and "resume-upgrade" not in ops_test.model.applications[app_name].status_message
    ):
        return

    try:
        action = await leader_unit.run_action("resume-refresh")
        await action.wait()
    # Catch renaming of resume-upgrade to resume-refresh
    except Exception:
        action = await leader_unit.run_action("resume-upgrade")
        await action.wait()
    assert action.status == "completed", "resume-refresh failed, expected to succeed."

    await ops_test.model.wait_for_idle(apps=[app_name], timeout=1000, idle_period=120)
