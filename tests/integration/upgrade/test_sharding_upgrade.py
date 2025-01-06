#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import time
from pathlib import Path

import pytest
from pytest_operator.plugin import OpsTest

from ..ha_tests import helpers as ha_helpers
from ..helpers import DEPLOYMENT_TIMEOUT, find_unit, unit_hostname
from ..sharding_tests.helpers import (
    deploy_cluster_components,
    generate_mongodb_client,
    integrate_cluster,
)
from ..sharding_tests.writes_helpers import (
    SHARD_ONE_DB_NAME,
    SHARD_TWO_DB_NAME,
    count_shard_writes,
    stop_continous_writes,
)

logger = logging.getLogger(__name__)

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
        SHARD_TWO_APP_NAME: 3,
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
async def test_upgrade(
    ops_test: OpsTest, continuous_writes_to_shard_one, continuous_writes_to_shard_two
) -> None:
    """Verify that the sharded cluster can be safely upgraded without losing writes."""
    config_server_unit = await find_unit(ops_test, leader=True, app_name=CONFIG_SERVER_APP_NAME)
    try:
        action = await config_server_unit.run_action("pre-refresh-check")
        await action.wait()
    # Catch renaming of pre-upgrade-check to pre-refresh-check
    except Exception:
        action = await config_server_unit.run_action("pre-upgrade-check")
        await action.wait()
    assert action.status == "completed", "pre-refresh-check failed, expected to succeed."

    new_charm = await ops_test.build_charm(".")
    await run_upgrade_sequence(ops_test, CONFIG_SERVER_APP_NAME, new_charm=new_charm)

    for shard_app_name in SHARD_COMPONENTS:
        await run_upgrade_sequence(ops_test, shard_app_name, new_charm=new_charm)

    # We want to be sure that everything is settled down
    await ops_test.model.wait_for_idle(
        CLUSTER_COMPONENTS, status="active", idle_period=20, timeout=20 * 60
    )
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
        shard_one_actual_writes == shard_one_expected_writes["number"]
    ), "continuous writes to shard one failed during upgrade"
    assert (
        shard_two_actual_writes == shard_two_expected_writes["number"]
    ), "continuous writes to shard two failed during upgrade"

    # after all shards have upgraded, verify that the balancer has been turned back on
    mongos_client = await generate_mongodb_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )
    balancer_state = mongos_client.admin.command("balancerStatus")
    assert balancer_state["mode"] != "off", "balancer not turned back on from config server"


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_pre_upgrade_check_failure(ops_test: OpsTest) -> None:
    """Verify that the pre-upgrade check fails if there is a problem with one of the shards."""
    # Disable network on a replicas prior to integration.
    # After disabling the network, it will be impossible to retrieve the hostname, and ip address,
    # so save them before disabling, so they can used to re-enable the network.
    shard_unit = ops_test.model.applications[SHARD_ONE_APP_NAME].units[0]
    shard_one_host_name = await unit_hostname(ops_test, shard_unit.name)
    ha_helpers.cut_network_from_unit(shard_one_host_name)

    config_server_unit = await find_unit(ops_test, leader=True, app_name=CONFIG_SERVER_APP_NAME)
    try:
        action = await config_server_unit.run_action("pre-refresh-check")
        await action.wait()
    # Catch renaming of pre-upgrade-check to pre-refresh-check
    except Exception:
        action = await config_server_unit.run_action("pre-upgrade-check")
        await action.wait()
    assert action.status == "failed", "pre-refresh-check succeeded, expected to fail."

    # re-enable network on sharded cluster and wait for idle active
    ha_helpers.restore_network_for_unit(shard_one_host_name)

    async with ops_test.fast_forward(fast_interval="1m"):
        # sleep for twice the median election time
        time.sleep(MEDIAN_REELECTION_TIME * 2)

        await ops_test.model.wait_for_idle(
            apps=CLUSTER_COMPONENTS,
            idle_period=20,
            status="active",
            raise_on_blocked=False,
        )

    # TODO Future PR: Add more cases for failing pre-refresh-check


async def run_upgrade_sequence(ops_test: OpsTest, app_name: str, new_charm: Path) -> None:
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

    await ops_test.model.applications[app_name].refresh(path=new_charm)
    await ops_test.model.wait_for_idle(apps=[app_name], timeout=1000, idle_period=120)

    # resume upgrade only needs to be ran when:
    # 1. there are more than one units in the application
    # 2. AND the underlying workload was updated
    if len(ops_test.model.applications[app_name].units) < 2:
        return

    if (
        "resume-refresh" not in ops_test.model.applications[app_name].status_message
        and "resume-upgrade" not in ops_test.model.applications[app_name].status_message
    ):
        return

    logger.info(f"Calling resume-refresh for {app_name}")
    try:
        action = await leader_unit.run_action("resume-refresh")
        await action.wait()
    # Catch renaming of resume-upgrade to resume-refresh
    except Exception:
        action = await leader_unit.run_action("resume-upgrade")
        await action.wait()
    assert action.status == "completed", "resume-refresh failed, expected to succeed."

    await ops_test.model.wait_for_idle(apps=[app_name], timeout=1000, idle_period=30)
