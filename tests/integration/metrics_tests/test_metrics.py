#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import os
import time

import ops
import pytest
import urllib3
from pytest_operator.plugin import OpsTest

from ..ha_tests import helpers as ha_helpers
from ..helpers import (
    DEPLOYMENT_TIMEOUT,
    UNIT_IDS,
    check_or_scale_app,
    find_unit,
    get_app_name,
    get_unit_ip,
    unit_hostname,
)

MONGODB_EXPORTER_PORT = 9216
MEDIAN_REELECTION_TIME = 12


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.skipif(
    os.environ.get("PYTEST_SKIP_DEPLOY", False),
    reason="skipping deploy, model expected to be provided.",
)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy one unit of MongoDB."""
    app_name = await get_app_name(ops_test)
    if app_name:
        return await check_or_scale_app(ops_test, app_name, len(UNIT_IDS))
    if await get_app_name(ops_test):
        return

    my_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(my_charm, num_units=3)
    await ops_test.model.wait_for_idle(timeout=DEPLOYMENT_TIMEOUT)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
async def test_endpoints(ops_test: OpsTest):
    """Sanity check that endpoints are running."""
    app_name = await get_app_name(ops_test)
    application = ops_test.model.applications[app_name]

    for unit in application.units:
        await verify_endpoints(ops_test, unit)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
async def test_endpoints_new_password(ops_test: OpsTest):
    """Verify that endpoints still function correctly after the monitor user password changes."""
    app_name = await get_app_name(ops_test)
    application = ops_test.model.applications[app_name]
    leader_unit = await find_unit(ops_test, leader=True)
    action = await leader_unit.run_action("set-password", **{"username": "monitor"})
    action = await action.wait()
    # wait for non-leader units to receive relation changed event.
    time.sleep(3)
    await ops_test.model.wait_for_idle(apps=[app_name], status="active", idle_period=15)
    for unit in application.units:
        await verify_endpoints(ops_test, unit)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
async def test_endpoints_network_cut(ops_test: OpsTest):
    """Verify that endpoint still function correctly after a network cut."""
    app_name = await get_app_name(ops_test)
    unit = ops_test.model.applications[app_name].units[0]
    hostname = await unit_hostname(ops_test, unit.name)
    unit_ip = await get_unit_ip(ops_test, unit.name)

    ha_helpers.cut_network_from_unit(hostname)
    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # wait until network is reestablished for the unit
    ha_helpers.restore_network_for_unit(hostname)
    ha_helpers.wait_network_restore(ops_test.model.info.name, hostname, unit_ip)
    await verify_endpoints(ops_test, unit)


# helpers


async def verify_endpoints(ops_test: OpsTest, unit: ops.model.Unit) -> str:
    """Verifies mongodb endpoint is functional on a given unit."""
    http = urllib3.PoolManager()

    unit_address = await get_unit_ip(ops_test, unit.name)
    mongodb_exporter_url = f"http://{unit_address}:{MONGODB_EXPORTER_PORT}/metrics"
    mongo_resp = http.request("GET", mongodb_exporter_url)

    assert mongo_resp.status == 200

    # if configured correctly there should be more than one mongodb metric present
    mongodb_metrics = mongo_resp._body.decode("utf8")
    assert mongodb_metrics.count("mongo") > 1
