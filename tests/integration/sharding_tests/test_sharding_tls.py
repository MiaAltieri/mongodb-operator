#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.


import pytest
from pytest_operator.plugin import OpsTest

from ..helpers import (
    DEPLOYMENT_TIMEOUT,
    destroy_cluster,
    wait_for_mongodb_units_blocked,
)
from ..tls_tests import helpers as tls_helpers
from .helpers import deploy_cluster_components, integrate_cluster

MONGOD_SERVICE = "snap.charmed-mongodb.mongod.service"
MONGOS_SERVICE = "snap.charmed-mongodb.mongos.service"
DIFFERENT_CERTS_APP_NAME = "self-signed-certificates-separate"
CERTS_APP_NAME = "self-signed-certificates"
SHARD_ONE_APP_NAME = "shard-one"
SHARD_TWO_APP_NAME = "shard-two"
CONFIG_SERVER_APP_NAME = "config-server"
CLUSTER_COMPONENTS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, CONFIG_SERVER_APP_NAME]
SHARD_REL_NAME = "sharding"
CONFIG_SERVER_REL_NAME = "config-server"
CERT_REL_NAME = "certificates"
TIMEOUT = 15 * 60


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy a sharded cluster."""
    await deploy_cluster_components(ops_test)

    # deploy the s3 integrator charm
    await ops_test.model.deploy(CERTS_APP_NAME, channel="stable")

    await ops_test.model.wait_for_idle(
        apps=[CERTS_APP_NAME, CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME],
        idle_period=20,
        raise_on_blocked=False,
        timeout=DEPLOYMENT_TIMEOUT,
        raise_on_error=False,
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_built_cluster_with_tls(ops_test: OpsTest) -> None:
    """Tests that the cluster can be integrated with TLS."""
    await integrate_cluster(ops_test)
    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
    )

    await integrate_with_tls(ops_test)

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
    )

    await check_cluster_tls_enabled(ops_test)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_rotate_tls(ops_test: OpsTest) -> None:
    """Tests that each cluster component can rotate TLS certs."""
    for cluster_app in CLUSTER_COMPONENTS:
        await rotate_and_verify_certs(ops_test, cluster_app)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_disable_cluster_with_tls(ops_test: OpsTest) -> None:
    """Tests that the cluster can disable TLS."""
    await remove_tls_integrations(ops_test)
    await check_cluster_tls_disabled(ops_test)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_tls_then_build_cluster(ops_test: OpsTest) -> None:
    """Tests that the cluster can be integrated with TLS."""
    await destroy_cluster(ops_test, applications=CLUSTER_COMPONENTS)
    await deploy_cluster_components(ops_test)

    await integrate_with_tls(ops_test)
    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
    )

    await integrate_cluster(ops_test)

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
    )

    await check_cluster_tls_enabled(ops_test)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_tls_inconsistent_rels(ops_test: OpsTest) -> None:
    await ops_test.model.deploy(
        CERTS_APP_NAME, application_name=DIFFERENT_CERTS_APP_NAME, channel="stable"
    )

    # CASE 1: Config-server has TLS enabled - but shard does not
    await ops_test.model.applications[SHARD_ONE_APP_NAME].remove_relation(
        f"{SHARD_ONE_APP_NAME}:{CERT_REL_NAME}",
        f"{CERTS_APP_NAME}:{CERT_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_blocked=False,
    )

    await wait_for_mongodb_units_blocked(
        ops_test, SHARD_ONE_APP_NAME, status="Shard requires TLS to be enabled.", timeout=450
    )

    # Re-integrate to bring cluster back to steady state
    await ops_test.model.integrate(
        f"{SHARD_ONE_APP_NAME}:{CERT_REL_NAME}",
        f"{CERTS_APP_NAME}:{CERT_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_blocked=False,
        status="active",
    )

    # CASE 2: Config-server does not have TLS enabled - but shard does
    await ops_test.model.applications[CONFIG_SERVER_APP_NAME].remove_relation(
        f"{CONFIG_SERVER_APP_NAME}:{CERT_REL_NAME}",
        f"{CERTS_APP_NAME}:{CERT_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_blocked=False,
    )
    await wait_for_mongodb_units_blocked(
        ops_test,
        SHARD_ONE_APP_NAME,
        status="Shard has TLS enabled, but config-server does not.",
        timeout=450,
    )

    # CASE 3: Cluster components are using different CA's

    # Re-integrate to bring cluster back to steady state
    await ops_test.model.integrate(
        f"{CONFIG_SERVER_APP_NAME}:{CERT_REL_NAME}",
        f"{DIFFERENT_CERTS_APP_NAME}:{CERT_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=CLUSTER_COMPONENTS,
        idle_period=20,
        timeout=TIMEOUT,
        raise_on_blocked=False,
    )
    await wait_for_mongodb_units_blocked(
        ops_test,
        SHARD_ONE_APP_NAME,
        status="Shard CA and Config-Server CA don't match.",
        timeout=450,
    )


async def check_cluster_tls_disabled(ops_test: OpsTest) -> None:
    # check each replica set is running with TLS enabled
    for cluster_component in CLUSTER_COMPONENTS:
        for unit in ops_test.model.applications[cluster_component].units:
            assert await tls_helpers.check_tls(
                ops_test, unit, enabled=False, app_name=cluster_component, mongos=False
            ), f"MongoDB TLS not disabled in unit {unit.name}"

    # check mongos is running with TLS enabled
    for unit in ops_test.model.applications[CONFIG_SERVER_APP_NAME].units:
        assert await tls_helpers.check_tls(
            ops_test, unit, enabled=False, app_name=CONFIG_SERVER_APP_NAME, mongos=True
        ), f"Mongos TLS not disabled in unit {unit.name}"


async def check_cluster_tls_enabled(ops_test: OpsTest) -> None:
    # check each replica set is running with TLS enabled
    for cluster_component in CLUSTER_COMPONENTS:
        for unit in ops_test.model.applications[cluster_component].units:
            assert await tls_helpers.check_tls(
                ops_test, unit, enabled=True, app_name=cluster_component, mongos=False
            ), f"MongoDB TLS not enabled in unit {unit.name}"

    # check mongos is running with TLS enabled
    for unit in ops_test.model.applications[CONFIG_SERVER_APP_NAME].units:
        assert await tls_helpers.check_tls(
            ops_test, unit, enabled=True, app_name=CONFIG_SERVER_APP_NAME, mongos=True
        ), f"Mongos TLS not enabled in unit {unit.name}"


async def remove_tls_integrations(ops_test: OpsTest) -> None:
    """Removes the TLS integration from all cluster components."""
    for app in CLUSTER_COMPONENTS:
        await ops_test.model.applications[app].remove_relation(
            f"{app}:{CERT_REL_NAME}",
            f"{CERTS_APP_NAME}:{CERT_REL_NAME}",
        )


async def integrate_with_tls(ops_test: OpsTest) -> None:
    """Integrates cluster components with self-signed certs operator."""
    for app in CLUSTER_COMPONENTS:
        await ops_test.model.integrate(
            f"{CERTS_APP_NAME}:{CERT_REL_NAME}",
            f"{app}:{CERT_REL_NAME}",
        )


async def rotate_and_verify_certs(ops_test: OpsTest, app: str) -> None:
    """Verify provided app can rotate its TLS certs."""
    original_tls_info = {}
    for unit in ops_test.model.applications[app].units:
        original_tls_info[unit.name] = {}
        original_tls_info[unit.name]["external_cert_contents"] = (
            await tls_helpers.get_file_content(ops_test, unit.name, tls_helpers.EXTERNAL_CERT_PATH)
        )
        original_tls_info[unit.name]["internal_cert_contents"] = (
            await tls_helpers.get_file_content(ops_test, unit.name, tls_helpers.INTERNAL_CERT_PATH)
        )
        original_tls_info[unit.name]["external_cert"] = await tls_helpers.time_file_created(
            ops_test, unit.name, tls_helpers.EXTERNAL_CERT_PATH
        )
        original_tls_info[unit.name]["internal_cert"] = await tls_helpers.time_file_created(
            ops_test, unit.name, tls_helpers.INTERNAL_CERT_PATH
        )
        original_tls_info[unit.name]["mongod_service"] = await tls_helpers.time_process_started(
            ops_test, unit.name, MONGOD_SERVICE
        )
        if app == CONFIG_SERVER_APP_NAME:
            original_tls_info[unit.name]["mongos_service"] = (
                await tls_helpers.time_process_started(ops_test, unit.name, MONGOD_SERVICE)
            )
        await tls_helpers.check_certs_correctly_distributed(ops_test, unit, app_name=app)

    # set external and internal key using auto-generated key for each unit
    for unit in ops_test.model.applications[app].units:
        action = await unit.run_action(action_name="set-tls-private-key")
        action = await action.wait()
        assert action.status == "completed", "setting external and internal key failed."

    # wait for certificate to be available and processed. Can get receive two certificate
    # available events and restart twice so we want to ensure we are idle for at least 1 minute
    await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=1000, idle_period=60)

    # After updating both the external key and the internal key a new certificate request will be
    # made; then the certificates should be available and updated.
    for unit in ops_test.model.applications[app].units:
        new_external_cert = await tls_helpers.get_file_content(
            ops_test, unit.name, tls_helpers.EXTERNAL_CERT_PATH
        )
        new_internal_cert = await tls_helpers.get_file_content(
            ops_test, unit.name, tls_helpers.INTERNAL_CERT_PATH
        )
        new_external_cert_time = await tls_helpers.time_file_created(
            ops_test, unit.name, tls_helpers.EXTERNAL_CERT_PATH
        )
        new_internal_cert_time = await tls_helpers.time_file_created(
            ops_test, unit.name, tls_helpers.INTERNAL_CERT_PATH
        )
        new_mongod_service_time = await tls_helpers.time_process_started(
            ops_test, unit.name, MONGOD_SERVICE
        )
        if app == CONFIG_SERVER_APP_NAME:
            new_mongos_service_time = await tls_helpers.time_process_started(
                ops_test, unit.name, MONGOS_SERVICE
            )

        await tls_helpers.check_certs_correctly_distributed(ops_test, unit, app_name=app)
        assert (
            new_external_cert != original_tls_info[unit.name]["external_cert_contents"]
        ), "external cert not rotated"

        assert (
            new_internal_cert != original_tls_info[unit.name]["external_cert_contents"]
        ), "external cert not rotated"
        assert (
            new_external_cert_time > original_tls_info[unit.name]["external_cert"]
        ), f"external cert for {unit.name} was not updated."
        assert (
            new_internal_cert_time > original_tls_info[unit.name]["internal_cert"]
        ), f"internal cert for {unit.name} was not updated."

        # Once the certificate requests are processed and updated the .service file should be
        # restarted
        assert (
            new_mongod_service_time > original_tls_info[unit.name]["mongod_service"]
        ), f"mongod service for {unit.name} was not restarted."

        if app == CONFIG_SERVER_APP_NAME:
            assert (
                new_mongos_service_time > original_tls_info[unit.name]["mongos_service"]
            ), f"mongos service for {unit.name} was not restarted."

    # Verify that TLS is functioning on all units.
    await check_cluster_tls_enabled(ops_test)
