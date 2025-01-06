#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import secrets
import string
import time
from typing import Dict

import pytest
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_delay, wait_fixed

from ..backup_tests import helpers as backup_helpers
from ..helpers import (
    DEPLOYMENT_TIMEOUT,
    destroy_cluster,
    get_leader_id,
    get_password,
    set_password,
)
from . import writes_helpers
from .helpers import generate_mongodb_client, write_data_to_mongodb

S3_APP_NAME = "s3-integrator"
SHARD_ONE_APP_NAME = "shard-one"
SHARD_ONE_APP_NAME_NEW = "shard-one-new"
SHARD_TWO_APP_NAME = "shard-two"
SHARD_TWO_APP_NAME_NEW = "shard-two-new"
CONFIG_SERVER_APP_NAME = "config-server"
CONFIG_SERVER_APP_NAME_NEW = "config-server-new"
SHARD_APPS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME]
CLUSTER_APPS = [SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME, CONFIG_SERVER_APP_NAME]
CLUSTER_APPS_NEW = [SHARD_ONE_APP_NAME_NEW, SHARD_TWO_APP_NAME_NEW, CONFIG_SERVER_APP_NAME_NEW]
SHARD_REL_NAME = "sharding"
CONFIG_SERVER_REL_NAME = "config-server"
S3_REL_NAME = "s3-credentials"
OPERATOR_PASSWORD = "operator-password"
SHARD_ONE_DB_NAME = "new-db"
SHARD_ONE_COLL_NAME = "test_collection"
SHARD_TWO_DB_NAME = "new-db-2"
SHARD_TWO_COLL_NAME = "test_collection"
TIMEOUT = 10 * 60


@pytest.fixture()
async def add_writes_to_shards(ops_test: OpsTest):
    """Adds writes to each shard before test starts and clears writes at the end of the test."""
    await writes_helpers.start_continous_writes(
        ops_test, 1, config_server_name=CONFIG_SERVER_APP_NAME
    )
    time.sleep(20)
    await writes_helpers.stop_continous_writes(ops_test, config_server_name=CONFIG_SERVER_APP_NAME)

    # move continuous writes to shard-one
    mongos_client = await generate_mongodb_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )

    mongos_client.admin.command("movePrimary", SHARD_ONE_DB_NAME, to=SHARD_ONE_APP_NAME)

    # add writes to shard-two
    write_data_to_mongodb(
        mongos_client,
        db_name=SHARD_TWO_DB_NAME,
        coll_name=SHARD_TWO_COLL_NAME,
        content={"horse-breed": "unicorn", "real": True},
    )
    mongos_client.admin.command("movePrimary", SHARD_TWO_DB_NAME, to=SHARD_TWO_APP_NAME)

    yield
    await writes_helpers.remove_db_writes(
        ops_test, db_name=SHARD_ONE_DB_NAME, coll_name=SHARD_ONE_COLL_NAME
    )
    await writes_helpers.remove_db_writes(
        ops_test, db_name=SHARD_TWO_DB_NAME, coll_name=SHARD_TWO_COLL_NAME
    )


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy a sharded cluster."""
    await deploy_cluster_backup_test(ops_test)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_set_credentials_in_cluster(ops_test: OpsTest, github_secrets) -> None:
    """Tests that sharded cluster can be configured for s3 configurations."""
    await backup_helpers.set_credentials(ops_test, github_secrets, cloud="AWS")
    choices = string.ascii_letters + string.digits
    unique_path = "".join([secrets.choice(choices) for _ in range(4)])
    configuration_parameters = {
        "bucket": "data-charms-testing",
        "path": f"mongodb-vm/test-{unique_path}",
        "endpoint": "https://s3.amazonaws.com",
        "region": "us-east-1",
    }

    # apply new configuration options
    await ops_test.model.applications[S3_APP_NAME].set_config(configuration_parameters)
    await ops_test.model.wait_for_idle(apps=[S3_APP_NAME], status="active", timeout=TIMEOUT)
    await setup_cluster_and_s3(ops_test)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_create_and_list_backups_in_cluster(ops_test: OpsTest) -> None:
    """Tests that sharded cluster can successfully create and list backups."""
    # verify backup list works
    backups = await backup_helpers.get_backup_list(ops_test, db_app_name=CONFIG_SERVER_APP_NAME)
    assert backups, "backups not outputted"

    # verify backup is started
    leader_unit = await backup_helpers.get_leader_unit(
        ops_test, db_app_name=CONFIG_SERVER_APP_NAME
    )
    action = await leader_unit.run_action(action_name="create-backup")
    backup_result = await action.wait()
    assert "backup started" in backup_result.results["backup-status"], "backup didn't start"

    # verify backup is present in the list of backups
    # the action `create-backup` only confirms that the command was sent to the `pbm`. Creating a
    # backup can take a lot of time so this function returns once the command was successfully
    # sent to pbm. Therefore we should retry listing the backup several times
    for attempt in Retrying(stop=stop_after_delay(TIMEOUT), wait=wait_fixed(3), reraise=True):
        with attempt:
            backups = await backup_helpers.count_logical_backups(leader_unit)
            assert backups == 1


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_shards_cannot_run_backup_actions(ops_test: OpsTest) -> None:
    shard_unit = await backup_helpers.get_leader_unit(ops_test, db_app_name=SHARD_ONE_APP_NAME)
    action = await shard_unit.run_action(action_name="create-backup")
    attempted_backup = await action.wait()
    assert attempted_backup.status == "failed", "shard ran create-backup command."

    action = await shard_unit.run_action(action_name="list-backups")
    attempted_backup = await action.wait()
    assert attempted_backup.status == "failed", "shard ran list-backup command."

    action = await shard_unit.run_action(action_name="restore")
    attempted_backup = await action.wait()
    assert attempted_backup.status == "failed", "shard ran list-backup command."


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_rotate_backup_password(ops_test: OpsTest) -> None:
    """Tests that sharded cluster can successfully create and list backups."""
    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME],
        idle_period=20,
        timeout=TIMEOUT,
        status="active",
    )
    config_leader_id = await get_leader_id(ops_test, app_name=CONFIG_SERVER_APP_NAME)
    new_password = "new-password"

    shard_backup_password = await get_password(
        ops_test, username="backup", app_name=SHARD_ONE_APP_NAME
    )
    assert (
        shard_backup_password != new_password
    ), "shard-one is incorrectly already set to the new password."

    shard_backup_password = await get_password(
        ops_test, username="backup", app_name=SHARD_TWO_APP_NAME
    )
    assert (
        shard_backup_password != new_password
    ), "shard-two is incorrectly already set to the new password."

    await set_password(
        ops_test, unit_id=config_leader_id, username="backup", password=new_password
    )
    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME],
        idle_period=20,
        timeout=TIMEOUT,
        status="active",
    )
    config_svr_backup_password = await get_password(
        ops_test, username="backup", app_name=CONFIG_SERVER_APP_NAME
    )

    assert (
        config_svr_backup_password == new_password
    ), "Application config-srver did not rotate password"

    shard_backup_password = await get_password(
        ops_test, username="backup", app_name=SHARD_ONE_APP_NAME
    )
    assert shard_backup_password == new_password, "Application shard-one did not rotate password"

    shard_backup_password = await get_password(
        ops_test, username="backup", app_name=SHARD_TWO_APP_NAME
    )
    assert shard_backup_password == new_password, "Application shard-two did not rotate password"

    # verify backup actions work after password rotation
    leader_unit = await backup_helpers.get_leader_unit(
        ops_test, db_app_name=CONFIG_SERVER_APP_NAME
    )
    action = await leader_unit.run_action(action_name="create-backup")
    backup_result = await action.wait()
    assert (
        "backup started" in backup_result.results["backup-status"]
    ), "backup didn't start after password rotation"

    # verify backup is present in the list of backups
    # the action `create-backup` only confirms that the command was sent to the `pbm`. Creating a
    # backup can take a lot of time so this function returns once the command was successfully
    # sent to pbm. Therefore we should retry listing the backup several times
    for attempt in Retrying(stop=stop_after_delay(20), wait=wait_fixed(3), reraise=True):
        with attempt:
            backups = await backup_helpers.count_logical_backups(leader_unit)
            assert backups == 2, "Backup not created after password rotation."


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_restore_backup(ops_test: OpsTest, add_writes_to_shards) -> None:
    """Tests that sharded Charmed MongoDB cluster supports restores."""
    # count total writes
    cluster_writes = await writes_helpers.get_cluster_writes_count(
        ops_test,
        shard_app_names=SHARD_APPS,
        db_names=[SHARD_ONE_DB_NAME, SHARD_TWO_DB_NAME],
        config_server_name=CONFIG_SERVER_APP_NAME,
    )

    assert cluster_writes["total_writes"], "no writes to backup"
    assert cluster_writes[SHARD_ONE_APP_NAME], "no writes to backup for shard one"
    assert cluster_writes[SHARD_TWO_APP_NAME], "no writes to backup for shard two"
    assert (
        cluster_writes[SHARD_ONE_APP_NAME] + cluster_writes[SHARD_TWO_APP_NAME]
        == cluster_writes["total_writes"]
    ), "writes not synced"

    leader_unit = await backup_helpers.get_leader_unit(
        ops_test, db_app_name=CONFIG_SERVER_APP_NAME
    )
    prev_backups = await backup_helpers.count_logical_backups(leader_unit)
    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME], status="active", idle_period=20
    ),
    action = await leader_unit.run_action(action_name="create-backup")
    first_backup = await action.wait()
    assert first_backup.status == "completed", "First backup not started."

    # verify that backup was made on the bucket
    for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(5), reraise=True):
        with attempt:
            backups = await backup_helpers.count_logical_backups(leader_unit)
            assert backups == prev_backups + 1, "Backup not created."

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME], status="active", idle_period=20
    ),

    # add writes to be cleared after restoring the backup.
    await add_and_verify_unwanted_writes(ops_test, cluster_writes)

    # find most recent backup id and restore
    list_result = await backup_helpers.get_backup_list(
        ops_test, db_app_name=CONFIG_SERVER_APP_NAME
    )
    most_recent_backup = list_result.split("\n")[-1]
    backup_id = most_recent_backup.split()[0]
    action = await leader_unit.run_action(action_name="restore", **{"backup-id": backup_id})
    restore = await action.wait()
    assert restore.results["restore-status"] == "restore started", "restore not successful"

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME], status="active", idle_period=20
    ),

    await verify_writes_restored(ops_test, cluster_writes)


@pytest.mark.runner(["self-hosted", "linux", "X64", "jammy", "large"])
@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_migrate_restore_backup(ops_test: OpsTest, add_writes_to_shards) -> None:
    """Tests that sharded Charmed MongoDB cluster supports restores."""
    config_leader_id = await get_leader_id(ops_test, app_name=CONFIG_SERVER_APP_NAME)
    await set_password(
        ops_test, unit_id=config_leader_id, username="operator", password=OPERATOR_PASSWORD
    )
    await ops_test.model.wait_for_idle(apps=CLUSTER_APPS, status="active", idle_period=20)

    # count total writes
    cluster_writes = await writes_helpers.get_cluster_writes_count(
        ops_test,
        shard_app_names=SHARD_APPS,
        db_names=[SHARD_ONE_DB_NAME, SHARD_TWO_DB_NAME],
        config_server_name=CONFIG_SERVER_APP_NAME,
    )
    assert cluster_writes["total_writes"], "no writes to backup"
    assert cluster_writes[SHARD_ONE_APP_NAME], "no writes to backup for shard one"
    assert cluster_writes[SHARD_TWO_APP_NAME], "no writes to backup for shard two"
    assert (
        cluster_writes[SHARD_ONE_APP_NAME] + cluster_writes[SHARD_TWO_APP_NAME]
        == cluster_writes["total_writes"]
    ), "writes not synced"

    leader_unit = await backup_helpers.get_leader_unit(
        ops_test, db_app_name=CONFIG_SERVER_APP_NAME
    )
    prev_backups = await backup_helpers.count_logical_backups(leader_unit)
    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME], status="active", idle_period=20
    ),
    action = await leader_unit.run_action(action_name="create-backup")
    first_backup = await action.wait()
    assert first_backup.status == "completed", "First backup not started."

    # verify that backup was made on the bucket
    for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(5), reraise=True):
        with attempt:
            backups = await backup_helpers.count_logical_backups(leader_unit)
            assert backups == prev_backups + 1, "Backup not created."

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME], status="active", idle_period=20
    ),

    # add writes to be cleared after restoring the backup.
    await add_and_verify_unwanted_writes(ops_test, cluster_writes)

    # Destroy the old cluster and create a new cluster with the same exact topology and password
    await destroy_cluster(
        ops_test, applications=[CONFIG_SERVER_APP_NAME, SHARD_ONE_APP_NAME, SHARD_TWO_APP_NAME]
    )
    await deploy_cluster_backup_test(ops_test, deploy_s3_integrator=False, new_names=True)
    await setup_cluster_and_s3(ops_test, new_names=True)
    config_leader_id = await get_leader_id(ops_test, app_name=CONFIG_SERVER_APP_NAME_NEW)
    await set_password(
        ops_test, unit_id=config_leader_id, username="operator", password=OPERATOR_PASSWORD
    )
    await ops_test.model.wait_for_idle(
        apps=CLUSTER_APPS_NEW, status="active", idle_period=20, timeout=TIMEOUT
    )

    # find most recent backup id and restore
    leader_unit = await backup_helpers.get_leader_unit(
        ops_test, db_app_name=CONFIG_SERVER_APP_NAME_NEW
    )
    list_result = await backup_helpers.get_backup_list(
        ops_test, db_app_name=CONFIG_SERVER_APP_NAME_NEW
    )
    most_recent_backup = list_result.split("\n")[-1]
    backup_id = most_recent_backup.split()[0]

    action = await leader_unit.run_action(action_name="restore", **{"backup-id": backup_id})
    attempted_restore = await action.wait()
    assert (
        attempted_restore.status == "failed"
    ), "config-server ran restore without necessary remapping."

    remap_pattern = f"{CONFIG_SERVER_APP_NAME_NEW}={CONFIG_SERVER_APP_NAME},{SHARD_ONE_APP_NAME_NEW}={SHARD_ONE_APP_NAME},{SHARD_TWO_APP_NAME_NEW}={SHARD_TWO_APP_NAME}"
    action = await leader_unit.run_action(
        action_name="restore", **{"backup-id": backup_id, "remap-pattern": remap_pattern}
    )
    restore = await action.wait()
    assert restore.results["restore-status"] == "restore started", "restore not successful"

    await ops_test.model.wait_for_idle(
        apps=[CONFIG_SERVER_APP_NAME_NEW], status="active", idle_period=20
    ),

    await verify_writes_restored(ops_test, cluster_writes, new_names=True)


async def deploy_cluster_backup_test(
    ops_test: OpsTest, deploy_s3_integrator=True, new_names=False
) -> None:
    """Deploy a cluster for the backup test."""
    my_charm = await ops_test.build_charm(".")

    config_server_name = CONFIG_SERVER_APP_NAME if not new_names else CONFIG_SERVER_APP_NAME_NEW
    shard_one_name = SHARD_ONE_APP_NAME if not new_names else SHARD_ONE_APP_NAME_NEW
    shard_two_name = SHARD_TWO_APP_NAME if not new_names else SHARD_TWO_APP_NAME_NEW
    await ops_test.model.deploy(
        my_charm,
        num_units=2,
        config={"role": "config-server"},
        application_name=config_server_name,
    )
    await ops_test.model.deploy(
        my_charm, num_units=2, config={"role": "shard"}, application_name=shard_one_name
    )
    await ops_test.model.deploy(
        my_charm, num_units=1, config={"role": "shard"}, application_name=shard_two_name
    )

    # deploy the s3 integrator charm
    if deploy_s3_integrator:
        await ops_test.model.deploy(S3_APP_NAME, channel="edge")

    await ops_test.model.wait_for_idle(
        apps=[S3_APP_NAME, config_server_name, shard_one_name, shard_two_name],
        idle_period=20,
        raise_on_blocked=False,
        timeout=DEPLOYMENT_TIMEOUT,
        raise_on_error=False,
    )


async def setup_cluster_and_s3(ops_test: OpsTest, new_names=False) -> None:
    """Deploy a cluster for the backup test."""
    config_server_name = CONFIG_SERVER_APP_NAME if not new_names else CONFIG_SERVER_APP_NAME_NEW
    shard_one_name = SHARD_ONE_APP_NAME if not new_names else SHARD_ONE_APP_NAME_NEW
    shard_two_name = SHARD_TWO_APP_NAME if not new_names else SHARD_TWO_APP_NAME_NEW

    # provide config-server to entire cluster and s3-integrator to config-server - integrations
    # made in succession to test race conditions.
    await ops_test.model.integrate(
        f"{S3_APP_NAME}:{S3_REL_NAME}",
        f"{config_server_name}:{S3_REL_NAME}",
    )
    await ops_test.model.integrate(
        f"{shard_one_name}:{SHARD_REL_NAME}",
        f"{config_server_name}:{CONFIG_SERVER_REL_NAME}",
    )
    await ops_test.model.integrate(
        f"{shard_two_name}:{SHARD_REL_NAME}",
        f"{config_server_name}:{CONFIG_SERVER_REL_NAME}",
    )

    await ops_test.model.wait_for_idle(
        apps=[
            config_server_name,
            shard_one_name,
            shard_two_name,
        ],
        idle_period=20,
        status="active",
        timeout=TIMEOUT,
    )


async def add_and_verify_unwanted_writes(ops_test, old_cluster_writes: Dict) -> None:
    """Add writes to all shards that will be cleared after restoring backup.

    Note: this test also verifies every shard has unwanted writes.
    """
    await writes_helpers.insert_unwanted_data(ops_test)

    # new writes added to cluster in `insert_unwanted_data` get sent to shard-one - add more
    # writes to shard-two
    mongos_client = await generate_mongodb_client(
        ops_test, app_name=CONFIG_SERVER_APP_NAME, mongos=True
    )
    write_data_to_mongodb(
        mongos_client,
        db_name=SHARD_TWO_DB_NAME,
        coll_name=SHARD_TWO_COLL_NAME,
        content={"horse-breed": "pegasus", "real": True},
    )
    new_total_writes = await writes_helpers.get_cluster_writes_count(
        ops_test,
        shard_app_names=SHARD_APPS,
        db_names=[SHARD_ONE_DB_NAME, SHARD_TWO_DB_NAME],
        config_server_name=CONFIG_SERVER_APP_NAME,
    )

    assert (
        new_total_writes["total_writes"] > old_cluster_writes["total_writes"]
    ), "No writes to be cleared after restoring."
    assert (
        new_total_writes[SHARD_ONE_APP_NAME] > old_cluster_writes[SHARD_ONE_APP_NAME]
    ), "No writes to be cleared on shard-one after restoring."
    assert (
        new_total_writes[SHARD_TWO_APP_NAME] > old_cluster_writes[SHARD_TWO_APP_NAME]
    ), "No writes to be cleared on shard-two after restoring."


async def verify_writes_restored(
    ops_test, exppected_cluster_writes: Dict, new_names=False
) -> None:
    """Verify that writes were correctly restored."""
    config_server_name = CONFIG_SERVER_APP_NAME if not new_names else CONFIG_SERVER_APP_NAME_NEW
    shard_one_name = SHARD_ONE_APP_NAME if not new_names else SHARD_ONE_APP_NAME_NEW
    shard_two_name = SHARD_TWO_APP_NAME if not new_names else SHARD_TWO_APP_NAME_NEW
    shard_apps = [shard_one_name, shard_two_name]

    # verify all writes are present
    for attempt in Retrying(stop=stop_after_delay(4), wait=wait_fixed(20), reraise=True):
        with attempt:
            restored_total_writes = await writes_helpers.get_cluster_writes_count(
                ops_test,
                shard_app_names=shard_apps,
                db_names=[SHARD_ONE_DB_NAME, SHARD_TWO_DB_NAME],
                config_server_name=config_server_name,
            )
            assert (
                restored_total_writes["total_writes"] == exppected_cluster_writes["total_writes"]
            ), "writes not correctly restored to whole cluster"
            assert (
                restored_total_writes[shard_one_name]
                == exppected_cluster_writes[SHARD_ONE_APP_NAME]
            ), f"writes not correctly restored to {shard_one_name}"
            assert (
                restored_total_writes[shard_two_name]
                == exppected_cluster_writes[SHARD_TWO_APP_NAME]
            ), f"writes not correctly restored to {shard_two_name}"
