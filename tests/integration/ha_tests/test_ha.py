#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.


import asyncio
import time

import pytest
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

from . import helpers

ANOTHER_DATABASE_APP_NAME = "another-database-a"
MEDIAN_REELECTION_TIME = 12
RESTART_DELAY = 60 * 5
ORIGINAL_RESTART_DELAY = 5


@pytest.fixture()
async def continuous_writes(ops_test: OpsTest):
    """Starts continuous write operations to MongoDB for test and clears writes at end of test."""
    await helpers.start_continous_writes(ops_test, 1)
    yield
    await helpers.clear_db_writes(ops_test)


@pytest.fixture()
async def reset_restart_delay(ops_test: OpsTest):
    """Resets service file delay on all units."""
    yield
    app = await helpers.app_name(ops_test)
    for unit in ops_test.model.applications[app].units:
        await helpers.update_restart_delay(ops_test, unit, ORIGINAL_RESTART_DELAY)


@pytest.fixture()
async def change_logging(ops_test: OpsTest):
    """Enables appending logging for a test and resets the logging at the end of the test."""
    app = await helpers.app_name(ops_test)
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
    primary = await helpers.replica_set_primary(ip_addresses, ops_test)

    for unit in ops_test.model.applications[app].units:
        # tests which use this fixture restart the primary. Therefore the primary should not be
        # restarted as to leave the restart testing to the test itself.
        if unit.name == primary.name:
            continue

        # must restart unit to ensure that changes to logging are made
        await helpers.stop_mongod(ops_test, unit)
        await helpers.update_service_logging(ops_test, unit, logging=True)
        await helpers.start_mongod(ops_test, unit)

        # sleep long enough for the mongod to start up correctly
        time.sleep(15)
    yield

    app = await helpers.app_name(ops_test)
    for unit in ops_test.model.applications[app].units:
        # must restart unit to ensure that changes to logging are made
        await helpers.stop_mongod(ops_test, unit)
        await helpers.update_service_logging(ops_test, unit, logging=False)
        await helpers.start_mongod(ops_test, unit)

        # sleep long enough for the mongod to start up correctly
        time.sleep(15)

        # remove the log file as to not clog up space on the replicas.
        rm_cmd = f"run --unit {unit.name} rm {helpers.MONGODB_LOG_PATH}"
        await ops_test.juju(*rm_cmd.split())


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy one unit of MongoDB."""
    # it is possible for users to provide their own cluster for HA testing. Hence check if there
    # is a pre-existing cluster.
    if await helpers.app_name(ops_test):
        return

    my_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(my_charm, num_units=3)
    await ops_test.model.wait_for_idle()


async def test_storage_re_use(ops_test, continuous_writes):
    """Verifies that database units with attached storage correctly repurpose storage.

    It is not enough to verify that Juju attaches the storage. Hence test checks that the mongod
    properly uses the storage that was provided. (ie. doesn't just re-sync everything from
    primary, but instead computes a diff between current storage and primary storage.)
    """
    app = await helpers.app_name(ops_test)
    if helpers.storage_type(ops_test, app) == "rootfs":
        pytest.skip(
            "re-use of storage can only be used on deployments with persistent storage not on rootfs deployments"
        )

    # removing the only replica can be disastrous
    if len(ops_test.model.applications[app].units) < 2:
        await ops_test.model.applications[app].add_unit(count=1)
        await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=1000)

    # remove a unit and attach it's storage to a new unit
    unit = ops_test.model.applications[app].units[0]
    unit_storage_id = helpers.storage_id(ops_test, unit.name)
    expected_units = len(ops_test.model.applications[app].units) - 1
    removal_time = time.time()
    await ops_test.model.destroy_unit(unit.name)
    await ops_test.model.wait_for_idle(
        apps=[app], status="active", timeout=1000, wait_for_exact_units=expected_units
    )
    new_unit = await helpers.add_unit_with_storage(ops_test, app, unit_storage_id)

    assert await helpers.reused_storage(
        ops_test, new_unit.public_address, removal_time
    ), "attached storage not properly re-used by MongoDB."

    # verify that the no writes were skipped
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes


@pytest.mark.abort_on_fail
async def test_add_units(ops_test: OpsTest, continuous_writes) -> None:
    """Tests juju add-unit functionality.

    Verifies that when a new unit is added to the MongoDB application that it is added to the
    MongoDB replica set configuration.
    """
    # add units and wait for idle
    app = await helpers.app_name(ops_test)
    expected_units = len(ops_test.model.applications[app].units) + 2
    await ops_test.model.applications[app].add_unit(count=2)
    await ops_test.model.wait_for_idle(
        apps=[app], status="active", timeout=1000, wait_for_exact_units=expected_units
    )

    # grab unit ips
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]

    # connect to replica set uri and get replica set members
    member_ips = await helpers.fetch_replica_set_members(ip_addresses, ops_test)

    # verify that the replica set members have the correct units
    assert set(member_ips) == set(ip_addresses)

    # verify that the no writes were skipped
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes


@pytest.mark.abort_on_fail
async def test_scale_down_capablities(ops_test: OpsTest, continuous_writes) -> None:
    """Tests clusters behavior when scaling down a minority and removing a primary replica.

    - NOTE: on a provided cluster this calculates the largest set of minority members and removes
    them, the primary is guaranteed to be one of those minority members.

    This test verifies that the behavior of:
    1.  when a leader is deleted that the new leader, on calling leader_elected will reconfigure
    the replicaset.
    2. primary stepping down leads to a replica set with a new primary.
    3. removing a minority of units (2 out of 5) is feasiable.
    4. race conditions due to removing multiple units is handled.
    5. deleting a non-leader unit is properly handled.
    """
    deleted_unit_ips = []
    app = await helpers.app_name(ops_test)
    units_to_remove = []
    minority_count = int(len(ops_test.model.applications[app].units) / 2)

    # find leader unit
    leader_unit = await helpers.find_unit(ops_test, leader=True)
    minority_count -= 1

    # verify that we have a leader
    assert leader_unit is not None, "No unit is leader"
    deleted_unit_ips.append(leader_unit.public_address)
    units_to_remove.append(leader_unit.name)

    # find non-leader units to remove such that the largest minority possible is removed.
    avail_units = []
    for unit in ops_test.model.applications[app].units:
        if not unit.name == leader_unit.name:
            avail_units.append(unit)

    for _ in range(minority_count):
        unit_to_remove = avail_units.pop()
        deleted_unit_ips.append(unit_to_remove.public_address)
        units_to_remove.append(unit_to_remove.name)

    # destroy units simultaneously
    expected_units = len(ops_test.model.applications[app].units) - len(units_to_remove)
    await ops_test.model.destroy_units(*units_to_remove)

    # wait for app to be active after removal of units
    await ops_test.model.wait_for_idle(
        apps=[app], status="active", timeout=1000, wait_for_exact_units=expected_units
    )

    # grab unit ips
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]

    # check that the replica set with the remaining units has a primary
    try:
        primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    except RetryError:
        primary = None

    # verify that the primary is not None
    assert primary is not None, "replica set has no primary"

    # check that the primary is one of the remaining units
    assert (
        primary.public_address in ip_addresses
    ), "replica set primary is not one of the available units"

    # verify that the configuration of mongodb no longer has the deleted ip
    member_ips = await helpers.fetch_replica_set_members(ip_addresses, ops_test)

    assert set(member_ips) == set(ip_addresses), "mongod config contains deleted units"

    # verify that the no writes were skipped
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes


async def test_replication_across_members(ops_test: OpsTest, continuous_writes) -> None:
    """Check consistency, ie write to primary, read data from secondaries."""
    # first find primary, write to primary, then read from each unit
    await helpers.insert_focal_to_cluster(ops_test)
    app = await helpers.app_name(ops_test)
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
    primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    password = await helpers.get_password(ops_test, app)

    secondaries = set(ip_addresses) - set([primary.public_address])
    for secondary in secondaries:
        client = MongoClient(helpers.unit_uri(secondary, password, app), directConnection=True)

        db = client["new-db"]
        test_collection = db["test_ubuntu_collection"]
        query = test_collection.find({}, {"release_name": 1})
        assert query[0]["release_name"] == "Focal Fossa"

        client.close()

    # verify that the no writes were skipped
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes


async def test_unique_cluster_dbs(ops_test: OpsTest, continuous_writes) -> None:
    """Verify unique clusters do not share DBs."""
    # first find primary, write to primary,
    await helpers.insert_focal_to_cluster(ops_test)

    # deploy new cluster
    my_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(my_charm, num_units=1, application_name=ANOTHER_DATABASE_APP_NAME)
    await ops_test.model.wait_for_idle(apps=[ANOTHER_DATABASE_APP_NAME], status="active")

    # write data to new cluster
    ip_addresses = [
        unit.public_address
        for unit in ops_test.model.applications[ANOTHER_DATABASE_APP_NAME].units
    ]
    password = await helpers.get_password(ops_test, app=ANOTHER_DATABASE_APP_NAME)
    client = helpers.replica_set_client(ip_addresses, password, app=ANOTHER_DATABASE_APP_NAME)
    db = client["new-db"]
    test_collection = db["test_ubuntu_collection"]
    test_collection.insert_one({"release_name": "Jammy Jelly", "version": 22.04, "LTS": False})
    client.close()

    cluster_1_entries = await helpers.retrieve_entries(
        ops_test,
        app=ANOTHER_DATABASE_APP_NAME,
        db_name="new-db",
        collection_name="test_ubuntu_collection",
        query_field="release_name",
    )

    cluster_2_entries = await helpers.retrieve_entries(
        ops_test,
        app=helpers.APP_NAME,
        db_name="new-db",
        collection_name="test_ubuntu_collection",
        query_field="release_name",
    )

    common_entries = cluster_2_entries.intersection(cluster_1_entries)
    assert len(common_entries) == 0, "Writes from one cluster are replicated to another cluster."

    # verify that the no writes were skipped
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes


async def test_replication_member_scaling(ops_test: OpsTest, continuous_writes) -> None:
    """Verify newly added and newly removed members properly replica data.

    Verify newly members have replicated data and newly removed members are gone without data.
    """
    # first find primary, write to primary,
    await helpers.insert_focal_to_cluster(ops_test)

    app = await helpers.app_name(ops_test)
    original_ip_addresses = [
        unit.public_address for unit in ops_test.model.applications[app].units
    ]
    expected_units = len(ops_test.model.applications[app].units) + 1
    await ops_test.model.applications[app].add_unit(count=1)
    await ops_test.model.wait_for_idle(
        apps=[app], status="active", timeout=1000, wait_for_exact_units=expected_units
    )

    new_ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
    new_member_ip = list(set(new_ip_addresses) - set(original_ip_addresses))[0]
    password = await helpers.get_password(ops_test, app)
    client = MongoClient(helpers.unit_uri(new_member_ip, password, app), directConnection=True)

    # check for replicated data while retrying to give time for replica to copy over data.
    try:
        for attempt in Retrying(stop=stop_after_delay(2 * 60), wait=wait_fixed(3)):
            with attempt:
                db = client["new-db"]
                test_collection = db["test_ubuntu_collection"]
                query = test_collection.find({}, {"release_name": 1})
                assert query[0]["release_name"] == "Focal Fossa"

    except RetryError:
        assert False, "Newly added unit doesn't replicate data."

    client.close()

    # verify that the no writes were skipped
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes


async def test_kill_db_process(ops_test, continuous_writes):
    # locate primary unit
    app = await helpers.app_name(ops_test)
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
    primary = await helpers.replica_set_primary(ip_addresses, ops_test)

    await helpers.kill_unit_process(ops_test, primary.name, kill_code="SIGKILL")

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    writes = await helpers.count_writes(ops_test)
    time.sleep(5)
    more_writes = await helpers.count_writes(ops_test)
    assert more_writes > writes, "writes not continuing to DB"

    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # verify that db service got restarted and is ready
    assert await helpers.mongod_ready(ops_test, primary.public_address)

    # verify that a new primary gets elected (ie old primary is secondary)
    new_primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    assert new_primary.name != primary.name

    # verify that no writes to the db were missed
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes, "writes to the db were missed."

    # verify that old primary is up to date.
    assert await helpers.secondary_up_to_date(
        ops_test, primary.public_address, total_expected_writes["number"]
    ), "secondary not up to date with the cluster after restarting."


async def test_freeze_db_process(ops_test, continuous_writes):
    # locate primary unit
    app = await helpers.app_name(ops_test)
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
    primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    await helpers.kill_unit_process(ops_test, primary.name, kill_code="SIGSTOP")

    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # verify that a new primary gets elected
    new_primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    assert new_primary.name != primary.name

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    writes = await helpers.count_writes(ops_test)
    time.sleep(5)
    more_writes = await helpers.count_writes(ops_test)

    # un-freeze the old primary
    await helpers.kill_unit_process(ops_test, primary.name, kill_code="SIGCONT")

    # check this after un-freezing the old primary so that if this check fails we still "turned
    # back on" the mongod process
    assert more_writes > writes, "writes not continuing to DB"

    # verify that db service got restarted and is ready
    assert await helpers.mongod_ready(ops_test, primary.public_address)

    # verify all units are running under the same replset
    member_ips = await helpers.fetch_replica_set_members(ip_addresses, ops_test)
    assert set(member_ips) == set(ip_addresses), "all members not running under the same replset"

    # verify there is only one primary after un-freezing old primary
    assert (
        await helpers.count_primaries(ops_test) == 1
    ), "there are more than one primary in the replica set."

    # verify that the old primary does not "reclaim" primary status after un-freezing old primary
    new_primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    assert new_primary.name != primary.name, "un-frozen primary should be secondary."

    # verify that no writes were missed.
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert actual_writes == total_expected_writes["number"], "db writes missing."

    # verify that old primary is up to date.
    assert await helpers.secondary_up_to_date(
        ops_test, primary.public_address, actual_writes
    ), "secondary not up to date with the cluster after restarting."


async def test_restart_db_process(ops_test, continuous_writes, change_logging):
    # locate primary unit
    app = await helpers.app_name(ops_test)
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
    old_primary = await helpers.replica_set_primary(ip_addresses, ops_test)

    # send SIGTERM, we expect `systemd` to restart the process
    sig_term_time = time.time()
    await helpers.kill_unit_process(ops_test, old_primary.name, kill_code="SIGTERM")

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    writes = await helpers.count_writes(ops_test)
    time.sleep(5)
    more_writes = await helpers.count_writes(ops_test)
    assert more_writes > writes, "writes not continuing to DB"

    # verify that db service got restarted and is ready
    assert await helpers.mongod_ready(ops_test, old_primary.public_address)

    # verify that a new primary gets elected (ie old primary is secondary)
    new_primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    assert new_primary.name != old_primary.name

    # verify that a stepdown was performed on restart. SIGTERM should send a graceful restart and
    # send a replica step down signal. Performed with a retry to give time for the logs to update.
    try:
        for attempt in Retrying(stop=stop_after_delay(30), wait=wait_fixed(3)):
            with attempt:
                assert await helpers.db_step_down(
                    ops_test, old_primary.name, sig_term_time
                ), "old primary departed without stepping down."
    except RetryError:
        False, "old primary departed without stepping down."

    # verify that no writes were missed
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes

    # verify that old primary is up to date.
    assert await helpers.secondary_up_to_date(
        ops_test, old_primary.public_address, total_expected_writes["number"]
    ), "secondary not up to date with the cluster after restarting."


async def test_full_cluster_crash(ops_test: OpsTest, continuous_writes, reset_restart_delay):
    app = await helpers.app_name(ops_test)

    # update all units to have a new RESTART_DELAY,  Modifying the Restart delay to 3 minutes
    # should ensure enough time for all replicas to be down at the same time.
    for unit in ops_test.model.applications[app].units:
        await helpers.update_restart_delay(ops_test, unit, RESTART_DELAY)

    # kill all units "simultaneously"
    await asyncio.gather(
        *[
            helpers.kill_unit_process(ops_test, unit.name, kill_code="SIGKILL")
            for unit in ops_test.model.applications[app].units
        ]
    )

    # This test serves to verify behavior when all replicas are down at the same time that when
    # they come back online they operate as expected. This check verifies that we meet the criterea
    # of all replicas being down at the same time.
    assert await helpers.all_db_processes_down(ops_test), "Not all units down at the same time."

    # sleep for twice the median election time and the restart delay
    time.sleep(MEDIAN_REELECTION_TIME * 2 + RESTART_DELAY)

    # verify all units are up and running
    for unit in ops_test.model.applications[app].units:
        assert await helpers.mongod_ready(
            ops_test, unit.public_address
        ), f"unit {unit.name} not restarted after cluster crash."

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    writes = await helpers.count_writes(ops_test)
    time.sleep(5)
    more_writes = await helpers.count_writes(ops_test)
    assert more_writes > writes, "writes not continuing to DB"

    # verify presence of primary, replica set member configuration, and number of primaries
    await helpers.verify_replica_set_configuration(ops_test)

    # verify that no writes to the db were missed
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)

    # verify that no writes were missed.
    assert actual_writes == total_expected_writes["number"], "db writes missing."


async def test_full_cluster_restart(ops_test: OpsTest, continuous_writes, reset_restart_delay):
    app = await helpers.app_name(ops_test)

    # update all units to have a new RESTART_DELAY,  Modifying the Restart delay to 3 minutes
    # should ensure enough time for all replicas to be down at the same time.
    for unit in ops_test.model.applications[app].units:
        await helpers.update_restart_delay(ops_test, unit, RESTART_DELAY)

    # kill all units "simultaneously"
    await asyncio.gather(
        *[
            helpers.kill_unit_process(ops_test, unit.name, kill_code="SIGTERM")
            for unit in ops_test.model.applications[app].units
        ]
    )

    # This test serves to verify behavior when all replicas are down at the same time that when
    # they come back online they operate as expected. This check verifies that we meet the criterea
    # of all replicas being down at the same time.
    assert await helpers.all_db_processes_down(ops_test), "Not all units down at the same time."

    # sleep for twice the median election time and the restart delay
    time.sleep(MEDIAN_REELECTION_TIME * 2 + RESTART_DELAY)

    # verify all units are up and running
    for unit in ops_test.model.applications[app].units:
        assert await helpers.mongod_ready(
            ops_test, unit.public_address
        ), f"unit {unit.name} not restarted after cluster crash."

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    writes = await helpers.count_writes(ops_test)
    time.sleep(5)
    more_writes = await helpers.count_writes(ops_test)
    assert more_writes > writes, "writes not continuing to DB"

    # verify presence of primary, replica set member configuration, and number of primaries
    await helpers.verify_replica_set_configuration(ops_test)

    # verify that no writes to the db were missed
    total_expected_writes = await helpers.stop_continous_writes(ops_test)
    actual_writes = await helpers.count_writes(ops_test)
    assert total_expected_writes["number"] == actual_writes, "writes to the db were missed."


async def test_network_cut(ops_test, continuous_writes):
    # locate primary unit
    app = await helpers.app_name(ops_test)
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
    primary = await helpers.replica_set_primary(ip_addresses, ops_test)
    all_units = ops_test.model.applications[app].units
    model_name = ops_test.model.info.name

    primary_hostname = await helpers.unit_hostname(ops_test, primary.name)
    primary_unit_ip = await helpers.get_unit_ip(ops_test, primary.name)

    # before cutting network verify that connection is possible
    assert await helpers.mongod_ready(
        ops_test,
        primary.public_address,
    ), f"Connection to host {primary.public_address} is not possible"

    helpers.cut_network_from_unit(primary_hostname)

    # verify machine is not reachable from peer units
    for unit in set(all_units) - {primary}:
        hostname = await helpers.unit_hostname(ops_test, unit.name)
        assert not helpers.is_machine_reachable_from(
            hostname, primary_hostname
        ), "unit is reachable from peer"

    # verify machine is not reachable from controller
    controller = await helpers.get_controller_machine(ops_test)
    assert not helpers.is_machine_reachable_from(
        controller, primary_hostname
    ), "unit is reachable from controller"

    # sleep for twice the median election time
    time.sleep(MEDIAN_REELECTION_TIME * 2)

    # verify new writes are continuing by counting the number of writes before and after a 5 second
    # wait
    writes = await helpers.count_writes(ops_test, down_unit=primary.name)
    time.sleep(5)
    more_writes = await helpers.count_writes(ops_test, down_unit=primary.name)
    assert more_writes > writes, "writes not continuing to DB"

    # verify that a new primary got elected
    new_primary = await helpers.replica_set_primary(ip_addresses, ops_test, down_unit=primary.name)
    assert new_primary.name != primary.name

    # verify that no writes to the db were missed
    total_expected_writes = await helpers.stop_continous_writes(ops_test, down_unit=primary.name)
    actual_writes = await helpers.count_writes(ops_test, down_unit=primary.name)
    assert total_expected_writes["number"] == actual_writes, "writes to the db were missed."

    # restore network connectivity to old primary
    helpers.restore_network_for_unit(primary_hostname)

    # wait until network is reestablished for the unit
    helpers.wait_network_restore(model_name, primary_hostname, primary_unit_ip)

    # self healing is performed with update status hook
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=1000)

    # verify we have connection to the old primary
    new_ip = helpers.instance_ip(model_name, primary_hostname)
    assert await helpers.mongod_ready(
        ops_test,
        new_ip,
    ), f"Connection to host {new_ip} is not possible"

    # verify presence of primary, replica set member configuration, and number of primaries
    await helpers.verify_replica_set_configuration(ops_test)

    # verify that old primary is up to date.
    assert await helpers.secondary_up_to_date(
        ops_test, new_ip, total_expected_writes["number"]
    ), "secondary not up to date with the cluster after restarting."
