# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import calendar
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from subprocess import PIPE, check_output
from typing import List, Optional

import ops
import yaml
from pymongo import MongoClient
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure
from pytest_operator.plugin import OpsTest
from tenacity import (
    RetryError,
    Retrying,
    retry,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)

from ..helpers import get_app_name, get_unit_ip, instance_ip

# TODO move these to a separate file for constants \ config
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
PORT = 27017
APP_NAME = METADATA["name"]
MONGO_COMMON_DIR = "/var/snap/charmed-mongodb/common"
DB_PROCESS = "/usr/bin/mongod"
MONGODB_LOG_PATH = f"{MONGO_COMMON_DIR}/var/log/mongodb/mongodb.log"
MONGOD_SERVICE_DEFAULT_PATH = "/etc/systemd/system/snap.charmed-mongodb.mongod.service"
TMP_SERVICE_PATH = "tests/integration/ha_tests/tmp.service"
LOGGING_OPTIONS = f"--logpath={MONGO_COMMON_DIR}/var/log/mongodb/mongodb.log --logappend"
EXPORTER_PROC = "/usr/bin/mongodb_exporter"
GREP_PROC = "grep"

logger = logging.getLogger(__name__)


class ProcessError(Exception):
    """Raised when a process fails."""


class ProcessRunningError(Exception):
    """Raised when a process is running when it is not expected to be."""


def replica_set_client(replica_ips: List[str], password: str, app_name: str) -> MongoClient:
    """Generates the replica set URI for multiple IP addresses.

    Args:
        replica_ips: list of ips hosting the replica set.
        password: password of database.
        app_name: name of application which hosts the cluster.
    """
    hosts = ["{}:{}".format(replica_ip, PORT) for replica_ip in replica_ips]
    hosts = ",".join(hosts)

    replica_set_uri = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app_name}"
    return MongoClient(replica_set_uri)


async def fetch_replica_set_members(replica_ips: List[str], ops_test: OpsTest, app_name: str):
    """Fetches the IPs listed as replica set members in the MongoDB replica set configuration.

    Args:
        replica_ips: list of ips hosting the replica set.
        ops_test: reference to deployment.
        app_name: name of application which has the cluster.
    """
    # connect to replica set uri
    app_name = app_name or await get_app_name(ops_test)
    password = await get_password(ops_test, app_name)
    client = replica_set_client(replica_ips, password, app_name)

    # get ips from MongoDB replica set configuration
    rs_config = client.admin.command("replSetGetConfig")
    member_ips = []
    for member in rs_config["config"]["members"]:
        # get member ip without ":PORT"
        member_ips.append(member["host"].split(":")[0])

    client.close()

    return member_ips


def unit_uri(ip_address: str, password, app_name=APP_NAME) -> str:
    """Generates URI that is used by MongoDB to connect to a single replica.

    Args:
        ip_address: ip address of replica/unit
        password: password of database.
        app_name: name of application which has the cluster.
    """
    return f"mongodb://operator:{password}@{ip_address}:{PORT}/admin?replicaSet={app_name}"


# TODO remove this duplicate with helpers.py
async def get_password(ops_test: OpsTest, app_name, down_unit=None) -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    # some tests disable the network for units, so find a unit that is available
    for unit in ops_test.model.applications[app_name].units:
        if not unit.name == down_unit:
            unit_id = unit.name.split("/")[1]
            break

    action = await ops_test.model.units.get(f"{app_name}/{unit_id}").run_action("get-password")
    action = await action.wait()
    return action.results["password"]


async def fetch_primary(
    replica_set_hosts: List[str], ops_test: OpsTest, down_unit=None, app_name=None
) -> str:
    """Returns IP address of current replica set primary."""
    # connect to MongoDB client
    app_name = app_name or await get_app_name(ops_test)

    password = await get_password(ops_test, app_name, down_unit)
    client = replica_set_client(replica_set_hosts, password, app_name)

    # grab the replica set status
    try:
        status = client.admin.command("replSetGetStatus")
    except (ConnectionFailure, ConfigurationError, OperationFailure):
        return None
    finally:
        client.close()
    primary = None
    # loop through all members in the replica set
    for member in status["members"]:
        # check replica's current state
        if member["stateStr"] == "PRIMARY":
            # get member ip without ":PORT"
            primary = member["name"].split(":")[0]

    return primary


# TODO remove duplication with common helpers
async def count_primaries(ops_test: OpsTest, password: str = None, app_name: str = None) -> int:
    """Returns the number of primaries in a replica set."""
    # connect to MongoDB client
    app_name = app_name or await get_app_name(ops_test)
    password = password or await get_password(ops_test, app_name)
    replica_set_hosts = [
        unit.public_address for unit in ops_test.model.applications[app_name].units
    ]
    client = replica_set_client(replica_set_hosts, password, app_name)

    # grab the replica set status
    try:
        status = client.admin.command("replSetGetStatus")
    except (ConnectionFailure, ConfigurationError, OperationFailure):
        return None
    finally:
        client.close()

    primaries = 0
    # loop through all members in the replica set
    for member in status["members"]:
        # check replica's current state
        if member["stateStr"] == "PRIMARY":
            primaries += 1

    return primaries


@retry(
    retry=retry_if_result(lambda x: x is None),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
async def replica_set_primary(
    replica_set_hosts: List[str],
    ops_test: OpsTest,
    down_unit=None,
    app_name=None,
) -> Optional[ops.model.Unit]:
    """Returns the primary of the replica set.

    Retrying 5 times to give the replica set time to elect a new primary, also checks against the
    valid_ips to verify that the primary is not outdated.
    """
    app_name = app_name or await get_app_name(ops_test)
    primary_ip = await fetch_primary(replica_set_hosts, ops_test, down_unit, app_name)
    # return None if primary is no longer in the replica set
    if primary_ip is not None and primary_ip not in replica_set_hosts:
        return None

    for unit in ops_test.model.applications[app_name].units:
        if unit.public_address == str(primary_ip):
            return unit


async def retrieve_entries(ops_test, app_name, db_name, collection_name, query_field):
    """Retries entries from a specified collection within a specified database."""
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app_name].units]
    password = await get_password(ops_test, app_name)
    client = replica_set_client(ip_addresses, password, app_name)

    db = client[db_name]
    test_collection = db[collection_name]

    # read all entries from original cluster
    cursor = test_collection.find({})
    cluster_entries = set()
    for document in cursor:
        cluster_entries.add(document[query_field])

    client.close()
    return cluster_entries


async def find_unit(ops_test: OpsTest, leader: bool, app_name=None) -> ops.model.Unit:
    """Helper function identifies the a unit, based on need for leader or non-leader."""
    ret_unit = None
    app_name = app_name or await get_app_name(ops_test)
    for unit in ops_test.model.applications[app_name].units:
        if await unit.is_leader_from_status() == leader:
            ret_unit = unit

    return ret_unit


async def clear_db_writes(ops_test: OpsTest) -> bool:
    """Stop the DB process and remove any writes to the test collection."""
    await stop_continous_writes(ops_test)

    # remove collection from database
    app_name = await get_app_name(ops_test)
    password = await get_password(ops_test, app_name)
    hosts = [unit.public_address for unit in ops_test.model.applications[app_name].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app_name}"

    client = MongoClient(connection_string)
    db = client["new-db"]

    # collection for continuous writes
    test_collection = db["test_collection"]
    test_collection.drop()

    # collection for replication tests
    test_collection = db["test_ubuntu_collection"]
    test_collection.drop()

    client.close()


async def start_continous_writes(ops_test: OpsTest, starting_number: int) -> None:
    """Starts continuous writes to MongoDB with available replicas.

    In the future this should be put in a dummy charm.
    """
    app_name = await get_app_name(ops_test)
    password = await get_password(ops_test, app_name)
    hosts = [unit.public_address for unit in ops_test.model.applications[app_name].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app_name}"

    # run continuous writes in the background.
    subprocess.Popen(
        [
            "python3",
            "tests/integration/ha_tests/continuous_writes.py",
            connection_string,
            str(starting_number),
        ]
    )


async def stop_continous_writes(
    ops_test: OpsTest, down_unit=None, app_name=None
) -> dict[str, any]:
    """Stops continuous writes to MongoDB and returns the last written value.

    In the future this should be put in a dummy charm.
    """
    # stop the process
    proc = subprocess.Popen(["pkill", "-9", "-f", "continuous_writes.py"])

    # wait for process to be killed
    proc.communicate()

    app_name = app_name or await get_app_name(ops_test)
    password = await get_password(ops_test, app_name, down_unit)
    hosts = [unit.public_address for unit in ops_test.model.applications[app_name].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app_name}"

    client = MongoClient(connection_string)
    db = client["new-db"]
    test_collection = db["test_collection"]

    # last written value should be the highest number in the database.
    last_written_value = test_collection.find_one(sort=[("number", -1)])
    client.close()
    return last_written_value


async def count_writes(ops_test: OpsTest, down_unit=None, app_name=None) -> int:
    """New versions of pymongo no longer support the count operation, instead find is used."""
    app_name = app_name or await get_app_name(ops_test)
    password = await get_password(ops_test, app_name, down_unit)
    hosts = [unit.public_address for unit in ops_test.model.applications[app_name].units]
    hosts = ",".join(hosts)
    connection_string = f"mongodb://operator:{password}@{hosts}/admin?replicaSet={app_name}"

    client = MongoClient(connection_string)
    db = client["new-db"]
    test_collection = db["test_collection"]
    count = test_collection.count_documents({})
    client.close()
    return count


async def secondary_up_to_date(ops_test: OpsTest, unit_ip, expected_writes, app_name=None) -> bool:
    """Checks if secondary is up to date with the cluster.

    Retries over the period of one minute to give secondary adequate time to copy over data.
    """
    app_name = app_name or await get_app_name(ops_test)
    password = await get_password(ops_test, app_name)
    connection_string = f"mongodb://operator:{password}@{unit_ip}:{PORT}/admin?"
    client = MongoClient(connection_string, directConnection=True)

    try:
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                db = client["new-db"]
                test_collection = db["test_collection"]
                secondary_writes = test_collection.count_documents({})
                assert secondary_writes == expected_writes
    except RetryError:
        return False
    finally:
        client.close()

    return True


def storage_type(ops_test, app):
    """Retrieves type of storage associated with an application.

    Note: this function exists as a temporary solution until this issue is resolved:
    https://github.com/juju/python-libjuju/issues/694
    """
    model_name = ops_test.model.info.name
    proc = subprocess.check_output(f"juju storage --model={model_name}".split())
    proc = proc.decode("utf-8")
    for line in proc.splitlines():
        if "Storage" in line:
            continue

        if len(line) == 0:
            continue

        if "detached" in line:
            continue

        unit_name = line.split()[0]
        app_name = unit_name.split("/")[0]
        if app_name == app:
            return line.split()[3]


def storage_id(ops_test, unit_name):
    """Retrieves  storage id associated with provided unit.

    Note: this function exists as a temporary solution until this issue is resolved:
    https://github.com/juju/python-libjuju/issues/694
    """
    model_name = ops_test.model.info.name
    proc = subprocess.check_output(f"juju storage --model={model_name}".split())
    proc = proc.decode("utf-8")
    for line in proc.splitlines():
        if "Storage" in line:
            continue

        if len(line) == 0:
            continue

        if "detached" in line:
            continue

        if line.split()[0] == unit_name:
            return line.split()[1]


async def reused_storage(ops_test: OpsTest, unit_name: str, removal_time: float) -> bool:
    """Returns True if storage provided to mongod has been reused.

    MongoDB startup message indicates storage reuse:
        If member transitions to STARTUP2 from STARTUP then it is syncing/getting data from
        primary.
        If member transitions to STARTUP2 from REMOVED then it is reusing the storage we
        provided.
    """
    cat_cmd = f"exec --unit {unit_name} -- cat {MONGODB_LOG_PATH}"
    return_code, output, _ = await ops_test.juju(*cat_cmd.split())

    if return_code != 0:
        raise ProcessError(
            f"Expected cat command {cat_cmd} to succeed instead it failed: {return_code}"
        )

    for line in output.split("\n"):
        if not len(line):
            continue

        item = json.loads(line)

        # "attr" is needed and stores the state information and changes of mongodb
        if "attr" not in item:
            continue

        # Compute reuse time
        re_use_time = convert_time(item["t"]["$date"])

        # Get newstate and oldstate if present
        newstate = item["attr"].get("newState", "")
        oldstate = item["attr"].get("oldState", "")
        if newstate == "STARTUP2" and oldstate == "REMOVED" and re_use_time > removal_time:
            return True

    return False


async def insert_focal_to_cluster(ops_test: OpsTest, app_name=None) -> None:
    """Inserts the Focal Fossa data into the MongoDB cluster via primary replica."""
    app_name = app_name or await get_app_name(ops_test)
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app_name].units]
    primary = (await replica_set_primary(ip_addresses, ops_test)).public_address
    password = await get_password(ops_test, app_name)
    client = MongoClient(unit_uri(primary, password, app_name), directConnection=True)
    db = client["new-db"]
    test_collection = db["test_ubuntu_collection"]
    test_collection.insert_one({"release_name": "Focal Fossa", "version": 20.04, "LTS": True})
    client.close()


async def kill_unit_process(ops_test: OpsTest, unit_name: str, kill_code: str, app_name=None):
    """Kills the DB process on the unit according to the provided kill code."""
    # killing the only replica can be disastrous
    app_name = app_name or await get_app_name(ops_test)
    if len(ops_test.model.applications[app_name].units) < 2:
        await ops_test.model.applications[app_name].add_unit(count=1)
        await ops_test.model.wait_for_idle(apps=[app_name], status="active", timeout=1000)
    kill_cmd = f"exec --unit {unit_name} -- pkill --signal {kill_code} -f {DB_PROCESS}"
    return_code, _, _ = await ops_test.juju(*kill_cmd.split())

    if return_code != 0:
        raise ProcessError(
            f"Expected kill command {kill_cmd} to succeed instead it failed: {return_code}"
        )


async def mongod_ready(ops_test, unit_ip, app_name=None) -> bool:
    """Verifies replica is running and available."""
    app_name = app_name or await get_app_name(ops_test)
    password = await get_password(ops_test, app_name)
    client = MongoClient(unit_uri(unit_ip, password, app_name), directConnection=True)
    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 5), wait=wait_fixed(3)):
            with attempt:
                # The ping command is cheap and does not require auth.
                client.admin.command("ping")
    except RetryError:
        return False
    finally:
        client.close()

    return True


async def db_step_down(ops_test: OpsTest, old_primary_unit: str, sigterm_time: int, app_name=None):
    # loop through all units that aren't the old primary
    app_name = app_name or await get_app_name(ops_test)
    for unit in ops_test.model.applications[app_name].units:
        # verify log file exists on this machine
        search_file = f"exec --unit {unit.name} ls {MONGODB_LOG_PATH}"
        return_code, _, _ = await ops_test.juju(*search_file.split())
        if return_code == 2:
            continue

        # these log files can get quite large. According to the Juju team the 'run' command
        # cannot be used for more than 16MB of data so it is best to use juju ssh or juju scp.
        log_file = check_output(
            f"JUJU_MODEL={ops_test.model_full_name} juju ssh {unit.name} 'sudo cat {MONGODB_LOG_PATH}'",
            stderr=PIPE,
            shell=True,
            universal_newlines=True,
        )

        for line in log_file.splitlines():
            if not len(line):
                continue

            item = json.loads(line)

            step_down_time = convert_time(item["t"]["$date"])
            if (
                "Starting an election due to step up request" in line
                and step_down_time >= sigterm_time
            ):
                return True

    return False


async def all_db_processes_down(ops_test: OpsTest, app_name=None) -> bool:
    """Verifies that all units of the charm do not have the DB process running."""
    app_name = app_name or await get_app_name(ops_test)

    try:
        for attempt in Retrying(stop=stop_after_attempt(60), wait=wait_fixed(3)):
            with attempt:
                for unit in ops_test.model.applications[app_name].units:
                    search_db_process = f"exec --unit {unit.name} pgrep -x mongod"
                    _, processes, _ = await ops_test.juju(*search_db_process.split())
                    # splitting processes by "\n" results in one or more empty lines, hence we
                    # need to process these lines accordingly.
                    processes = [proc for proc in processes.split("\n") if len(proc) > 0]
                    if len(processes) > 0:
                        raise ProcessRunningError
    except RetryError:
        return False

    return True


async def update_restart_delay(ops_test: OpsTest, unit, delay: int):
    """Updates the restart delay in the DB service file.

    When the DB service fails it will now wait for `delay` number of seconds.
    """
    # load the service file from the unit and update it with the new delay
    await unit.scp_from(source=MONGOD_SERVICE_DEFAULT_PATH, destination=TMP_SERVICE_PATH)
    with open(TMP_SERVICE_PATH, "r") as mongodb_service_file:
        mongodb_service = mongodb_service_file.readlines()

    for index, line in enumerate(mongodb_service):
        if "RestartSec" in line:
            mongodb_service[index] = f"RestartSec={delay}s\n"

    with open(TMP_SERVICE_PATH, "w") as service_file:
        service_file.writelines(mongodb_service)

    # upload the changed file back to the unit, we cannot scp this file directly to
    # MONGOD_SERVICE_DEFAULT_PATH since this directory has strict permissions, instead we scp it
    # elsewhere and then move it to MONGOD_SERVICE_DEFAULT_PATH.
    await unit.scp_to(source=TMP_SERVICE_PATH, destination="mongod.service")
    mv_cmd = (
        f"exec --unit {unit.name} mv /home/ubuntu/mongod.service {MONGOD_SERVICE_DEFAULT_PATH}"
    )
    return_code, _, _ = await ops_test.juju(*mv_cmd.split())
    if return_code != 0:
        raise ProcessError(f"Command: {mv_cmd} failed on unit: {unit.name}.")

    # remove tmp file from machine
    subprocess.call(["rm", TMP_SERVICE_PATH])

    # reload the daemon for systemd otherwise changes are not saved
    reload_cmd = f"exec --unit {unit.name} systemctl daemon-reload"
    return_code, _, _ = await ops_test.juju(*reload_cmd.split())
    if return_code != 0:
        raise ProcessError(f"Command: {reload_cmd} failed on unit: {unit.name}.")


async def stop_mongod(ops_test: OpsTest, unit) -> None:
    """Safely stops the mongod process."""
    stop_db_process = f"exec --unit {unit.name} snap stop charmed-mongodb.mongod"
    await ops_test.juju(*stop_db_process.split())


async def start_mongod(ops_test: OpsTest, unit) -> None:
    """Safely starts the mongod process."""
    start_db_process = f"exec --unit {unit.name} snap start charmed-mongodb.mongod"
    await ops_test.juju(*start_db_process.split())


@retry(stop=stop_after_attempt(8), wait=wait_fixed(15))
async def verify_replica_set_configuration(ops_test: OpsTest, app_name=None) -> None:
    """Verifies presence of primary, replica set members, and number of primaries."""
    app_name = app_name or await get_app_name(ops_test)
    # `get_unit_ip` is used instead of `.public_address` because of a bug in python-libjuju that
    # incorrectly reports the IP addresses after the network is restored this is reported as a
    # bug here: https://github.com/juju/python-libjuju/issues/738 . Once this bug is resolved use
    # of `get_unit_ip` should be replaced with `.public_address`
    ip_addresses = [
        await get_unit_ip(ops_test, unit.name)
        for unit in ops_test.model.applications[app_name].units
    ]

    # verify presence of primary
    new_primary = await replica_set_primary(ip_addresses, ops_test, app_name=app_name)
    assert new_primary.name, "primary not elected."

    # verify all units are running under the same replset
    member_ips = await fetch_replica_set_members(ip_addresses, ops_test, app_name=app_name)
    assert set(member_ips) == set(ip_addresses), "all members not running under the same replset"

    # verify there is only one primary
    assert (
        await count_primaries(ops_test, app_name=app_name) == 1
    ), "there are more than one primary in the replica set."


def convert_time(time_as_str: str) -> int:
    """Converts a string time representation to an integer time representation, in UTC."""
    # parse time representation, provided in this format: 'YYYY-MM-DDTHH:MM:SS.MMM+00:00'
    d = datetime.strptime(time_as_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    return calendar.timegm(d.timetuple())


def cut_network_from_unit(machine_name: str) -> None:
    """Cut network from a lxc container.

    Args:
        machine_name: lxc container hostname
    """
    # apply a mask (device type `none`)
    cut_network_command = f"lxc config device add {machine_name} eth0 none"
    subprocess.check_call(cut_network_command.split())


def restore_network_for_unit(machine_name: str) -> None:
    """Restore network from a lxc container.

    Args:
        machine_name: lxc container hostname
    """
    # remove mask from eth0
    restore_network_command = f"lxc config device remove {machine_name} eth0"
    subprocess.check_call(restore_network_command.split())


async def get_controller_machine(ops_test: OpsTest) -> str:
    """Return controller machine hostname.

    Args:
        ops_test: The ops test framework instance
    Returns:
        Controller hostname (str)
    """
    _, raw_controller, _ = await ops_test.juju("show-controller")

    controller = yaml.safe_load(raw_controller.strip())

    return [
        machine.get("instance-id")
        for machine in controller[ops_test.controller_name]["controller-machines"].values()
    ][0]


def is_machine_reachable_from(origin_machine: str, target_machine: str) -> bool:
    """Test network reachability between hosts.

    Args:
        origin_machine: hostname of the machine to test connection from
        target_machine: hostname of the machine to test connection to
    """
    try:
        subprocess.check_call(f"lxc exec {origin_machine} -- ping -c 3 {target_machine}".split())
        return True
    except subprocess.CalledProcessError:
        return False


@retry(stop=stop_after_attempt(60), wait=wait_fixed(15))
def wait_network_restore(model_name: str, hostname: str, old_ip: str) -> None:
    """Wait until network is restored.

    Args:
        model_name: The name of the model
        hostname: The name of the instance
        old_ip: old registered IP address
    """
    if instance_ip(model_name, hostname) == old_ip:
        raise Exception("Network not restored, IP address has not changed yet.")


async def scale_and_verify(ops_test: OpsTest, count: int, remove_leader: bool = False):
    if count == 0:
        logger.warning("Skipping scale up/down by 0")
        return

    app_name = await get_app_name(ops_test)

    if count > 0:
        logger.info(f"Scaling up by {count} units")
        await ops_test.model.applications[app_name].add_units(count)
    else:
        logger.info(f"Scaling down by {abs(count)} units")
        # find leader unit
        leader_unit = await find_unit(ops_test, leader=True)
        units_to_remove = []
        for unit in ops_test.model.applications[app_name].units:
            if not remove_leader and unit.name == leader_unit.name:
                continue
            if len(units_to_remove) < abs(count):
                units_to_remove.append(unit.name)

        logger.info(f"Units to remove {units_to_remove}")
        await ops_test.model.applications[app_name].destroy_units(*units_to_remove)
    logger.info("Waiting for idle")
    await ops_test.model.wait_for_idle(
        apps=[app_name],
        status="active",
        timeout=1000,
    )
    logger.info("Validating replica set has primary")
    ip_addresses = [unit.public_address for unit in ops_test.model.applications[app_name].units]
    primary = await replica_set_primary(ip_addresses, ops_test, app_name=app_name)

    assert primary is not None, "Replica set has no primary"


async def verify_writes(ops_test: OpsTest, app_name=None):
    # verify that no writes to the db were missed
    app_name = app_name or await get_app_name(ops_test)
    total_expected_writes = await stop_continous_writes(ops_test)
    actual_writes = await count_writes(ops_test, app_name)
    assert total_expected_writes["number"] == actual_writes, "writes to the db were missed."
