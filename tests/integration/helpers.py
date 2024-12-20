# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

import ops
import yaml
from dateutil.parser import parse
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest
from tenacity import (
    Retrying,
    retry,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
PORT = 27017
UNIT_IDS = [0, 1, 2]
SERIES = "jammy"
DEPLOYMENT_TIMEOUT = 2000
logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


class Status:
    """Model class for status."""

    def __init__(self, value: str, since: str, message: Optional[str] = None):
        self.value = value
        self.since = parse(since, ignoretz=True)
        self.message = message


class Unit:
    """Model class for a Unit, with properties widely used."""

    def __init__(
        self,
        id: int,
        name: str,
        ip: str,
        hostname: str,
        is_leader: bool,
        machine_id: int,
        workload_status: Status,
        agent_status: Status,
        app_status: Status,
    ):
        self.id = id
        self.name = name
        self.ip = ip
        self.hostname = hostname
        self.is_leader = is_leader
        self.machine_id = machine_id
        self.workload_status = workload_status
        self.agent_status = agent_status
        self.app_status = app_status

    def dump(self) -> Dict[str, Any]:
        """To json."""
        result = {}
        for key, val in vars(self).items():
            result[key] = vars(val) if isinstance(val, Status) else val
        return result


async def destroy_cluster(
    ops_test: OpsTest, applications: list[str], destroy_storage: bool = False
) -> None:
    """Destroy cluster in a forceful way."""
    for app in applications:
        await ops_test.model.applications[app].destroy(
            destroy_storage=destroy_storage, force=True, no_wait=False
        )

    # destroy does not wait for applications to be removed, perform this check manually
    for attempt in Retrying(stop=stop_after_attempt(100), wait=wait_fixed(10), reraise=True):
        with attempt:
            # pytest_operator has a bug where the number of applications does not get correctly
            # updated. Wrapping the call with `fast_forward` resolves this
            async with ops_test.fast_forward():
                finished = all((item not in ops_test.model.applications for item in applications))
            # This case we don't raise an error in the context manager which fails to restore the
            # `update-status-hook-interval` value to it's former state.
            assert finished, "old cluster not destroyed successfully"


def unit_uri(ip_address: str, password, app=APP_NAME) -> str:
    """Generates URI that is used by MongoDB to connect to a single replica.

    Args:
        ip_address: ip address of replica/unit
        password: password of database.
        app: name of application which has the cluster.
    """
    return f"mongodb://operator:" f"{password}@" f"{ip_address}:{PORT}/admin?replicaSet={app}"


async def get_password(ops_test: OpsTest, username="operator", app_name=None) -> str:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
        String with the password stored on the peer relation databag.
    """
    app_name = app_name or await get_app_name(ops_test)

    # can retrieve from any unit running unit so we pick the first
    unit_name = ops_test.model.applications[app_name].units[0].name
    unit_id = unit_name.split("/")[1]

    action = await ops_test.model.units.get(f"{app_name}/{unit_id}").run_action(
        "get-password", **{"username": username}
    )
    action = await action.wait()
    try:
        password = action.results["password"]
        return password
    except KeyError:
        logger.error("Failed to get password. Action %s. Results %s", action, action.results)
        return None


@retry(
    retry=retry_if_result(lambda x: x == 0),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
async def count_primaries(ops_test: OpsTest, password: str, app_name=None) -> int:
    """Counts the number of primaries in a replica set.

    Will retry counting when the number of primaries is 0 at most 5 times.
    """
    app_name = app_name or await get_app_name(ops_test)
    number_of_primaries = 0
    for unit_id in UNIT_IDS:
        # get unit
        unit = ops_test.model.applications[app_name].units[unit_id]

        # connect to mongod
        client = MongoClient(
            unit_uri(unit.public_address, password, app_name), directConnection=True
        )

        # check primary status
        if client.is_primary:
            number_of_primaries += 1

    return number_of_primaries


async def find_unit(ops_test: OpsTest, leader: bool, app_name=None) -> ops.model.Unit:
    """Helper function identifies the a unit, based on need for leader or non-leader."""
    app_name = app_name or await get_app_name(ops_test)
    ret_unit = None
    for unit in ops_test.model.applications[app_name].units:
        if await unit.is_leader_from_status() == leader:
            ret_unit = unit

    return ret_unit


async def get_leader_id(ops_test: OpsTest, app_name=None) -> int:
    """Returns the unit number of the juju leader unit."""
    app_name = app_name or await get_app_name(ops_test)
    for unit in ops_test.model.applications[app_name].units:
        if await unit.is_leader_from_status():
            return int(unit.name.split("/")[1])
    return -1


async def set_password(
    ops_test: OpsTest, unit_id: int, username: str = "operator", password: str = "secret"
) -> dict[str, any]:
    """Use the charm action to retrieve the password from provided unit.

    Returns:
    String with the password stored on the peer relation databag.
    """
    app_name = await get_app_name(ops_test)
    action = await ops_test.model.units.get(f"{app_name}/{unit_id}").run_action(
        "set-password", **{"username": username, "password": password}
    )
    action = await action.wait()
    return action.results


async def get_application_relation_data(
    ops_test: OpsTest,
    application_name: str,
    relation_name: str,
    key: str,
    relation_id: str = None,
    relation_alias: str = None,
) -> Optional[str]:
    """Get relation data for an application.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        relation_name: name of the relation to get connection data from
        key: key of data to be retrieved
        relation_id: id of the relation to get connection data from
        relation_alias: alias of the relation (like a connection name)
            to get connection data from
    Returns:
        the that that was requested or None
            if no data in the relation
    Raises:
        ValueError if it's not possible to get application unit data
            or if there is no data for the particular relation endpoint
            and/or alias.
    """
    unit_name = f"{application_name}/0"
    raw_data = (await ops_test.juju("show-unit", unit_name))[1]

    if not raw_data:
        raise ValueError(f"no unit info could be grabbed for {unit_name}")
    data = yaml.safe_load(raw_data)

    # Filter the data based on the relation name.
    relation_data = [v for v in data[unit_name]["relation-info"] if v["endpoint"] == relation_name]
    if relation_id:
        # Filter the data based on the relation id.
        relation_data = [v for v in relation_data if v["relation-id"] == relation_id]

    if relation_alias:
        # Filter the data based on the cluster/relation alias.
        relation_data = [
            v
            for v in relation_data
            if json.loads(v["application-data"]["data"])["alias"] == relation_alias
        ]

    if len(relation_data) == 0:
        raise ValueError(
            f"no relation data could be grabbed on relation with endpoint {relation_name} and alias {relation_alias}"
        )

    return relation_data[0]["application-data"].get(key)


async def get_secret_id(ops_test, app_or_unit: Optional[str] = None) -> str:
    """Retrieve secret ID for an app or unit."""
    complete_command = "list-secrets"

    if app_or_unit:
        prefix = "unit" if app_or_unit[-1].isdigit() else "application"
        formated_app_or_unit = f"{prefix}-{app_or_unit}"
        if prefix == "unit":
            formated_app_or_unit = formated_app_or_unit.replace("/", "-")
        complete_command += f" --owner {formated_app_or_unit}"

    _, stdout, _ = await ops_test.juju(*complete_command.split())
    output_lines_split = [line.split() for line in stdout.strip().split("\n")]
    if app_or_unit:
        return [line[0] for line in output_lines_split if app_or_unit in line][0]

    return output_lines_split[1][0]


async def get_secret_content(ops_test, secret_id) -> Dict[str, str]:
    """Retrieve contents of a Juju Secret."""
    secret_id = secret_id.split("/")[-1]
    complete_command = f"show-secret {secret_id} --reveal --format=json"
    _, stdout, _ = await ops_test.juju(*complete_command.split())
    data = json.loads(stdout)
    return data[secret_id]["content"]["Data"]


async def check_or_scale_app(ops_test: OpsTest, user_app_name: str, required_units: int) -> None:
    """A helper function that scales existing cluster if necessary."""
    # check if we need to scale
    current_units = len(ops_test.model.applications[user_app_name].units)

    if current_units == required_units:
        return
    elif current_units > required_units:
        for i in range(0, current_units):
            unit_to_remove = [ops_test.model.applications[user_app_name].units[i].name]
            await ops_test.model.destroy_units(*unit_to_remove)
            await ops_test.model.wait_for_idle()
    else:
        units_to_add = required_units - current_units
    await ops_test.model.applications[user_app_name].add_unit(count=units_to_add)
    await ops_test.model.wait_for_idle()


async def get_app_name(ops_test: OpsTest, test_deployments: List[str] = []) -> str:
    """Returns the name of the cluster running MongoDB.

    This is important since not all deployments of the MongoDB charm have the application name
    "mongodb".

    Note: if multiple clusters are running MongoDB this will return the one first found.
    """
    status = await ops_test.model.get_status()
    for app in ops_test.model.applications:
        # note that format of the charm field is not exactly "mongodb" but instead takes the form
        # of `local:focal/mongodb-6`
        if "mongodb" in status["applications"][app]["charm"]:
            logger.debug("Found mongodb app named '%s'", app)

            if app in test_deployments:
                logger.debug("mongodb app named '%s', was deployed by the test, not by user", app)
                continue

            return app

    return None


async def unit_hostname(ops_test: OpsTest, unit_name: str) -> str:
    """Get hostname for a unit.

    Args:
        ops_test: The ops test object passed into every test case
        unit_name: The name of the unit to be tested

    Returns:
        The machine/container hostname
    """
    _, raw_hostname, _ = await ops_test.juju("ssh", unit_name, "hostname")
    return raw_hostname.strip()


def instance_ip(model: str, instance: str) -> str:
    """Translate juju instance name to IP.

    Args:
        model: The name of the model
        instance: The name of the instance

    Returns:
        The (str) IP address of the instance
    """
    output = subprocess.check_output(f"juju machines --model {model}".split())

    for line in output.decode("utf8").splitlines():
        if instance in line:
            return line.split()[2]


async def get_unit_ip(ops_test: OpsTest, unit_name: str) -> str:
    """Wrapper for getting unit ip.

    Juju incorrectly reports the IP addresses after the network is restored this is reported as a
    bug here: https://github.com/juju/python-libjuju/issues/738 . Once this bug is resolved use of
    `get_unit_ip` should be replaced with `.public_address`

    Args:
        ops_test: The ops test object passed into every test case
        unit_name: The name of the unit to be tested

    Returns:
        The (str) ip of the unit
    """
    return instance_ip(ops_test.model.info.name, await unit_hostname(ops_test, unit_name))


def audit_log_line_sanity_check(entry) -> bool:
    fields = ["atype", "ts", "local", "remote", "users", "roles", "param", "result"]
    for field in fields:
        if entry.get(field) is None:
            logger.error("Field '%s' not found in audit log entry \"%s\"", field, entry)
            return False
    return True


async def get_unit_hostname(ops_test: OpsTest, unit_id: int, app: str) -> str:
    """Get the hostname of a specific unit."""
    _, hostname, _ = await ops_test.juju("ssh", f"{app}/{unit_id}", "hostname")
    return hostname.strip()


async def get_raw_application(ops_test: OpsTest, app: str) -> Dict[str, Any]:
    """Get raw application details."""
    ret_code, stdout, stderr = await ops_test.juju(
        *f"status --model {ops_test.model.info.name} {app} --format=json".split()
    )
    if ret_code != 0:
        logger.error(f"Invalid return [{ret_code=}]: {stderr=}")
        raise Exception(f"[{ret_code=}] {stderr=}")
    return json.loads(stdout)["applications"][app]


async def get_application_units(ops_test: OpsTest, app: str) -> List[Unit]:
    """Get fully detailed units of an application."""
    # Juju incorrectly reports the IP addresses after the network is restored this is reported as a
    # bug here: https://github.com/juju/python-libjuju/issues/738. Once this bug is resolved use of
    # `get_unit_ip` should be replaced with `.public_address`
    raw_app = await get_raw_application(ops_test, app)
    units = []
    for u_name, unit in raw_app["units"].items():
        unit_id = int(u_name.split("/")[-1])

        if not unit.get("public-address"):
            # unit not ready yet...
            continue

        unit = Unit(
            id=unit_id,
            name=u_name.replace("/", "-"),
            ip=unit["public-address"],
            hostname=await get_unit_hostname(ops_test, unit_id, app),
            is_leader=unit.get("leader", False),
            machine_id=int(unit["machine"]),
            workload_status=Status(
                value=unit["workload-status"]["current"],
                since=unit["workload-status"]["since"],
                message=unit["workload-status"].get("message"),
            ),
            agent_status=Status(
                value=unit["juju-status"]["current"],
                since=unit["juju-status"]["since"],
            ),
            app_status=Status(
                value=raw_app["application-status"]["current"],
                since=raw_app["application-status"]["since"],
                message=raw_app["application-status"].get("message"),
            ),
        )

        units.append(unit)

    return units


async def check_all_units_blocked_with_status(
    ops_test: OpsTest, db_app_name: str, status: Optional[str]
) -> None:
    # this is necessary because ops_model.units does not update the unit statuses
    for unit in await get_application_units(ops_test, db_app_name):
        assert (
            unit.workload_status.value == "blocked"
        ), f"unit {unit.name} not in blocked state, in {unit.workload_status.value}"
        if status:
            assert (
                unit.workload_status.message == status
            ), f"unit {unit.name} not in blocked state, in {unit.workload_status.value}"


async def wait_for_mongodb_units_blocked(
    ops_test: OpsTest, db_app_name: str, status: Optional[str] = None, timeout=20
) -> None:
    """Waits for units of MongoDB to be in the blocked state.

    This is necessary because the MongoDB app can report a different status than the units.
    """
    hook_interval_key = "update-status-hook-interval"
    try:
        old_interval = (await ops_test.model.get_config())[hook_interval_key]
        await ops_test.model.set_config({hook_interval_key: "1m"})
        for attempt in Retrying(stop=stop_after_delay(timeout), wait=wait_fixed(1), reraise=True):
            with attempt:
                await check_all_units_blocked_with_status(ops_test, db_app_name, status)
    finally:
        await ops_test.model.set_config({hook_interval_key: old_interval})


def is_relation_joined(ops_test: OpsTest, endpoint_one: str, endpoint_two: str) -> bool:
    """Check if a relation is joined.

    Args:
        ops_test: The ops test object passed into every test case
        endpoint_one: The first endpoint of the relation
        endpoint_two: The second endpoint of the relation
    """
    for rel in ops_test.model.relations:
        endpoints = [endpoint.name for endpoint in rel.endpoints]
        if endpoint_one in endpoints and endpoint_two in endpoints:
            return True
    return False
