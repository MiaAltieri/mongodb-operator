# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
from single_kernel_mongo.config.literals import SNAP
from single_kernel_mongo.lib.charms.operator_libs_linux.v2.snap import Snap, SnapState


@pytest.fixture(autouse=True)
def mock_snap_cache(mocker):
    mocker.patch(
        "single_kernel_mongo.lib.charms.operator_libs_linux.v2.snap.SnapCache.__getitem__",
        return_value=Snap(
            "charmed-mongodb",
            state=SnapState.Available,
            channel=SNAP.channel,
            revision=SNAP.revision,
            confinement="classic",
            apps=None,
        ),
    )


@pytest.fixture
def mock_fs_interactions(mocker):
    mocker.patch(
        "single_kernel_mongo.lib.charms.operator_libs_linux.v2.snap.Snap.present",
        new_callable=mocker.PropertyMock,
        return_value=True,
    )
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.exec")
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.delete")
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.write")
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.start")
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.stop")
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.active", return_value=True)
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.update_env")
    mocker.patch("single_kernel_mongo.core.vm_workload.VMWorkload.copy_to_unit")
    mocker.patch("pathlib.Path.mkdir")
    mocker.patch("pathlib.Path.write_text")
    mocker.patch("builtins.open")
    mocker.patch(
        "single_kernel_mongo.managers.config.MongoDBExporterConfigManager.configure_and_restart"
    )
    mocker.patch("single_kernel_mongo.managers.config.BackupConfigManager.configure_and_restart")
