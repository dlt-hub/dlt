import os
import contextlib
from dlt.common.exceptions import VenvNotFound
from dlt.common.runners.venv import Venv
from dlt.common.schema import Schema
from dlt.common.typing import TSecretValue

from dlt.helpers.dbt import create_venv as _create_venv, package_runner as _package_runner, DBTPackageRunner, DEFAULT_DLT_VERSION as _DEFAULT_DLT_VERSION
from dlt.pipeline.pipeline import Pipeline



def get_venv(pipeline: Pipeline, venv_path: str = "dbt", dbt_version: str = _DEFAULT_DLT_VERSION) -> Venv:
    # keep venv inside pipeline if path is relative
    if not os.path.isabs(venv_path):
        pipeline._pipeline_storage.create_folder(venv_path, exists_ok=True)
        venv_dir = pipeline._pipeline_storage.make_full_path(venv_path)
    else:
        venv_dir = venv_path
    # try to restore existing venv
    with contextlib.suppress(VenvNotFound):
        # TODO: check dlt version in venv and update it if local version updated
        return Venv.restore(venv_dir)

    return _create_venv(venv_dir, [pipeline.destination.spec().destination_name], dbt_version)


def package(
        pipeline: Pipeline,
        package_location: str,
        package_repository_branch: str = None,
        package_repository_ssh_key: TSecretValue = TSecretValue(""),  # noqa
        auto_full_refresh_when_out_of_sync: bool = None,
        venv: Venv = None
) -> DBTPackageRunner:
    schema = pipeline.default_schema if pipeline.default_schema_name else Schema(pipeline.dataset_name)
    job_client = pipeline._sql_job_client(schema)
    if not venv:
        venv = Venv.restore_current()
    return _package_runner(
        venv,
        job_client.config,
        pipeline.working_dir,
        package_location,
        package_repository_branch,
        package_repository_ssh_key,
        auto_full_refresh_when_out_of_sync
    )
