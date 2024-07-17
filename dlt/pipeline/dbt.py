import os
import contextlib
from dlt.common.exceptions import VenvNotFound
from dlt.common.runners import Venv
from dlt.common.schema import Schema
from dlt.common.typing import ConfigValue, TSecretValue
from dlt.common.schema.utils import normalize_schema_name

from dlt.helpers.dbt import (
    create_venv as _create_venv,
    package_runner as _package_runner,
    DBTPackageRunner,
    DEFAULT_DBT_VERSION as _DEFAULT_DBT_VERSION,
    restore_venv as _restore_venv,
)
from dlt.pipeline.pipeline import Pipeline


def get_venv(
    pipeline: Pipeline, venv_path: str = "dbt", dbt_version: str = _DEFAULT_DBT_VERSION
) -> Venv:
    """Creates or restores a virtual environment in which the `dbt` packages are executed.

    The recommended way to execute dbt package is to use a separate virtual environment where only the dbt-core
    and required destination dependencies are installed. This avoid dependency clashes with the user-installed libraries.
    This method will create such environment at the location specified in `venv_path` and automatically install required dependencies
    as required by `pipeline`.

    Args:
        pipeline (Pipeline): A pipeline for which the required dbt dependencies are inferred
        venv_path (str, optional): A path where virtual environment is created or restored from.
                                   If relative path is provided, the environment will be created within pipeline's working directory. Defaults to "dbt".
        dbt_version (str, optional): Version of dbt to be used. Exact version (ie. "1.2.4") or pip requirements string (ie. ">=1.1<1.5" may be provided).

    Returns:
        Venv: A Virtual Environment with dbt dependencies installed
    """
    # keep venv inside pipeline if path is relative
    if not os.path.isabs(venv_path):
        pipeline._pipeline_storage.create_folder(venv_path, exists_ok=True)
        venv_dir = pipeline._pipeline_storage.make_full_path_safe(venv_path)
    else:
        venv_dir = venv_path
    # try to restore existing venv
    with contextlib.suppress(VenvNotFound):
        # TODO: check dlt version in venv and update it if local version updated
        return _restore_venv(venv_dir, [pipeline.destination.spec().destination_type], dbt_version)

    return _create_venv(venv_dir, [pipeline.destination.spec().destination_type], dbt_version)


def package(
    pipeline: Pipeline,
    package_location: str,
    package_repository_branch: str = ConfigValue,
    package_repository_ssh_key: TSecretValue = TSecretValue(""),  # noqa
    auto_full_refresh_when_out_of_sync: bool = ConfigValue,
    venv: Venv = None,
) -> DBTPackageRunner:
    """Creates a Python wrapper over `dbt` package present at specified location, that allows to control it (ie. run and test) from Python code.

    The created wrapper minimizes the required effort to run `dbt` packages on datasets created with `dlt`. It clones the package repo and keeps it up to data,
    shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.
    The wrapper creates a `dbt` profile from `dlt` pipeline configuration. Specifically:
    1. destination is used to infer correct dbt profile
    2. destinations credentials are passed to dbt via environment variables
    3. dataset_name is used to configure the dbt database schema

    Args:
        pipeline (Pipeline): A pipeline containing destination, credentials and dataset_name used to configure the dbt package.
        package_location (str): A git repository url to be cloned or a local path where dbt package is present
        package_repository_branch (str, optional): A branch name, tag name or commit-id to check out. Defaults to None.
        package_repository_ssh_key (TSecretValue, optional): SSH key to be used to clone private repositories. Defaults to TSecretValue("").
        auto_full_refresh_when_out_of_sync (bool, optional): If set to True (default), the wrapper will automatically fall back to full-refresh mode when schema is out of sync
                                                             See: https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change
        venv (Venv, optional): A virtual environment with required dbt dependencies. Defaults to None which will execute dbt package in current environment.

    Returns:
        DBTPackageRunner: A configured and authenticated Python `dbt` wrapper
    """
    schema = (
        pipeline.default_schema
        if pipeline.default_schema_name
        else Schema(normalize_schema_name(pipeline.dataset_name))
    )
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
        auto_full_refresh_when_out_of_sync,
    )
