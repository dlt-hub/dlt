import os
import contextlib
from typing import List
import pkg_resources
import semver

from dlt.common.runners.venv import Venv
from dlt.common.destination import DestinationClientDwhConfiguration
from dlt.common.configuration.specs import CredentialsWithDefault
from dlt.common.typing import TSecretValue
from dlt.version import get_installed_requirement_string

from dlt.helpers.dbt.runner import create_runner, DBTPackageRunner

DEFAULT_DLT_VERSION = ">=1.1<1.5"


def _default_profile_name(credentials: DestinationClientDwhConfiguration) -> str:
    profile_name = credentials.destination_name
    # in case of credentials with default add default to the profile name
    if isinstance(credentials.credentials, CredentialsWithDefault):
        if credentials.credentials.has_default_credentials():
            profile_name += "_default"
    return profile_name


def _create_dbt_deps(destination_names: List[str], dbt_version: str = DEFAULT_DLT_VERSION) -> List[str]:
    if dbt_version:
        # if parses as version use "==" operator
        with contextlib.suppress(ValueError):
            semver.parse(dbt_version)
            dbt_version = "==" + dbt_version
    else:
        dbt_version = ""

    all_packages = destination_names + ["core"]
    for idx, package in enumerate(all_packages):
        package_w_ver = "dbt-" + package + dbt_version
        # verify package
        pkg_resources.Requirement.parse(package_w_ver)
        all_packages[idx] = package_w_ver

    dlt_requirement = get_installed_requirement_string()

    return all_packages + [dlt_requirement]


def create_venv(venv_dir: str, destination_names: List[str], dbt_version: str = DEFAULT_DLT_VERSION) -> Venv:
    return Venv.create(venv_dir, _create_dbt_deps(destination_names, dbt_version))


def package_runner(
    venv: Venv,
    destination_configuration: DestinationClientDwhConfiguration,
    working_dir: str,
    package_location: str,
    package_repository_branch: str = None,
    package_repository_ssh_key: TSecretValue = TSecretValue(""),  # noqa
    auto_full_refresh_when_out_of_sync: bool = None
) -> DBTPackageRunner:
    default_profile_name = _default_profile_name(destination_configuration)
    dataset_name = destination_configuration.dataset_name
    return create_runner(
        venv,
        destination_configuration.credentials,
        working_dir,
        dataset_name,
        package_location,
        package_repository_branch=package_repository_branch,
        package_repository_ssh_key=package_repository_ssh_key,
        package_profile_name=default_profile_name,
        auto_full_refresh_when_out_of_sync=auto_full_refresh_when_out_of_sync
    )
