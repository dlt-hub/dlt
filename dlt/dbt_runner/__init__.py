from typing import Sequence

from dlt.common.runners.venv import Venv
from dlt.common.destination import DestinationClientDwhConfiguration
from dlt.common.configuration.specs import CredentialsWithDefault
from dlt.common.typing import TSecretValue

from dlt.dbt_runner.runner import create_runner, DBTPackageRunner


def _default_profile_name(credentials: DestinationClientDwhConfiguration,) -> str:
    profile_name = credentials.destination_name
    # in case of credentials with default add default to the profile name
    if isinstance(credentials.credentials, CredentialsWithDefault):
        if credentials.credentials.has_default_credentials():
            profile_name += "_default"
    return profile_name


def package_runner(
    venv: Venv,
    destination_configuration: DestinationClientDwhConfiguration,
    working_dir: str,
    package_location: str,
    package_repository_branch: str = None,
    package_repository_ssh_key: TSecretValue = TSecretValue(""),  # noqa
    auto_full_refresh_when_out_of_sync: bool = None,
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
