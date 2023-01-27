from typing import Sequence

from dlt.common.runners.venv import Venv
from dlt.common.destination import DestinationClientDwhConfiguration
from dlt.common.configuration.specs import CredentialsWithDefault

from dlt.dbt_runner.runner import get_runner, DBTPackageRunner


def _default_profile_name(credentials: DestinationClientDwhConfiguration,) -> str:
    profile_name = credentials.destination_name
    # in case of credentials with default add default to the profile name
    if isinstance(credentials.credentials, CredentialsWithDefault):
        if credentials.credentials.has_default_credentials():
            profile_name += "_default"
    return profile_name


# @with_custom_environ
# def run_dbt_in_venv(
#     venv: Venv,
#     credentials: DestinationClientDwhConfiguration,
#     working_dir: str,
#     package_location: str,
#     package_run_params: Sequence[str] = ("--fail-fast", ),
#     package_source_tests_selector: str = None,
#     destination_dataset_name: str = None
# ) -> Sequence[DBTNodeResult]:
#     # write credentials to environ, those are passed when calling venv
#     add_config_to_env(credentials.credentials)
#     default_profile_name = _default_profile_name(credentials)
#     dataset_name = credentials.dataset_name

#     args = [working_dir, default_profile_name, dataset_name, package_location, package_run_params, package_source_tests_selector, destination_dataset_name]

#     script = f"""
# from functools import partial

# from dlt.common.runners.stdout import exec_to_stdout
# from dlt.dbt_runner.runner import get_runner

# r = get_runner({", ".join(map(lambda arg: repr(arg), args))})
# with exec_to_stdout(r.run):
#     pass
# """

#     try:
#         i = iter_stdout_with_result(venv, "python", "-c", script)
#         while True:
#             print(next(i).strip())
#     except StopIteration as si:
#         # return result from generator
#         return si.value  # type: ignore
#     except CalledProcessError as cpe:
#         print(cpe.stderr)
#         raise


def dbt_package(
    venv: Venv,
    destination_configuration: DestinationClientDwhConfiguration,
    working_dir: str,
    package_location: str,
    package_run_params: Sequence[str] = ("--fail-fast", ),
    package_source_tests_selector: str = None,
    destination_dataset_name: str = None
) -> DBTPackageRunner:
    default_profile_name = _default_profile_name(destination_configuration)
    dataset_name = destination_configuration.dataset_name
    return get_runner(
        venv,
        destination_configuration.credentials,
        working_dir,
        default_profile_name,
        dataset_name,
        package_location,
        package_run_params,
        package_source_tests_selector,
        destination_dataset_name
    )
