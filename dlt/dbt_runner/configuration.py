import dataclasses
from os import environ
from typing import List, Optional, Type

from dlt.common.typing import StrAny, TSecretValue
from dlt.common.configuration import resolve_configuration, configspec
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.specs import RunConfiguration, PoolRunnerConfiguration, TPoolType, PostgresCredentials, GcpClientCredentials


@configspec
class DBTRunnerConfiguration(RunConfiguration, PoolRunnerConfiguration):
    pool_type: TPoolType = "none"
    stop_after_runs: int = 1
    package_volume_path: str = "/var/local/app"
    package_repository_url: str = "https://github.com/scale-vector/rasa_semantic_schema_customization.git"
    package_repository_branch: Optional[str] = None
    package_repository_ssh_key: TSecretValue = TSecretValue("")  # the default is empty value which will disable custom SSH KEY
    package_profiles_dir: str = "."
    package_profile_prefix: str = "rasa_semantic_schema"
    package_source_tests_selector: str = "tag:prerequisites"
    package_additional_vars: Optional[StrAny] = None
    package_run_params: List[str] = dataclasses.field(default_factory=lambda: ["--fail-fast"])
    auto_full_refresh_when_out_of_sync: bool = True

    source_schema_prefix: str = None
    dest_schema_prefix: Optional[str] = None

    def on_resolved(self) -> None:
        if self.package_repository_ssh_key and self.package_repository_ssh_key[-1] != "\n":
            # must end with new line, otherwise won't be parsed by Crypto
            self.package_repository_ssh_key = TSecretValue(self.package_repository_ssh_key + "\n")
        if self.stop_after_runs != 1:
            # always stop after one run
            self.stop_after_runs = 1


def gen_configuration_variant(explicit_values: StrAny = None) -> DBTRunnerConfiguration:
    # derive concrete config depending on env vars present
    DBTRunnerConfigurationImpl: Type[DBTRunnerConfiguration]
    environ = EnvironProvider()

    source_schema_prefix: str = environ.get_value("dataset_name", type(str))  # type: ignore

    if environ.get_value("project_id", type(str), GcpClientCredentials.__namespace__):
        @configspec
        class DBTRunnerConfigurationPostgres(PostgresCredentials, DBTRunnerConfiguration):
            SOURCE_SCHEMA_PREFIX: str = source_schema_prefix
        DBTRunnerConfigurationImpl = DBTRunnerConfigurationPostgres

    else:
        @configspec
        class DBTRunnerConfigurationGcp(GcpClientCredentials, DBTRunnerConfiguration):
            SOURCE_SCHEMA_PREFIX: str = source_schema_prefix
        DBTRunnerConfigurationImpl = DBTRunnerConfigurationGcp

    return resolve_configuration(DBTRunnerConfigurationImpl(), explicit_value=explicit_values)
