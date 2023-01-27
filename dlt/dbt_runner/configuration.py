import os
from typing import Optional, Sequence

from dlt.common.typing import StrAny, TSecretValue
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, RunConfiguration


@configspec
class DBTRunnerConfiguration(BaseConfiguration):
    package_location: str = None
    package_repository_branch: Optional[str] = None
    package_repository_ssh_key: TSecretValue = TSecretValue("")  # the default is empty value which will disable custom SSH KEY
    package_profiles_dir: Optional[str] = None
    package_profile_name: Optional[str] = None
    package_source_tests_selector: Optional[str] = None
    package_additional_vars: Optional[StrAny] = None
    package_run_params: Optional[Sequence[str]] = None

    auto_full_refresh_when_out_of_sync: bool = True
    destination_dataset_name: Optional[str] = None

    runtime: RunConfiguration

    def on_resolved(self) -> None:
        if self.package_run_params is None:
            self.package_run_params = []
        if not isinstance(self.package_run_params, list):
            # we always expect lists
            self.package_run_params = list(self.package_run_params)
        if not self.package_profiles_dir and not self.package_profile_name:
            # use "profile.yml" located in the same folder as current module
            self.package_profiles_dir = os.path.dirname(__file__)
        if self.package_repository_ssh_key and self.package_repository_ssh_key[-1] != "\n":
            # must end with new line, otherwise won't be parsed by Crypto
            self.package_repository_ssh_key = TSecretValue(self.package_repository_ssh_key + "\n")
