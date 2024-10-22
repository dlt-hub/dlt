import os
from typing import Optional, Sequence

from dlt.common.typing import StrAny, TSecretStrValue
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, RuntimeConfiguration


@configspec
class DBTRunnerConfiguration(BaseConfiguration):
    package_location: str = None
    package_repository_branch: Optional[str] = None
    # the default is empty value which will disable custom SSH KEY
    package_repository_ssh_key: Optional[TSecretStrValue] = ""
    package_profiles_dir: Optional[str] = None
    package_profile_name: Optional[str] = None
    auto_full_refresh_when_out_of_sync: bool = True

    package_additional_vars: Optional[StrAny] = None

    runtime: RuntimeConfiguration = None

    def on_resolved(self) -> None:
        if not self.package_profiles_dir:
            # use "profile.yml" located in the same folder as current module
            self.package_profiles_dir = os.path.dirname(__file__)
        if self.package_repository_ssh_key and self.package_repository_ssh_key[-1] != "\n":
            # must end with new line, otherwise won't be parsed by Crypto
            self.package_repository_ssh_key = self.package_repository_ssh_key + "\n"
