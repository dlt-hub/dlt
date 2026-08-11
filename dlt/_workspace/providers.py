from typing import Any, Optional, List
import os
from dlt.common.configuration.providers.toml import (
    ConfigTomlProvider,
    SecretsTomlProvider,
)
from dlt.common.configuration.providers.yaml import (
    ConfigYamlProvider,
    SecretsYamlProvider,
)


class ProfilePathMixin:
    """A mixin that adds profile-aware path resolution.

    Overrides both `_resolve_toml_paths` and `_resolve_yaml_paths` with the same logic, so it can
    be combined with either a toml or a yaml settings provider.
    """

    def __init__(self, *args: Any, profile: str, **kwargs: Any) -> None:
        self._profile = profile
        super().__init__(*args, **kwargs)

    def _resolve_profile_paths(self, file_name: str, resolvable_dirs: List[str]) -> List[str]:
        resolvable_files = []
        for d in resolvable_dirs:
            # append each a profile and a base file name for each directory
            # profile name is always first
            resolvable_files.append(os.path.join(d, f"{self._profile}.{file_name}"))
            resolvable_files.append(os.path.join(d, file_name))
        return resolvable_files

    _resolve_toml_paths = _resolve_profile_paths
    _resolve_yaml_paths = _resolve_profile_paths


class ProfileSecretsTomlProvider(ProfilePathMixin, SecretsTomlProvider):
    def __init__(self, settings_dir: str, profile: str, global_dir: Optional[str] = None) -> None:
        """a secret toml provider loading from {profile}.secrets.toml file."""
        super().__init__(settings_dir=settings_dir, global_dir=global_dir, profile=profile)


class ProfileConfigTomlProvider(ProfilePathMixin, ConfigTomlProvider):
    def __init__(self, settings_dir: str, profile: str, global_dir: Optional[str] = None) -> None:
        """a config toml provider loading from {profile}.config.toml file."""
        super().__init__(settings_dir=settings_dir, global_dir=global_dir, profile=profile)


class ProfileSecretsYamlProvider(ProfilePathMixin, SecretsYamlProvider):
    def __init__(self, settings_dir: str, profile: str, global_dir: Optional[str] = None) -> None:
        """a secret yaml provider loading from {profile}.secrets.yaml file."""
        super().__init__(settings_dir=settings_dir, global_dir=global_dir, profile=profile)


class ProfileConfigYamlProvider(ProfilePathMixin, ConfigYamlProvider):
    def __init__(self, settings_dir: str, profile: str, global_dir: Optional[str] = None) -> None:
        """a config yaml provider loading from {profile}.config.yaml file."""
        super().__init__(settings_dir=settings_dir, global_dir=global_dir, profile=profile)
