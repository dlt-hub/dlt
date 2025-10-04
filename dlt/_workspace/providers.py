from typing import Any, Optional, List
import os
from dlt.common.configuration.providers.toml import (
    ConfigTomlProvider,
    SecretsTomlProvider,
)


class ProfilePathMixin:
    def __init__(self, *args: Any, profile: str, **kwargs: Any) -> None:
        """A mixin that adds profile-aware path resolution by overriding `_resolve_toml_paths`"""
        self._profile = profile
        super().__init__(*args, **kwargs)

    def _resolve_toml_paths(self, file_name: str, resolvable_dirs: List[str]) -> List[str]:
        resolvable_files = []
        for d in resolvable_dirs:
            # append each a profile and a base file name for each directory
            # profile name is always first
            resolvable_files.append(os.path.join(d, f"{self._profile}.{file_name}"))
            resolvable_files.append(os.path.join(d, file_name))
        return resolvable_files


class ProfileSecretsTomlProvider(ProfilePathMixin, SecretsTomlProvider):
    def __init__(self, settings_dir: str, profile: str, global_dir: Optional[str] = None) -> None:
        """a secret toml provider loading from {profile}.secrets.toml file."""
        super().__init__(settings_dir=settings_dir, global_dir=global_dir, profile=profile)


class ProfileConfigTomlProvider(ProfilePathMixin, ConfigTomlProvider):
    def __init__(self, settings_dir: str, profile: str, global_dir: Optional[str] = None) -> None:
        """a config toml provider loading from {profile}.config.toml file."""
        super().__init__(settings_dir=settings_dir, global_dir=global_dir, profile=profile)
