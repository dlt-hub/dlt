import os
from types import ModuleType
from typing import Any, Dict, List, Optional

from dlt.common import known_env
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.pluggable_run_context import (
    ProfilesRunContext,
    PluggableRunContext,
)
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.runtime.init import initialize_runtime
from dlt.common.runtime.run_context import (
    DOT_DLT,
    switch_context as _switch_context,
    context_uri,
    global_dir,
)
from dlt.common.typing import copy_sig_ret

from dlt._workspace.configuration import WorkspaceConfiguration, WorkspaceRuntimeConfiguration
from dlt._workspace.exceptions import WorkspaceRunContextNotAvailable
from dlt._workspace.profile import BUILT_IN_PROFILES, DEFAULT_PROFILE, read_profile_pin
from dlt._workspace.providers import ProfileConfigTomlProvider, ProfileSecretsTomlProvider
from dlt._workspace.run_context import (
    DEFAULT_LOCAL_FOLDER,
    DEFAULT_WORKSPACE_WORKING_FOLDER,
    default_working_dir,
    switch_profile as _switch_profile,
)


class WorkspaceRunContext(ProfilesRunContext):
    """A run context with workspace."""

    def __init__(self, name: str, run_dir: str, profile: str):
        self._init_run_dir = run_dir
        self._name = name
        self._profile = profile
        self._data_dir: str = None
        self._local_dir: str = None
        self._global_dir = global_dir()
        self._config: WorkspaceConfiguration = None

    @property
    def name(self) -> str:
        """Defines workspace name which is (normalized) parent folder name"""
        return self._name

    @property
    def global_dir(self) -> str:
        """Directory in which global settings are stored ie ~/.dlt/"""
        return self._global_dir

    @property
    def uri(self) -> str:
        return context_uri(self.name, self.run_dir, self.runtime_kwargs)

    @property
    def run_dir(self) -> str:
        """The default run dir is the current working directory but may be overridden by DLT_PROJECT_DIR env variable."""
        return os.environ.get(known_env.DLT_PROJECT_DIR, self._init_run_dir)

    @property
    def local_dir(self) -> str:
        assert self._local_dir, "local_dir used before workspace configuration got resolved"
        return os.environ.get(known_env.DLT_LOCAL_DIR, self._local_dir)

    @property
    def settings_dir(self) -> str:
        """Returns a path to dlt settings directory. If not overridden it resides in current working directory

        The name of the settings folder is '.dlt'. The path is current working directory '.' but may be overridden by DLT_PROJECT_DIR env variable.
        """
        return os.path.join(self.run_dir, DOT_DLT)

    @property
    def data_dir(self) -> str:
        assert self._data_dir, "data_dir used before workspace configuration got resolved"
        return os.environ.get(known_env.DLT_DATA_DIR, self._data_dir)

    def initial_providers(self) -> List[ConfigProvider]:
        return self._initial_providers(self.profile)

    def initialize_runtime(self, runtime_config: RuntimeConfiguration = None) -> None:
        if runtime_config is not None:
            assert isinstance(runtime_config, WorkspaceRuntimeConfiguration)
            self.config.runtime = runtime_config

        # this also resolves workspace config if necessary
        initialize_runtime(self.name, self.config.runtime)

        # if on runtime, add additional tracker
        if self.runtime_config.run_id:
            from dlt._workspace.helpers.runtime import runtime_artifacts
            from dlt.pipeline import trace

            if runtime_artifacts not in trace.TRACKING_MODULES:
                trace.TRACKING_MODULES.append(runtime_artifacts)

    @property
    def runtime_config(self) -> WorkspaceRuntimeConfiguration:
        return self.config.runtime

    @property
    def config(self) -> WorkspaceConfiguration:
        def _to_run_dir(dir_: Optional[str]) -> Optional[str]:
            if not dir_:
                return None
            return os.path.join(self.run_dir, dir_)

        if self._config is None:
            from dlt.common.configuration.resolve import resolve_configuration

            self._config = resolve_configuration(
                WorkspaceConfiguration(), sections=(known_sections.WORKSPACE,)
            )
            # overwrite name
            if self._config.settings.name:
                self._name = self._config.settings.name

            self._data_dir = (
                _to_run_dir(self._config.settings.working_dir) or self._make_default_working_dir()
            )
            self._local_dir = _to_run_dir(self._config.settings.local_dir) or default_working_dir(
                self.run_dir,
                self.name,
                self.profile,
                DEFAULT_LOCAL_FOLDER,
            )
        return self._config

    @property
    def module(self) -> Optional[ModuleType]:
        try:
            return self.import_run_dir_module(self.run_dir)
        except (ImportError, TypeError):
            return None

    @property
    def runtime_kwargs(self) -> Dict[str, Any]:
        return {"profile": self._profile}

    def get_data_entity(self, entity: str) -> str:
        return os.path.join(self.data_dir, entity)

    def get_run_entity(self, entity: str) -> str:
        """Default run context assumes that entities are defined in root dir"""
        return self.run_dir

    def get_setting(self, setting_path: str) -> str:
        return os.path.join(self.settings_dir, setting_path)

    def plug(self) -> None:
        # create temp and data dirs
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.local_dir, exist_ok=True)

    def unplug(self) -> None:
        pass

    def reset_config(self) -> None:
        # Drop resolved configuration to force re-resolve with refreshed providers
        self._config = None
        # no need to initialize the _config anew as it's done in .config property

    # SupportsProfilesOnContext

    @property
    def profile(self) -> str:
        return self._profile

    @property
    def default_profile(self) -> str:
        return DEFAULT_PROFILE

    def available_profiles(self) -> List[str]:
        profiles = list(BUILT_IN_PROFILES.keys())
        if pinned_profile := read_profile_pin(self):
            if pinned_profile not in BUILT_IN_PROFILES:
                profiles.append(pinned_profile)
        return profiles

    def configured_profiles(self) -> List[str]:
        """Returns profiles that have configuration or pipelines.

        A profile is considered configured if:
        - It is the current profile
        - It is the pinned profile
        - It has any toml configuration files (config.toml or secrets.toml with profile prefix)
        - It has pipelines in its working directory

        NOTE: calling this function is relatively expensive as it probes all available profiles
        """
        configured: set[str] = set()

        # current profile is always configured
        configured.add(self.profile)

        # pinned profile is always configured
        if pinned := read_profile_pin(self):
            configured.add(pinned)

        # probe all available profiles
        for profile_name in self.available_profiles():
            if profile_name in configured:
                continue

            # Check if profile has any toml config files
            if self._profile_has_config(profile_name):
                configured.add(profile_name)
                continue

            # Check if profile has any pipelines
            if self._profile_has_pipelines(profile_name):
                configured.add(profile_name)

        return list(configured)

    def _initial_providers(self, profile_name: str) -> List[ConfigProvider]:
        providers = [
            EnvironProvider(),
            ProfileSecretsTomlProvider(self.settings_dir, profile_name, self.global_dir),
            ProfileConfigTomlProvider(self.settings_dir, profile_name, self.global_dir),
        ]
        return providers

    def _make_default_working_dir(self, profile_name: str = None) -> str:
        return default_working_dir(
            self.settings_dir,
            self.name,
            profile_name or self.profile,
            DEFAULT_WORKSPACE_WORKING_FOLDER,
        )

    def _has_default_working_dir(self) -> bool:
        """Checks if current working dir has default layout that includes profiles"""
        return self._data_dir == self._make_default_working_dir()

    def _profile_has_config(self, profile_name: str) -> bool:
        """Check if a profile has any configuration files."""
        # check if any profile-specific files were found
        for provider in self._initial_providers(profile_name):
            for location in provider.present_locations:
                # check if it's a profile-specific file (starts with profile name)
                if os.path.basename(location).startswith(f"{profile_name}."):
                    return True
        return False

    def _profile_has_pipelines(self, profile_name: str) -> bool:
        """Check if a profile has any pipelines in its data directory."""
        # non default layouts can be probed
        if not self._has_default_working_dir():
            return False

        working_dir = self._make_default_working_dir(profile_name)
        pipelines_dir = os.path.join(working_dir, "pipelines")
        try:
            return os.path.isdir(pipelines_dir) and bool(os.listdir(pipelines_dir))
        except OSError:
            return False

    def switch_profile(self, new_profile: str) -> "WorkspaceRunContext":
        return switch_context(self.run_dir, new_profile, required="WorkspaceRunContext")


switch_context = copy_sig_ret(_switch_context, WorkspaceRunContext)(_switch_context)
switch_profile = copy_sig_ret(_switch_profile, WorkspaceRunContext)(_switch_profile)


def active() -> WorkspaceRunContext:
    """Returns currently active Workspace"""
    ctx = Container()[PluggableRunContext].context
    if not isinstance(ctx, WorkspaceRunContext):
        raise WorkspaceRunContextNotAvailable(ctx.run_dir)
    return ctx
