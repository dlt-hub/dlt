import os
from types import ModuleType
from typing import Any, Dict, List, Optional

from dlt.common import known_env
from dlt.common.utils import simple_repr
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
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

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.configuration import WorkspaceConfiguration, WorkspaceRuntimeConfiguration
from dlt._workspace.exceptions import (
    ProfileNotDeclared,
    TProfileSource,
    WorkspaceRunContextNotAvailable,
)
from dlt._workspace.profile import DEFAULT_PROFILE
from dlt._workspace.settings_files import profiles_with_settings, read_profile_pin
from dlt._workspace.workspace_definition import (
    declared_profiles,
    default_profile,
    environment_definition,
    environment_of_profile,
    is_profile_declared,
    load_workspace_definition,
    profile_definition,
    shown_workspace_file,
)
from dlt._workspace.providers import ProfileConfigTomlProvider, ProfileSecretsTomlProvider
from dlt._workspace.run_context import (
    DEFAULT_LOCAL_FOLDER,
    DEFAULT_WORKSPACE_WORKING_FOLDER,
    default_working_dir,
    switch_profile as _switch_profile,
)
from dlt._workspace.typing import TEnvironmentDefinition, TProfileDefinition, TWorkspaceDefinition


class WorkspaceRunContext(ProfilesRunContext):
    """A run context with workspace."""

    def __init__(self, name: str, run_dir: str, profile: Optional[str] = None):
        self._init_run_dir = run_dir
        self._name = name
        self._profile = profile
        self._data_dir: str = None
        self._local_dir: str = None
        self._global_dir = global_dir()
        self._config: WorkspaceConfiguration = None
        self._resolved_name: Optional[str] = None
        self._workspace_definition: TWorkspaceDefinition = None

    def __repr__(self) -> str:
        return simple_repr("WorkspaceRunContext", run_dir=self.run_dir, profile=self.profile)

    @property
    def name(self) -> str:
        """Workspace name: the parent folder name until the configuration is resolved, then
        `[workspace.settings] name` or the name of the remote workspace of the active environment.
        """
        return self._resolved_name or self._name

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
            self._config = self._resolve_config(self.environment)
            self._resolved_name = self._config.settings.name

            self._data_dir = (
                _to_run_dir(self._config.settings.working_dir) or self._make_default_working_dir()
            )
            self._local_dir = _to_run_dir(self._config.settings.local_dir) or default_working_dir(
                self.settings_dir,
                self.name,
                self.profile,
                DEFAULT_LOCAL_FOLDER,
            )
        return self._config

    def environment_config(self, environment: str) -> WorkspaceConfiguration:
        """Workspace configuration as the default write profile of `environment` resolves it: the
        effective name and the connected remote workspace, without changing the active profile."""
        profile = default_profile(self.get_environment_definition(environment), "write")
        if profile == self.profile:
            return self.config
        plug = Container()[PluggableRunContext]
        providers = plug.providers
        plug.providers = ConfigProvidersContainer(self._initial_providers(profile))
        try:
            return self._resolve_config(environment)
        finally:
            plug.providers = providers

    def _resolve_config(self, environment: Optional[str]) -> WorkspaceConfiguration:
        return resolve_configuration(
            WorkspaceConfiguration(
                name=self._name,
                environment=environment,
                default_environment=self.default_environment,
            ),
            sections=(known_sections.WORKSPACE,),
        )

    @property
    def module(self) -> Optional[ModuleType]:
        try:
            return self.import_run_dir_module(self.run_dir)
        except (ImportError, TypeError):
            return None

    @property
    def runtime_kwargs(self) -> Dict[str, Any]:
        return {"profile": self.profile}

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
        self._workspace_definition = None
        # no need to initialize the _config anew as it's done in .config property

    @property
    def profile(self) -> str:
        if self._profile is None:
            self._profile = self.default_profile
        return self._profile

    @property
    def workspace_definition(self) -> TWorkspaceDefinition:
        """The profiles and environments the workspace declares, migrated to the current engine
        version.

        Raises:
            ProfileNotDeclared: The profile of this context is not declared.
        """
        if self._workspace_definition is None:
            doc = load_workspace_definition(self)
            profile = self._profile or _default_local_profile(doc)
            if not is_profile_declared(doc, profile):
                raise ProfileNotDeclared(
                    profile,
                    shown_workspace_file(self),
                    list(doc["environments"]),
                    self._profile_source(profile),
                    list(declared_profiles(doc)),
                )
            self._workspace_definition = doc
        return self._workspace_definition

    def _profile_source(self, profile: str) -> TProfileSource:
        if self._profile is None:
            return "default"
        if os.environ.get(WORKSPACE__PROFILE) == profile:
            return "environ"
        if read_profile_pin(self) == profile:
            return "recorded"
        return "argument"

    def reload_workspace_definition(self) -> None:
        """Loads the declared profiles and environments again on the next access."""
        self._workspace_definition = None

    @property
    def profile_definitions(self) -> Dict[str, TProfileDefinition]:
        """Profiles the workspace declares: the local ones, then those of each environment."""
        return declared_profiles(self.workspace_definition)

    def get_profile_definition(self, name: str) -> TProfileDefinition:
        """Declared profile `name`."""
        return profile_definition(self.workspace_definition, name)

    def get_environment_definition(self, name: str) -> TEnvironmentDefinition:
        """Declared environment `name`, with its profiles."""
        return environment_definition(self.workspace_definition, name)

    def is_local_profile(self, name: str) -> bool:
        return self.get_profile_definition(name)["location"] == "local"

    @property
    def environment_definitions(self) -> Dict[str, TEnvironmentDefinition]:
        """Environments the workspace declares."""
        return self.workspace_definition["environments"]

    @property
    def environment(self) -> Optional[str]:
        """Environment of the active profile, `None` for a local profile."""
        return environment_of_profile(self.workspace_definition, self.profile)

    @property
    def default_profile(self) -> str:
        """The default local profile, assumed when nothing names a profile."""
        return _default_local_profile(self.workspace_definition)

    @property
    def default_environment(self) -> Optional[str]:
        """The default environment, assumed by commands that deploy or work with a deployment
        when nothing names one."""
        return next((n for n, e in self.environment_definitions.items() if e.get("default")), None)

    def available_profiles(self) -> List[str]:
        """The declared profiles."""
        return list(self.profile_definitions)

    def configured_profiles(self) -> List[str]:
        """Declared profiles that have a `<profile>.config.toml` or `<profile>.secrets.toml`."""
        with_settings = set(profiles_with_settings(self.settings_dir))
        return [name for name in self.profile_definitions if name in with_settings]

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

    def switch_profile(self, new_profile: str) -> "WorkspaceRunContext":
        return switch_context(self.run_dir, new_profile, required="WorkspaceRunContext")


def _default_local_profile(doc: TWorkspaceDefinition) -> str:
    for name, profile in doc["local_profiles"].items():
        if profile.get("default"):
            return name
    return DEFAULT_PROFILE


switch_context = copy_sig_ret(_switch_context, WorkspaceRunContext)(_switch_context)
switch_profile = copy_sig_ret(_switch_profile, WorkspaceRunContext)(_switch_profile)


def active() -> WorkspaceRunContext:
    """Returns currently active Workspace"""
    ctx = Container()[PluggableRunContext].context
    if not isinstance(ctx, WorkspaceRunContext):
        raise WorkspaceRunContextNotAvailable(ctx.run_dir)
    return ctx
