import os
from types import ModuleType
from typing import Any, ClassVar, Dict, List, Optional, Union
from abc import ABC, abstractmethod

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    ContainerInjectableContext,
)
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.typing import Self
from dlt.common.utils import uniq_id


class RunContextBase(ABC):
    """Describes where `dlt` looks for settings, pipeline working folder. Implementations must be picklable."""

    @abstractmethod
    def __init__(self, run_dir: Optional[str], *args: Any, **kwargs: Any):
        """An explicit run_dir, if None, run_dir should be auto-detected by particular implementation"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the run context. Entities like sources and destinations added to registries when this context
        is active, will be scoped to it. Typically corresponds to Python package name ie. `dlt`.
        """

    @property
    @abstractmethod
    def uri(self) -> str:
        """Uniquely identifies the context. By default it is a combination of `run_dir` and `runtime_kwargs`
        to create file:// uri
        """

    @property
    @abstractmethod
    def global_dir(self) -> str:
        """Directory in which global settings are stored ie ~/.dlt/"""

    @property
    @abstractmethod
    def run_dir(self) -> str:
        """Defines the context working directory, defaults to cwd()"""

    @property
    @abstractmethod
    def local_dir(self) -> str:
        """Defines data dir where local relative dirs and files are created, defaults to run_dir"""

    @property
    @abstractmethod
    def settings_dir(self) -> str:
        """Defines where the current settings (secrets and configs) are located"""

    @property
    @abstractmethod
    def data_dir(self) -> str:
        """Defines where the pipelines working folders are stored."""

    @property
    @abstractmethod
    def module(self) -> Optional[ModuleType]:
        """if run_dir is a top level importable python module, returns it, otherwise return None"""

    @property
    @abstractmethod
    def runtime_kwargs(self) -> Dict[str, Any]:
        """Additional kwargs used to initialize this instance of run context, used for reloading"""

    @abstractmethod
    def initial_providers(self) -> List[ConfigProvider]:
        """Returns initial providers for this context"""

    @abstractmethod
    def initialize_runtime(self, runtime_config: RuntimeConfiguration = None) -> None:
        """Initializes runtime (ie. log, telemetry) using RuntimeConfiguration"""
        pass

    @property
    @abstractmethod
    def runtime_config(self) -> RuntimeConfiguration:
        """Runtime configuration used for initialize_runtime"""
        pass

    @property
    @abstractmethod
    def config(self) -> BaseConfiguration:
        """Returns (optionally resolves) run context configuration"""
        pass

    @abstractmethod
    def get_data_entity(self, entity: str) -> str:
        """Gets path in data_dir where `entity` (ie. `pipelines`, `repos`) are stored"""

    @abstractmethod
    def get_run_entity(self, entity: str) -> str:
        """Gets path in run_dir where `entity` (ie. `sources`, `destinations` etc.) are stored"""

    @abstractmethod
    def get_setting(self, setting_path: str) -> str:
        """Gets path in settings_dir where setting (ie. `secrets.toml`) are stored"""

    @abstractmethod
    def unplug(self) -> None:
        """Called when context removed from container"""

    @abstractmethod
    def plug(self) -> None:
        """Called when context is added to container"""

    def reload(self) -> "RunContextBase":
        """This will reload current context by triggering run context plugin via Container"""
        from dlt.common.configuration.container import Container

        plug_ctx = Container()[PluggableRunContext]
        plug_ctx.reload(self.run_dir, runtime_kwargs=self.runtime_kwargs)
        return plug_ctx.context

    @staticmethod
    def import_run_dir_module(run_dir: str) -> ModuleType:
        """Returns a top Python module of the workspace (if importable)"""
        import importlib

        # trailing separator will be removed by abspath
        run_dir = os.path.abspath(run_dir)
        base_dir = os.path.basename(run_dir)
        if not base_dir:
            raise ImportError(f"`{run_dir=:}` looks like filesystem root")
        m_ = importlib.import_module(base_dir)
        if m_.__file__ and m_.__file__.startswith(run_dir):
            return m_
        else:
            raise ImportError(
                f"`{run_dir=:}` doesn't belong to module `{m_.__file__}` which seems unrelated."
            )

    @abstractmethod
    def reset_config(self) -> None:
        """Hook for contexts that store resolved configuration to reset it"""


class ProfilesRunContext(RunContextBase):
    """Adds profile support on run context. Note: runtime checkable protocols are slow on isinstance"""

    @property
    @abstractmethod
    def profile(self) -> str:
        """Returns current profile name"""

    @property
    @abstractmethod
    def default_profile(self) -> str:
        """Returns default profile name"""

    @abstractmethod
    def available_profiles(self) -> List[str]:
        """Returns available profiles"""

    def configured_profiles(self) -> List[str]:
        """Returns profiles with configurations or dlt entities, same as available by default"""
        return self.available_profiles()

    @abstractmethod
    def switch_profile(self, new_profile: str) -> Self:
        """Switches current profile and returns new run context"""


class PluggableRunContext(ContainerInjectableContext):
    """Injectable run context taken via plugin"""

    global_affinity: ClassVar[bool] = True

    context: RunContextBase = None
    providers: ConfigProvidersContainer

    _context_stack: List[Any] = []

    def __init__(self, init_context: RunContextBase = None) -> None:
        super().__init__()

        if init_context:
            self.context = init_context
        else:
            # autodetect run dir
            self._plug(run_dir=None)
        self.providers = ConfigProvidersContainer(self.context.initial_providers())

    def reload(
        self,
        run_dir_or_context: Optional[Union[str, RunContextBase]] = None,
        runtime_kwargs: Dict[str, Any] = None,
    ) -> None:
        """Reloads the context, using existing settings if not overwritten with method args"""

        if run_dir_or_context is None:
            run_dir_or_context = self.context.run_dir
            if runtime_kwargs is None:
                runtime_kwargs = self.context.runtime_kwargs
            elif self.context.runtime_kwargs:
                runtime_kwargs = {**self.context.runtime_kwargs, **runtime_kwargs}

        self.before_remove()
        if isinstance(run_dir_or_context, str):
            self._plug(run_dir_or_context, runtime_kwargs=runtime_kwargs)
        else:
            self.context = run_dir_or_context
        self.providers = ConfigProvidersContainer(self.context.initial_providers())
        self.after_add()
        # adds remaining providers and initializes runtime
        self.add_extras()

    def reload_providers(self) -> None:
        self.providers = ConfigProvidersContainer(self.context.initial_providers())
        # Re-add extras and re-initialize runtime so changes take effect
        self.providers.add_extras()
        # Invalidate any cached configuration on the context so it re-resolves using new providers
        self.context.reset_config()

    def after_add(self) -> None:
        super().after_add()

        # initialize runtime if context comes back into container
        self.initialize_runtime()

    def before_remove(self) -> None:
        super().before_remove()

        if self.context:
            self.context.unplug()

    def add_extras(self) -> None:
        # add extra providers
        self.providers.add_extras()
        # resolve runtime configuration
        self.initialize_runtime()
        # plug context
        self.context.plug()

    def initialize_runtime(self) -> None:
        """Calls initialize_runtime on context only if active in container. We do not want
        to initialize runtime if instance is not active
        """
        if not self.in_container:
            return
        self.context.initialize_runtime()

    def _plug(self, run_dir: Optional[str], runtime_kwargs: Dict[str, Any] = None) -> None:
        from dlt.common.configuration import plugins

        m = plugins.manager()
        self.context = m.hook.plug_run_context(run_dir=run_dir, runtime_kwargs=runtime_kwargs)
        assert self.context, "plug_run_context hook returned None"

    def push_context(self) -> str:
        """Pushes current context on stack and returns assert cookie"""
        cookie = uniq_id()
        self._context_stack.append((cookie, self.context, self.providers))
        return cookie

    def pop_context(self, cookie: str) -> None:
        """Pops context from stack and re-initializes it if in container"""
        _c, context, providers = self._context_stack.pop()
        if cookie != _c:
            raise ValueError(
                f"Run context stack mangled. Got cookie `{_c}` but expected `{cookie}`"
            )
        self.reload(context)

    def drop_context(self, cookie: str) -> None:
        """Pops context form stack but leaves new context for good"""
        state_ = self._context_stack.pop()
        _c = state_[0]
        if cookie != _c:
            raise ValueError(
                f"Run context stack mangled. Got cookie `{_c}` but expected `{cookie}`"
            )
