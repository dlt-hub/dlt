from types import ModuleType
from typing import Any, ClassVar, Dict, List, Optional, Protocol, Union

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.utils import uniq_id


class SupportsRunContext(Protocol):
    """Describes where `dlt` looks for settings, pipeline working folder. Implementations must be picklable."""

    def __init__(self, run_dir: Optional[str], *args: Any, **kwargs: Any):
        """An explicit run_dir, if None, run_dir should be auto-detected by particular implementation"""

    @property
    def name(self) -> str:
        """Name of the run context. Entities like sources and destinations added to registries when this context
        is active, will be scoped to it. Typically corresponds to Python package name ie. `dlt`.
        """

    @property
    def uri(self) -> str:
        """Uniquely identifies the context. By default it is a combination of `run_dir` and `runtime_kwargs`
        to create file:// uri
        """

    @property
    def global_dir(self) -> str:
        """Directory in which global settings are stored ie ~/.dlt/"""

    @property
    def run_dir(self) -> str:
        """Defines the context working directory, defaults to cwd()"""

    @property
    def local_dir(self) -> str:
        """Defines data dir where local relative dirs and files are created, defaults to run_dir"""

    @property
    def settings_dir(self) -> str:
        """Defines where the current settings (secrets and configs) are located"""

    @property
    def data_dir(self) -> str:
        """Defines where the pipelines working folders are stored."""

    @property
    def module(self) -> Optional[ModuleType]:
        """if run_dir is a top level importable python module, returns it, otherwise return None"""

    @property
    def runtime_kwargs(self) -> Dict[str, Any]:
        """Additional kwargs used to initialize this instance of run context, used for reloading"""

    def initial_providers(self) -> List[ConfigProvider]:
        """Returns initial providers for this context"""

    def get_data_entity(self, entity: str) -> str:
        """Gets path in data_dir where `entity` (ie. `pipelines`, `repos`) are stored"""

    def get_run_entity(self, entity: str) -> str:
        """Gets path in run_dir where `entity` (ie. `sources`, `destinations` etc.) are stored"""

    def get_setting(self, setting_path: str) -> str:
        """Gets path in settings_dir where setting (ie. `secrets.toml`) are stored"""

    def unplug(self) -> None:
        """Called when context removed from container"""

    def plug(self) -> None:
        """Called when context is added to container"""


class PluggableRunContext(ContainerInjectableContext):
    """Injectable run context taken via plugin"""

    global_affinity: ClassVar[bool] = True

    context: SupportsRunContext = None
    providers: ConfigProvidersContainer
    runtime_config: RuntimeConfiguration

    _context_stack: List[Any] = []

    def __init__(
        self, init_context: SupportsRunContext = None, runtime_config: RuntimeConfiguration = None
    ) -> None:
        super().__init__()

        if init_context:
            self.context = init_context
        else:
            # autodetect run dir
            self._plug(run_dir=None)
        self.providers = ConfigProvidersContainer(self.context.initial_providers())
        self.runtime_config = runtime_config

    def reload(
        self,
        run_dir_or_context: Optional[Union[str, SupportsRunContext]] = None,
        runtime_kwargs: Dict[str, Any] = None,
    ) -> None:
        """Reloads the context, using existing settings if not overwritten with method args"""

        if run_dir_or_context is None:
            run_dir_or_context = self.context.run_dir
            if runtime_kwargs is None:
                runtime_kwargs = self.context.runtime_kwargs
            elif self.context.runtime_kwargs:
                runtime_kwargs = {**self.context.runtime_kwargs, **runtime_kwargs}

        self.runtime_config = None
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
        self.providers.add_extras()

    def after_add(self) -> None:
        super().after_add()

        # initialize runtime if context comes back into container
        if self.runtime_config:
            self.initialize_runtime(self.runtime_config)

    def before_remove(self) -> None:
        super().before_remove()

        if self.context:
            self.context.unplug()

    def add_extras(self) -> None:
        from dlt.common.configuration.resolve import resolve_configuration

        # add extra providers
        self.providers.add_extras()
        # resolve runtime configuration
        if not self.runtime_config:
            self.initialize_runtime(resolve_configuration(RuntimeConfiguration()))
        # plug context
        self.context.plug()

    def initialize_runtime(self, runtime_config: RuntimeConfiguration) -> None:
        self.runtime_config = runtime_config

        # do not activate logger if not in the container
        if not self.in_container:
            return

        from dlt.common.runtime.init import initialize_runtime

        initialize_runtime(self.context, self.runtime_config)

    def _plug(self, run_dir: Optional[str], runtime_kwargs: Dict[str, Any] = None) -> None:
        from dlt.common.configuration import plugins

        m = plugins.manager()
        self.context = m.hook.plug_run_context(run_dir=run_dir, runtime_kwargs=runtime_kwargs)
        assert self.context, "plug_run_context hook returned None"

    def push_context(self) -> str:
        """Pushes current context on stack and returns assert cookie"""
        cookie = uniq_id()
        self._context_stack.append((cookie, self.context, self.providers, self.runtime_config))
        return cookie

    def pop_context(self, cookie: str) -> None:
        """Pops context from stack and re-initializes it if in container"""
        _c, context, providers, runtime_config = self._context_stack.pop()
        if cookie != _c:
            raise ValueError(
                f"Run context stack mangled. Got cookie `{_c}` but expected `{cookie}`"
            )
        self.runtime_config = runtime_config
        self.reload(context)

    def drop_context(self, cookie: str) -> None:
        """Pops context form stack but leaves new context for good"""
        state_ = self._context_stack.pop()
        _c = state_[0]
        if cookie != _c:
            raise ValueError(
                f"Run context stack mangled. Got cookie `{_c}` but expected `{cookie}`"
            )
