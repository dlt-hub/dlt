from typing import Any, ClassVar, Dict, List, Optional, Protocol

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer


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
    def global_dir(self) -> str:
        """Directory in which global settings are stored ie ~/.dlt/"""

    @property
    def run_dir(self) -> str:
        """Defines the current working directory"""

    @property
    def settings_dir(self) -> str:
        """Defines where the current settings (secrets and configs) are located"""

    @property
    def data_dir(self) -> str:
        """Defines where the pipelines working folders are stored."""

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


class PluggableRunContext(ContainerInjectableContext):
    """Injectable run context taken via plugin"""

    global_affinity: ClassVar[bool] = True

    context: SupportsRunContext
    providers: ConfigProvidersContainer
    runtime_config: RuntimeConfiguration

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

    def reload(self, run_dir: Optional[str] = None, runtime_kwargs: Dict[str, Any] = None) -> None:
        """Reloads the context, using existing settings if not overwritten with method args"""

        if run_dir is None:
            run_dir = self.context.run_dir
            if runtime_kwargs is None:
                runtime_kwargs = self.context.runtime_kwargs

        self.runtime_config = None
        self._plug(run_dir, runtime_kwargs=runtime_kwargs)

        self.providers = ConfigProvidersContainer(self.context.initial_providers())
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

    def add_extras(self) -> None:
        from dlt.common.configuration.resolve import resolve_configuration

        # add extra providers
        self.providers.add_extras()
        # resolve runtime configuration
        if not self.runtime_config:
            self.initialize_runtime(resolve_configuration(RuntimeConfiguration()))

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
