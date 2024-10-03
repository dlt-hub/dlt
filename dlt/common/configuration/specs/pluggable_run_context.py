from typing import ClassVar, Protocol

from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext


class SupportsRunContext(Protocol):
    """Describes where `dlt` looks for settings, pipeline working folder"""

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

    def __init__(self) -> None:
        super().__init__()

        from dlt.common.configuration import plugins

        m = plugins.manager()
        self.context = m.hook.plug_run_context()
        assert self.context, "plug_run_context hook returned None"
