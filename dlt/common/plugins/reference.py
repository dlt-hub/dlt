from typing import Type, Union, List, Any, Optional, Generic, TypeVar
from dlt.common.typing import TDataItem
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.schema.exceptions import DataValidationError
from dlt.common.pipeline import SupportsPipeline


TPluginConfig = TypeVar("TPluginConfig", bound=BaseConfiguration, covariant=True)


class SupportsCallbackPlugin:
    def on_start(self, pipeline: SupportsPipeline) -> None:
        pass

    def on_end(self, pipeline: SupportsPipeline) -> None:
        pass

    def on_step_start(self, step: str, pipeline: SupportsPipeline) -> None:
        pass

    def on_step_end(self, step: str, pipeline: SupportsPipeline) -> None:
        pass

    #
    # contracts callbacks
    #
    def on_schema_contract_violation(self, error: DataValidationError, **kwargs: Any) -> None:
        pass

    def on_schema_contract_violation_safe(self, error: DataValidationError, **kwargs: Any) -> None:
        pass


class Plugin(Generic[TPluginConfig]):
    NAME: Optional[str] = None
    SPEC: Type[TPluginConfig] = BaseConfiguration  # type: ignore[assignment]

    def __init__(self) -> None:
        self.step: Union[str, None] = None
        if not self.NAME:
            self.NAME = type(self).__name__.lower()
        if self.SPEC:
            self.config = resolve_configuration(self.SPEC(), sections=("plugin", self.NAME))


class CallbackPlugin(Plugin[TPluginConfig], SupportsCallbackPlugin, Generic[TPluginConfig]):
    pass


TSinglePluginArg = Union[Type[Plugin], Plugin, str]
TPluginArg = Union[List[TSinglePluginArg], TSinglePluginArg]
