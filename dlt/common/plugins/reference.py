from typing import Type, Union, List, Any
from dlt.common.typing import TDataItem
from dlt.common.schema.typing import TSchemaContract


class SupportsCallbackPlugin:
    def on_step_start(self, step: str) -> None:
        pass

    def on_step_end(self, step: str) -> None:
        pass

    #
    # extraction callbacks
    #
    def on_extractor_item_written(self, item: TDataItem, **kwargs: Any) -> None:
        pass

    #
    # normalizer callbacks
    #
    def filter_row(self, table_name: str, item: TDataItem, **kwargs: Any) -> TDataItem:
        return item

    #
    # contracts callbacks
    #
    def on_schema_contract_violation(
        self,
        schema_contract: TSchemaContract,
        table_name: str,
        violating_item: TDataItem,
        **kwargs: Any
    ) -> None:
        pass


class Plugin(SupportsCallbackPlugin):
    NAME: str = None

    def __init__(self) -> None:
        self.step: Union[str, None] = None


TSinglePluginArg = Union[Type[Plugin], Plugin, str]
TPluginArg = Union[List[TSinglePluginArg], TSinglePluginArg]
