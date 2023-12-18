from typing import Type
from dlt.common.typing import TDataItem
from dlt.common.pipeline import SupportsPipeline
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.schema.typing import TSchemaContract
from dlt.common.configuration.specs import configspec


class Plugin:
    NAME: str = None

    def __init__(self, pipeline: SupportsPipeline, step: str):
        self.pipeline = pipeline
        self.step = step

    def on_step_start(self):
        pass

    def on_step_end(self):
        pass

    #
    # extraction callbacks
    #
    def on_extractor_item_written(self, item: TDataItem, **kwargs):
        pass

    #
    # normalizer callbacks
    #
    def filter_row(self, table_name: str, item: TDataItem, **kwargs):
        return item
    
    #
    # contracts callbacks
    #
    def on_schema_contract_violation(self, schema_contract: TSchemaContract, table_name: str, violating_item: TDataItem, **kwargs):
        pass


_PLUGINS: list[Type[Plugin]] = []
def register_plugin(plugin: Plugin):
    global _PLUGINS
    _PLUGINS.append(plugin)

@configspec
class PluginsContext(ContainerInjectableContext):

    global_affinity: bool = False

    def __init__(self) -> None:
        self.plugins: list[Plugin] = []

    def setup_plugins(self, pipeline: SupportsPipeline, step: str):
        self.plugins = list(map(lambda p: p(pipeline, step), _PLUGINS))
