import dlt, os
from typing import Any
from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests
from dlt.common.plugins import CallbackPlugin, PluginConfig
from dlt.common.configuration.specs.base_configuration import configspec


@configspec
class MyPluginConfig(PluginConfig):
    second_name: str

class MyPlugin(CallbackPlugin):

    NAME: str = "my_plugin"
    SPEC = MyPluginConfig

    def filter_row(self, table_name: str, item: TDataItem, **kwargs: Any) -> TDataItem:
        print(item)
        item["new_value"] = self.config.second_name
        return item

os.environ["PLUGIN__MY_PLUGIN__SECOND_NAME"] = "some_new_value"

# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='player_data',
    plugins=[MyPlugin],
)
# Grab some player data from Chess.com API
data = []
for player in ['magnuscarlsen', 'rpragchess']:
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    data.append(response.json())
# Extract, normalize, and load the data
load_info = pipeline.run(data, table_name='player')
print(load_info)