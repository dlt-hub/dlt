import dlt, os
from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests
from dlt.common.plugins import Plugin, PluginConfig
from dlt.common.configuration.specs.base_configuration import configspec


@configspec
class MyPluginConfig(PluginConfig):
    second_name: str

class MyPlugin(Plugin):

    NAME: str = "my_plugin"
    SPEC = MyPluginConfig

    def on_step_start(self, step: str):
        print("Started step " + self.step)
        print("with config " + self.config.second_name)

    def on_step_end(self, step: str):
        print("Ended step " + self.step)

    def on_schema_contract_violation(self, table: str, violating_item: TDataItem, **kwargs):
        print("Violation!")

    def on_extractor_item_written(self, item: TDataItem, **kwargs):
        print(f"Written item {item} in step {self.step}")


os.environ["PLUGIN__MY_PLUGIN__SECOND_NAME"] = "second_name"

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