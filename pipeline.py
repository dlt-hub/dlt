import dlt
from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests
from dlt.common.plugins import register_plugin, Plugin


class MyPlugin(Plugin):

    def on_step_start(self):
        print("Started step " + self.step)

    def on_step_end(self):
        print("Ended step " + self.step)

    def on_schema_contract_violation(self, table: str, violating_item: TDataItem, **kwargs):
        print("Violation!")

    def on_extractor_item_written(self, item: TDataItem, **kwargs):
        print(f"Written item {item} in step {self.step}")

register_plugin(MyPlugin)

# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='player_data'
)
# Grab some player data from Chess.com API
data = []
for player in ['magnuscarlsen', 'rpragchess']:
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    data.append(response.json())
# Extract, normalize, and load the data
load_info = pipeline.run(data, table_name='player')