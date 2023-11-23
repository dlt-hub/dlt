from tests.pipeline.utils import assert_load_info


def intro_snippet() -> None:
    # @@@DLT_SNIPPET_START index
    import dlt
    from dlt.sources.helpers import requests

    # Create a dlt pipeline that will load
    # chess player data to the DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline", destination="duckdb", dataset_name="player_data"
    )
    # Grab some player data from Chess.com API
    data = []
    for player in ["magnuscarlsen", "rpragchess"]:
        response = requests.get(f"https://api.chess.com/pub/player/{player}")
        response.raise_for_status()
        data.append(response.json())
    # Extract, normalize, and load the data
    load_info = pipeline.run(data, table_name="player")
    # @@@DLT_SNIPPET_END index

    assert_load_info(load_info)
