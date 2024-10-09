"""The Requests Pipeline Template provides a simple starting point for a dlt pipeline with the requests library"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Iterator, Any

import dlt

from dlt.sources.helpers import requests
from dlt.sources import TDataItems


YEAR = 2022
MONTH = 10
BASE_PATH = "https://api.chess.com/pub/player"


@dlt.resource(primary_key="player_id")
def players():
    """Load player profiles from the chess api."""
    for player_name in ["magnuscarlsen", "rpragchess"]:
        path = f"{BASE_PATH}/{player_name}"
        response = requests.get(path)
        response.raise_for_status()
        yield response.json()


# this resource takes data from players and returns games for the configured
@dlt.transformer(data_from=players, write_disposition="append")
def players_games(player: Any) -> Iterator[TDataItems]:
    """Load all games for each player in october 2022"""
    player_name = player["username"]
    path = f"{BASE_PATH}/{player_name}/games/{YEAR:04d}/{MONTH:02d}"
    response = requests.get(path)
    response.raise_for_status()
    yield response.json()["games"]


@dlt.source(name="chess")
def chess():
    """A source function groups all resources into one schema."""
    return players(), players_games()


def load_chess_data() -> None:
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name="chess",
        destination="duckdb",
        dataset_name="chess_data",
    )

    load_info = p.run(chess())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_chess_data()
