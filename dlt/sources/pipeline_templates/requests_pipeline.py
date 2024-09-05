"""The Requests Pipeline Template provides a simple starting point for a dlt pipeline with the requests library"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Iterator, Any

import dlt

from dlt.sources import TDataItems
from dlt.sources.helpers import requests


YEAR = 2022
MONTH = 10


@dlt.source(name="my_fruitshop")
def source():
    """A source function groups all resources into one schema."""
    return players(), players_games()


@dlt.resource(name="players", primary_key="player_id")
def players():
    """Load player profiles from the chess api."""
    for player_name in ["magnuscarlsen", "rpragchess"]:
        yield requests.get(f"https://api.chess.com/pub/player/{player_name}").json()


# this resource takes data from players and returns games for the configured
@dlt.transformer(data_from=players, write_disposition="append")
def players_games(player: Any) -> Iterator[TDataItems]:
    """Load all games for each player in october 2022"""
    player_name = player["username"]
    path = f"https://api.chess.com/pub/player/{player_name}/games/{YEAR:04d}/{MONTH:02d}"
    yield requests.get(path).json()["games"]


def load_chess_data() -> None:
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name="fruitshop",
        destination="duckdb",
        dataset_name="fruitshop_data",
    )

    load_info = p.run(source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_chess_data()
