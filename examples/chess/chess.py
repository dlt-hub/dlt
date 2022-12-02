from typing import Any, Iterator

import dlt
import requests

from dlt.common.typing import TDataItems


@dlt.source
def chess(chess_url: str = dlt.config.value, title: str = "GM", max_players: int = 2, year: int = 2022, month: int = 10) -> Any:

    @dlt.resource(write_disposition="replace")
    def players() -> Iterator[TDataItems]:
        # https://api.chess.com/pub/titled/{title-abbrev}
        r = requests.get(f"{chess_url}titled/{title}")
        r.raise_for_status()
        # return players one by one, you could also return a list that would be faster but there's more code
        for p in r.json()["players"][:max_players]:
            yield p

    # this resource takes data from players and returns profiles
    @dlt.transformer(data_from=players, write_disposition="replace")
    def player_profile(username: Any) -> Iterator[TDataItems]:
        r = requests.get(f"{chess_url}player/{username}")
        r.raise_for_status()
        yield r.json()

    # this resource takes data from players and returns games for the last month if not specified otherwise
    @dlt.transformer(data_from=players, write_disposition="append")
    def player_games(username: Any) -> Iterator[TDataItems]:
        # https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}
        r = requests.get(f"{chess_url}player/{username}/games/{year:04d}/{month:02d}")
        r.raise_for_status()
        yield r.json()["games"]

    return players(), player_profile, player_games

print("You must run this from the examples/chess folder")
# chess_url in config.toml, credentials for postgres in secrets.toml, credentials always under credentials key
# mind the full_refresh: it makes the pipeline to load to a distinct dataset each time it is run and always is resetting the schema and state
info = dlt.pipeline(
    destination="postgres",
    dataset_name="chess",
    full_refresh=True
).run(
    chess(max_players=5, month=9)
)
# display where the data went
print(info)