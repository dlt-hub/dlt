from tests.utils import skipifgithubfork


@skipifgithubfork
def incremental_snippet() -> None:

    # @@@DLT_SNIPPET_START example
    # @@@DLT_SNIPPET_START markdown_source
    import threading
    from typing import Any, Iterator

    import dlt

    from dlt.common import sleep
    from dlt.common.typing import StrAny, TDataItems
    from dlt.sources.helpers.requests import client

    @dlt.source
    def chess(chess_url: str = dlt.config.value, title: str = "GM", max_players: int = 2, year: int = 2022, month: int = 10) -> Any:

        def _get_data_with_retry(path: str) -> StrAny:
            r = client.get(f"{chess_url}{path}")
            return r.json()  # type: ignore

        @dlt.resource(write_disposition="replace")
        def players() -> Iterator[TDataItems]:
            # return players one by one, you could also return a list that would be faster but we want to pass players item by item to the transformer
            yield from _get_data_with_retry(f"titled/{title}")["players"][:max_players]

        # this resource takes data from players and returns profiles
        # it uses `defer` decorator to enable parallel run in thread pool. defer requires return at the end so we convert yield into return (we return one item anyway)
        # you can still have yielding transformers, look for the test named `test_evolve_schema`
        @dlt.transformer(data_from=players, write_disposition="replace")
        @dlt.defer
        def players_profiles(username: Any) -> TDataItems:
            print(f"getting {username} profile via thread {threading.current_thread().name}")
            sleep(1)  # add some latency to show parallel runs
            return _get_data_with_retry(f"player/{username}")

        # this resource takes data from players and returns games for the last month if not specified otherwise
        @dlt.transformer(data_from=players, write_disposition="append")
        def players_games(username: Any) -> Iterator[TDataItems]:
            # https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}
            path = f"player/{username}/games/{year:04d}/{month:02d}"
            yield _get_data_with_retry(path)["games"]

        return players(), players_profiles, players_games

    # @@@DLT_SNIPPET_END markdown_source
    # @@@DLT_SNIPPET_START markdown_pipeline
    __name__ = "__main__" # @@@DLT_REMOVE
    if __name__ == "__main__":
        # create dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name="chess_pipeline", destination="duckdb", dataset_name="chess_data"
        )
        max_players = 5
        load_info = pipeline.run(chess(chess_url="https://api.chess.com/pub/", max_players=max_players))
        print(load_info)
    # @@@DLT_SNIPPET_END markdown_pipeline
    # @@@DLT_SNIPPET_END example

    # check that stuff was loaded
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["players"] == max_players

