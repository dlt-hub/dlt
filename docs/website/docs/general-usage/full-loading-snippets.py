def check_replace_strategies_snippet() -> None:
    # @@@DLT_SNIPPET_START check_replace_strategies
    import dlt
    from dlt.common.destination.typing import PreparedTableSchema
    from dlt.common.destination.utils import resolve_replace_strategy

    pipeline = dlt.pipeline("replace_strategies", destination="filesystem")
    caps = pipeline.destination.capabilities()

    # all strategies this destination may use
    print(caps.supported_replace_strategies)

    # strategy that dlt uses for a given table when you do not configure one
    regular_table: PreparedTableSchema = {"name": "items"}
    delta_table: PreparedTableSchema = {"name": "items", "table_format": "delta"}
    print(resolve_replace_strategy(regular_table, None, caps))
    print(resolve_replace_strategy(delta_table, None, caps))

    # None means the requested strategy is not available for that table
    print(resolve_replace_strategy(regular_table, "insert-from-staging", caps))
    # @@@DLT_SNIPPET_END check_replace_strategies

    assert "truncate-and-insert" in caps.supported_replace_strategies
    assert resolve_replace_strategy(regular_table, None, caps) == "truncate-and-insert"
    assert resolve_replace_strategy(delta_table, None, caps) == "insert-from-staging"
    assert resolve_replace_strategy(regular_table, "insert-from-staging", caps) is None


def set_replace_strategy_snippet() -> None:
    # @@@DLT_SNIPPET_START set_replace_strategy
    import dlt

    # configure the strategy on the destination of a single pipeline
    pipeline = dlt.pipeline(
        "github_issues",
        destination=dlt.destinations.duckdb(replace_strategy="insert-from-staging"),
        dataset_name="github",
    )
    # @@@DLT_SNIPPET_END set_replace_strategy

    config = pipeline.destination_client().config
    assert config.replace_strategy == "insert-from-staging"  # type: ignore[attr-defined]
