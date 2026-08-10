def replace_strategies_snippet() -> None:
    # @@@DLT_SNIPPET_START supported_replace_strategies
    import dlt

    pipeline = dlt.pipeline("github", destination="duckdb", dataset_name="github")
    caps = pipeline.destination.capabilities()
    print(caps.supported_replace_strategies)
    # @@@DLT_SNIPPET_END supported_replace_strategies

    assert caps.supported_replace_strategies[0] == "truncate-and-insert"

    # @@@DLT_SNIPPET_START replace_strategies_selector
    from dlt.destinations import filesystem

    fs_caps = filesystem().capabilities()
    select_strategies = fs_caps.replace_strategies_selector
    # a regular table
    print(
        select_strategies(
            fs_caps.supported_replace_strategies, table_schema={"name": "items"}
        )
    )
    # the same table in the delta table format
    print(
        select_strategies(
            fs_caps.supported_replace_strategies,
            table_schema={"name": "items", "table_format": "delta"},
        )
    )
    # @@@DLT_SNIPPET_END replace_strategies_selector

    assert select_strategies(
        fs_caps.supported_replace_strategies, table_schema={"name": "items"}
    ) == ["truncate-and-insert"]
    assert select_strategies(
        fs_caps.supported_replace_strategies,
        table_schema={"name": "items", "table_format": "delta"},
    ) == ["insert-from-staging"]
