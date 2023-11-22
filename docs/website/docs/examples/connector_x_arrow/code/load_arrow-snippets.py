def connector_x_snippet() -> None:
    # @@@DLT_SNIPPET_START markdown_source

    import connectorx as cx

    import dlt
    from dlt.sources.credentials import ConnectionStringCredentials

    def read_sql_x(
        conn_str: ConnectionStringCredentials = dlt.secrets.value, query: str = dlt.config.value
    ):
        yield cx.read_sql(
            conn_str.to_native_representation(), query, return_type="arrow2", protocol="binary"
        )

    # create genome resource with merge on `upid` primary key
    genome = dlt.resource(
        name="genome", write_disposition="merge", primary_key="upid", standalone=True
    )(read_sql_x)(
        "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",  # type: ignore[arg-type]
        "SELECT * FROM genome ORDER BY created LIMIT 1000",
    )
    # add incremental on created at
    genome.apply_hints(incremental=dlt.sources.incremental("created"))
    # @@@DLT_SNIPPET_END markdown_source
    # @@@DLT_SNIPPET_START markdown_pipeline
    __name__ = "__main__"  # @@@DLT_REMOVE
    if __name__ == "__main__":
        pipeline = dlt.pipeline(destination="duckdb")
        print(pipeline.run(genome))
        print(pipeline.last_trace.last_normalize_info)
        # NOTE: run pipeline again to see that no more records got loaded thanks to incremental working
    # @@@DLT_SNIPPET_END markdown_pipeline

    # check that stuff was loaded
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["genome"] == 1000
