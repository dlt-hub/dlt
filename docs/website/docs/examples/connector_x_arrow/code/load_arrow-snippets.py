def connector_x_snippet() -> None:
    # @@@DLT_SNIPPET_START example
    # @@@DLT_SNIPPET_START markdown_source
    import connectorx as cx

    import dlt
    from dlt.sources.credentials import ConnectionStringCredentials

    def read_sql_x(
        conn_str: ConnectionStringCredentials = dlt.secrets.value,
        query: str = dlt.config.value,
    ):
        yield cx.read_sql(
            conn_str.to_native_representation(),
            query,
            return_type="arrow2",
            protocol="binary",
        )

    def genome_resource():
        # create genome resource with merge on `upid` primary key
        genome = dlt.resource(
            name="acanthochromis_polyacanthus",
            write_disposition="merge",
            primary_key="analysis_id",
            standalone=True,
        )(read_sql_x)(
            "mysql://anonymous@ensembldb.ensembl.org:3306/acanthochromis_polyacanthus_core_100_1",  # type: ignore[arg-type]
            "SELECT * FROM analysis LIMIT 20",
        )
        # add incremental on created at
        genome.apply_hints(incremental=dlt.sources.incremental("created"))
        return genome

    # @@@DLT_SNIPPET_END markdown_source

    # @@@DLT_SNIPPET_START markdown_pipeline
    __name__ = "__main__"  # @@@DLT_REMOVE
    if __name__ == "__main__":
        pipeline = dlt.pipeline(destination="duckdb")
        genome = genome_resource()

        print(pipeline.run(genome))
        print(pipeline.last_trace.last_normalize_info)
        # NOTE: run pipeline again to see that no more records got loaded thanks to incremental loading
    # @@@DLT_SNIPPET_END markdown_pipeline

    # check that stuff was loaded # @@@DLT_REMOVE
    row_counts = pipeline.last_trace.last_normalize_info.row_counts  # @@@DLT_REMOVE
    assert row_counts["acanthochromis_polyacanthus"] == 20  # @@@DLT_REMOVE

    # @@@DLT_SNIPPET_END example
