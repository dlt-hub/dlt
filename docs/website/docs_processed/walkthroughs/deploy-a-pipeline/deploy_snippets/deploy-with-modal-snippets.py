from tests.pipeline.utils import assert_load_info


def test_modal_snippet() -> None:
    # @@@DLT_SNIPPET_START modal_image
    import modal

    # Define the Modal Image
    image = modal.Image.debian_slim().pip_install(
        "dlt>=1.1.0",
        "dlt[duckdb]",  # destination
        "dlt[sql_database]",  # source (MySQL)
        "dlt[parquet]",  # file format dependency
        "pymysql",  # database driver for MySQL source
    )

    app = modal.App("example-dlt", image=image)

    # Modal Volume used to store the duckdb database file
    vol = modal.Volume.from_name("duckdb-vol", create_if_missing=True)
    # @@@DLT_SNIPPET_END modal_image

    # @@@DLT_SNIPPET_START modal_function
    @app.function(volumes={"/data/": vol}, schedule=modal.Period(days=1), serialized=True)
    def load_tables() -> None:
        import dlt
        import os
        from dlt.sources.sql_database import sql_database

        # Define the source database credentials; in production, you would save this as a Modal Secret which can be referenced here as an environment variable
        os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"] = (
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
        )
        # Load tables "family" and "genome" with minimal reflection to avoid column constraint error
        source = sql_database(reflection_level="minimal").with_resources("family", "genome")

        # Create dlt pipeline object
        pipeline = dlt.pipeline(
            pipeline_name="sql_to_duckdb_pipeline",
            destination=dlt.destinations.duckdb(
                "/data/rfam.duckdb"
            ),  # write the duckdb database file to this file location, which will get mounted to the Modal Volume
            dataset_name="sql_to_duckdb_pipeline_data",
            progress="log",  # output progress of the pipeline
        )

        # Run the pipeline
        load_info = pipeline.run(source, write_disposition="replace")

        # Print run statistics
        print(load_info)
        # @@@DLT_SNIPPET_END modal_function

        assert_load_info(load_info)

    import pytest
    from modal.exception import ExecutionError

    # Any additional logic or calling the function
    with pytest.raises(ExecutionError) as excinfo:
        load_tables.remote()
    # >>  modal.exception.ExecutionError:
    # >>  Function has not been hydrated with the metadata it needs to run on Modal, because the App it is defined on is not running.
    assert "hydrated" in str(excinfo.value)
