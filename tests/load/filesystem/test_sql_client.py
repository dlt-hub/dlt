"""Test the duckdb supported sql client for special internal features"""


from typing import Any

import pytest
import dlt
import os

from dlt import Pipeline
from dlt.common.utils import uniq_id

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    GCS_BUCKET,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)
from dlt.destinations import filesystem
from tests.utils import TEST_STORAGE_ROOT
from dlt.destinations.exceptions import DatabaseUndefinedRelation


def _run_dataset_checks(
    pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    table_format: Any = None,
    alternate_access_pipeline: Pipeline = None,
) -> None:
    total_records = 200

    TEST_SECRET_NAME = "TEST_SECRET" + uniq_id()

    # only some buckets have support for persistent secrets
    needs_persistent_secrets = (
        destination_config.bucket_url.startswith("s3")
        or destination_config.bucket_url.startswith("az")
        or destination_config.bucket_url.startswith("abfss")
    )

    unsupported_persistent_secrets = destination_config.bucket_url.startswith("gs")

    @dlt.source()
    def source():
        @dlt.resource(
            table_format=table_format,
            write_disposition="replace",
        )
        def items():
            yield from [
                {
                    "id": i,
                    "children": [{"id": i + 100}, {"id": i + 1000}],
                }
                for i in range(total_records)
            ]

        @dlt.resource(
            table_format=table_format,
            write_disposition="replace",
        )
        def double_items():
            yield from [
                {
                    "id": i,
                    "double_id": i * 2,
                }
                for i in range(total_records)
            ]

        return [items, double_items]

    # run source
    pipeline.run(source(), loader_file_format=destination_config.file_format)

    if alternate_access_pipeline:
        pipeline.destination = alternate_access_pipeline.destination

    import duckdb
    from duckdb import HTTPException, IOException, InvalidInputException
    from dlt.destinations.impl.filesystem.sql_client import (
        FilesystemSqlClient,
        DuckDbCredentials,
    )

    # check we can create new tables from the views
    with pipeline.sql_client() as c:
        c.execute_sql(
            "CREATE TABLE items_joined AS (SELECT i.id, di.double_id FROM items as i JOIN"
            " double_items as di ON (i.id = di.id));"
        )
        with c.execute_query("SELECT * FROM items_joined ORDER BY id ASC;") as cursor:
            joined_table = cursor.fetchall()
            assert len(joined_table) == total_records
            assert list(joined_table[0]) == [0, 0]
            assert list(joined_table[5]) == [5, 10]
            assert list(joined_table[10]) == [10, 20]

    # inserting values into a view should fail gracefully
    with pipeline.sql_client() as c:
        try:
            c.execute_sql("INSERT INTO double_items VALUES (1, 2)")
        except Exception as exc:
            assert "double_items is not an table" in str(exc)

    # check that no automated views are created for a schema different than
    # the known one
    with pipeline.sql_client() as c:
        c.execute_sql("CREATE SCHEMA other_schema;")
        with pytest.raises(DatabaseUndefinedRelation):
            with c.execute_query("SELECT * FROM other_schema.items ORDER BY id ASC;") as cursor:
                pass
        # correct dataset view works
        with c.execute_query(f"SELECT * FROM {c.dataset_name}.items ORDER BY id ASC;") as cursor:
            table = cursor.fetchall()
            assert len(table) == total_records
        # no dataset prefix works
        with c.execute_query("SELECT * FROM items ORDER BY id ASC;") as cursor:
            table = cursor.fetchall()
            assert len(table) == total_records

    #
    # tests with external duckdb instance
    #

    duck_db_location = TEST_STORAGE_ROOT + "/" + uniq_id()

    def _external_duckdb_connection() -> duckdb.DuckDBPyConnection:
        external_db = duckdb.connect(duck_db_location)
        # the line below solves problems with certificate path lookup on linux, see duckdb docs
        external_db.sql("SET azure_transport_option_type = 'curl';")
        return external_db

    def _fs_sql_client_for_external_db(
        connection: duckdb.DuckDBPyConnection,
    ) -> FilesystemSqlClient:
        return FilesystemSqlClient(
            dataset_name="second",
            fs_client=pipeline.destination_client(),  #  type: ignore
            credentials=DuckDbCredentials(connection),
        )

    # we create a duckdb with a table an see wether we can add more views from the fs client
    external_db = _external_duckdb_connection()
    external_db.execute("CREATE SCHEMA first;")
    external_db.execute("CREATE SCHEMA second;")
    external_db.execute("CREATE TABLE first.items AS SELECT i FROM range(0, 3) t(i)")
    assert len(external_db.sql("SELECT * FROM first.items").fetchall()) == 3

    fs_sql_client = _fs_sql_client_for_external_db(external_db)
    with fs_sql_client as sql_client:
        sql_client.create_views_for_tables(
            {"items": "referenced_items", "_dlt_loads": "_dlt_loads"}
        )

    # views exist
    assert len(external_db.sql("SELECT * FROM second.referenced_items").fetchall()) == total_records
    assert len(external_db.sql("SELECT * FROM first.items").fetchall()) == 3
    external_db.close()

    # in case we are not connecting to a bucket, views should still be here after connection reopen
    if not needs_persistent_secrets and not unsupported_persistent_secrets:
        external_db = _external_duckdb_connection()
        assert (
            len(external_db.sql("SELECT * FROM second.referenced_items").fetchall())
            == total_records
        )
        external_db.close()
        return

    # in other cases secrets are not available and this should fail
    external_db = _external_duckdb_connection()
    with pytest.raises((HTTPException, IOException, InvalidInputException)):
        assert (
            len(external_db.sql("SELECT * FROM second.referenced_items").fetchall())
            == total_records
        )
    external_db.close()

    # gs does not support persistent secrest, so we can't do further checks
    if unsupported_persistent_secrets:
        return

    # create secret
    external_db = _external_duckdb_connection()
    fs_sql_client = _fs_sql_client_for_external_db(external_db)
    with fs_sql_client as sql_client:
        fs_sql_client.create_authentication(persistent=True, secret_name=TEST_SECRET_NAME)
    external_db.close()

    # now this should work
    external_db = _external_duckdb_connection()
    assert len(external_db.sql("SELECT * FROM second.referenced_items").fetchall()) == total_records

    # NOTE: when running this on CI, there seem to be some kind of race conditions that prevent
    # secrets from being removed as it does not find the file... We'll need to investigate this.
    return

    # now drop the secrets again
    fs_sql_client = _fs_sql_client_for_external_db(external_db)
    with fs_sql_client as sql_client:
        fs_sql_client.drop_authentication(TEST_SECRET_NAME)
    external_db.close()

    # fails again
    external_db = _external_duckdb_connection()
    with pytest.raises((HTTPException, IOException, InvalidInputException)):
        assert (
            len(external_db.sql("SELECT * FROM second.referenced_items").fetchall())
            == total_records
        )
    external_db.close()


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
    ),  # TODO: make SFTP work
    ids=lambda x: x.name,
)
def test_read_interfaces_filesystem(destination_config: DestinationTestConfiguration) -> None:
    # we force multiple files per table, they may only hold 700 items
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"

    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
        dev_mode=True,
    )

    _run_dataset_checks(pipeline, destination_config)

    # for gcs buckets we additionally test the s3 compat layer
    if destination_config.bucket_url == GCS_BUCKET:
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        pipeline = destination_config.setup_pipeline(
            "read_pipeline", dataset_name="read_test", dev_mode=True, destination=gcp_bucket
        )
        _run_dataset_checks(pipeline, destination_config)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
        # NOTE: delta does not work on memory buckets
    ),
    ids=lambda x: x.name,
)
def test_delta_tables(destination_config: DestinationTestConfiguration) -> None:
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
    )

    # in case of gcs we use the s3 compat layer for reading
    # for writing we still need to use the gc authentication, as delta_rs seems to use
    # methods on the s3 interface that are not implemented by gcs
    access_pipeline = pipeline
    if destination_config.bucket_url == GCS_BUCKET:
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        access_pipeline = destination_config.setup_pipeline(
            "read_pipeline", dataset_name="read_test", destination=gcp_bucket
        )

    _run_dataset_checks(
        pipeline,
        destination_config,
        table_format="delta",
        alternate_access_pipeline=access_pipeline,
    )


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_evolving_filesystem(destination_config: DestinationTestConfiguration) -> None:
    """test that files with unequal schemas still work together"""

    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )

    @dlt.resource(table_name="items")
    def items():
        yield from [{"id": i} for i in range(20)]

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
        dev_mode=True,
    )

    pipeline.run([items()], loader_file_format=destination_config.file_format)

    df = pipeline._dataset().items.df()
    assert len(df.index) == 20

    @dlt.resource(table_name="items")
    def items2():
        yield from [{"id": i, "other_value": "Blah"} for i in range(20, 50)]

    pipeline.run([items2()], loader_file_format=destination_config.file_format)

    # check df and arrow access
    assert len(pipeline._dataset().items.df().index) == 50
    assert pipeline._dataset().items.arrow().num_rows == 50
