"""Test the duckdb supported sql client for special internal features"""


from typing import Optional

import pytest
import dlt
import os
import shutil


from dlt import Pipeline
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TTableFormat

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    GCS_BUCKET,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)
from dlt.destinations import filesystem
from tests.utils import TEST_STORAGE_ROOT
from tests.cases import arrow_table_all_data_types
from dlt.destinations.exceptions import DatabaseUndefinedRelation


@pytest.fixture(scope="function", autouse=True)
def secret_directory():
    secrets_dir = f"{TEST_STORAGE_ROOT}/duck_secrets_{uniq_id()}"
    yield secrets_dir
    shutil.rmtree(secrets_dir, ignore_errors=True)


def _run_dataset_checks(
    pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    secret_directory: str,
    table_format: Optional[TTableFormat] = None,
) -> None:
    total_records = 200

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

        @dlt.resource(table_format=table_format)
        def arrow_all_types():
            yield arrow_table_all_data_types("arrow-table", num_rows=total_records)[0]

        return [items, double_items, arrow_all_types]

    # run source
    pipeline.run(source(), loader_file_format=destination_config.file_format)

    import duckdb
    from duckdb import HTTPException, IOException, InvalidInputException
    from dlt.destinations.impl.filesystem.sql_client import (
        FilesystemSqlClient,
        DuckDbCredentials,
    )

    with pipeline.sql_client() as c:
        # check if all data types are handled properly
        c.execute_sql("SELECT * FROM arrow_all_types;")

        # check we can create new tables from the views
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
        try:
            c.execute_sql("INSERT INTO double_items VALUES (1, 2)")
        except Exception as exc:
            assert "double_items is not an table" in str(exc)

        # check that no automated views are created for a schema different than
        # the known one
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
    # duckdb will store secrets lower case, that's why we could not delete it
    TEST_SECRET_NAME = "second_" + uniq_id()

    def _external_duckdb_connection() -> duckdb.DuckDBPyConnection:
        external_db = duckdb.connect(duck_db_location)
        # the line below solves problems with certificate path lookup on linux, see duckdb docs
        external_db.sql("SET azure_transport_option_type = 'curl';")
        external_db.sql(f"SET secret_directory = '{secret_directory}';")
        if table_format == "iceberg":
            FilesystemSqlClient._setup_iceberg(external_db)
        return external_db

    def _fs_sql_client_for_external_db(
        connection: duckdb.DuckDBPyConnection,
        persist_secrets: bool = False,
    ) -> FilesystemSqlClient:
        return FilesystemSqlClient(
            dataset_name="second",
            remote_client=pipeline.destination_client(),  #  type: ignore
            cache_db=DuckDbCredentials(connection),
            persist_secrets=persist_secrets,
        )

    # we create a duckdb with a table an see whether we can add more views from the fs client
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

    # test if view reflects source table accurately after it has changed
    # concretely, this tests if an existing view is replaced with formats that need it, such as
    # `iceberg` table format
    with fs_sql_client as sql_client:
        sql_client.create_views_for_tables({"arrow_all_types": "arrow_all_types"})
    assert external_db.sql("FROM second.arrow_all_types;").arrow().num_rows == total_records

    pipeline.run(  # run pipeline again to add rows to source table
        source().with_resources("arrow_all_types"),
        loader_file_format=destination_config.file_format,
    )
    # and recreate views because autorefresh is not enabled by default
    with fs_sql_client as sql_client:
        sql_client.create_view(
            "arrow_all_types", pipeline.default_schema.get_table("arrow_all_types")  # type: ignore
        )
    assert external_db.sql("FROM second.arrow_all_types;").arrow().num_rows == (2 * total_records)

    external_db.close()

    # in case we are not connecting to a bucket that needs secrets, views should still be here after connection reopen
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

    # gs does not support persistent secrets, so we can't do further checks
    if unsupported_persistent_secrets:
        return

    # create secret
    external_db = _external_duckdb_connection()
    fs_sql_client = _fs_sql_client_for_external_db(external_db, persist_secrets=True)

    try:
        with fs_sql_client as sql_client:
            # remove all previous secrets
            for secret_name in fs_sql_client.list_secrets():
                fs_sql_client.drop_secret(secret_name)

            fs_sql_client.create_secret(
                fs_sql_client.remote_client.config.bucket_url,
                fs_sql_client.remote_client.config.credentials,
                secret_name=TEST_SECRET_NAME,
            )
        external_db.close()

        # now this should work
        external_db = _external_duckdb_connection()
        assert (
            len(external_db.sql("SELECT * FROM second.referenced_items").fetchall())
            == total_records
        )

        # now drop the secrets again
        fs_sql_client = _fs_sql_client_for_external_db(external_db)
        with fs_sql_client as sql_client:
            fs_sql_client.drop_secret(TEST_SECRET_NAME)
        external_db.close()

        # fails again
        external_db = _external_duckdb_connection()
        with pytest.raises((HTTPException, IOException, InvalidInputException)):
            assert (
                len(external_db.sql("SELECT * FROM second.referenced_items").fetchall())
                == total_records
            )
        external_db.close()
    except Exception:
        with duckdb.connect() as conn:
            fs_sql_client = _fs_sql_client_for_external_db(conn)
            with fs_sql_client as sql_client:
                fs_sql_client.drop_secret(TEST_SECRET_NAME)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_local_configs=True,
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
    ),  # TODO: make SFTP work
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("disable_compression", [True, False])
def test_read_interfaces_filesystem(
    destination_config: DestinationTestConfiguration,
    disable_compression: bool,
    secret_directory: str,
) -> None:
    # we force multiple files per table, they may only hold 700 items
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"
    destination_config.disable_compression = disable_compression
    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )
    if destination_config.file_format in ["parquet"] and disable_compression:
        pytest.skip("Disabling compression for parquet has no effect, skipping test")

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="test_read_interfaces_filesystem",
        dev_mode=True,
    )

    _run_dataset_checks(pipeline, destination_config, secret_directory=secret_directory)

    # for gcs buckets we additionally test the s3 compat layer
    if destination_config.bucket_url == GCS_BUCKET:
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        pipeline = destination_config.setup_pipeline(
            "read_pipeline",
            dataset_name="test_read_interfaces_filesystem_gcs",
            dev_mode=True,
            destination=gcp_bucket,
        )
        _run_dataset_checks(pipeline, destination_config, secret_directory=secret_directory)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_local_configs=True,
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
    ),  # TODO: make SFTP work
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("autorefresh", (True, False))
def test_auto_refresh_views(
    destination_config: DestinationTestConfiguration, autorefresh: bool
) -> None:
    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="test_auto_refresh_views",
        dev_mode=True,
    )
    # set autorefresh
    pipeline.destination.config_params["always_refresh_views"] = autorefresh
    pipeline.run(
        [1, 2, 3],
        table_name="digits",
        table_format=destination_config.table_format,
        loader_file_format=destination_config.file_format,
    )

    # get dataset and keep connection
    with pipeline.dataset() as ds_:
        digits = ds_.digits
        # this also creates view
        assert len(digits.fetchall()) == 3

        # do not close dataset to not remove views and run again
        pipeline.run(
            [7, 8],
            table_name="digits",
            table_format=destination_config.table_format,
            loader_file_format=destination_config.file_format,
        )

        should_refresh = autorefresh or destination_config.table_format == "delta"
        assert len(digits.fetchall()) == 5 if should_refresh else 3


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
    ),
    ids=lambda x: x.name,
)
def test_table_formats(
    destination_config: DestinationTestConfiguration, secret_directory: str
) -> None:
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="test_table_formats",
        dev_mode=True,
    )

    _run_dataset_checks(
        pipeline,
        destination_config,
        secret_directory=secret_directory,
        table_format=destination_config.table_format,
    )


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(local_filesystem_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("disable_compression", [True, False])
def test_evolving_filesystem(
    destination_config: DestinationTestConfiguration, disable_compression: bool
) -> None:
    """test that files with unequal schemas still work together"""
    destination_config.disable_compression = disable_compression
    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )

    @dlt.resource(table_name="items", file_format=destination_config.file_format)
    def items():
        yield from [{"id": i} for i in range(20)]

    @dlt.resource(file_format="csv")
    def csv_items():
        yield from [{"id": i} for i in range(20)]

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="test_evolving_filesystem",
        dev_mode=True,
    )

    pipeline.run([items(), csv_items()])

    df = pipeline.dataset().items.df()
    assert len(df.index) == 20

    assert pipeline.default_schema.get_table("csv_items")["file_format"] == "csv"

    assert len(pipeline.dataset().csv_items.fetchall()) == 20

    @dlt.resource(table_name="items", file_format=destination_config.file_format)
    def items2():
        yield from [{"id": i, "other_value": "Blah"} for i in range(20, 50)]

    pipeline.run([items2()])

    # check df and arrow access
    assert len(pipeline.dataset().items.df().index) == 50
    assert pipeline.dataset().items.arrow().num_rows == 50
