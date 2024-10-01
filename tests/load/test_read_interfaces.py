from typing import Any

import pytest
import dlt
import os

from dlt import Pipeline
from dlt.common import Decimal
from dlt.common.utils import uniq_id

from typing import List
from functools import reduce

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    GCS_BUCKET,
    SFTP_BUCKET,
    FILE_BUCKET,
    MEMORY_BUCKET,
)
from dlt.destinations import filesystem


def _run_dataset_checks(
    pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    table_format: Any = None,
    alternate_access_pipeline: Pipeline = None,
) -> None:
    destination_type = pipeline.destination_client().config.destination_type

    skip_df_chunk_size_check = False
    expected_columns = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]
    if destination_type == "bigquery":
        chunk_size = 50
        total_records = 80
    elif destination_type == "mssql":
        chunk_size = 700
        total_records = 1000
    else:
        chunk_size = 2048
        total_records = 3000

    # on filesystem one chunk is one file and not the default vector size
    if destination_type == "filesystem":
        skip_df_chunk_size_check = True

    # we always expect 2 chunks based on the above setup
    expected_chunk_counts = [chunk_size, total_records - chunk_size]

    @dlt.source()
    def source():
        @dlt.resource(
            table_format=table_format,
            columns={
                "id": {"data_type": "bigint"},
                # we add a decimal with precision to see wether the hints are preserved
                "decimal": {"data_type": "decimal", "precision": 10, "scale": 3},
                "other_decimal": {"data_type": "decimal", "precision": 12, "scale": 3},
            },
        )
        def items():
            yield from [
                {
                    "id": i,
                    "children": [{"id": i + 100}, {"id": i + 1000}],
                    "decimal": Decimal("10.433"),
                    "other_decimal": Decimal("10.433"),
                }
                for i in range(total_records)
            ]

        @dlt.resource(
            table_format=table_format,
            columns={
                "id": {"data_type": "bigint"},
                "double_id": {"data_type": "bigint"},
            },
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
    s = source()
    pipeline.run(s, loader_file_format=destination_config.file_format)

    if alternate_access_pipeline:
        pipeline = alternate_access_pipeline

    # access via key
    table_relationship = pipeline._dataset()["items"]

    # full frame
    df = table_relationship.df()
    assert len(df.index) == total_records

    #
    # check dataframes
    #

    # chunk
    df = table_relationship.df(chunk_size=chunk_size)
    if not skip_df_chunk_size_check:
        assert len(df.index) == chunk_size
    # lowercase results for the snowflake case
    assert set(df.columns.values) == set(expected_columns)

    # iterate all dataframes
    frames = list(table_relationship.iter_df(chunk_size=chunk_size))
    if not skip_df_chunk_size_check:
        assert [len(df.index) for df in frames] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [f[expected_columns[0]].to_list() for f in frames])
    assert set(ids) == set(range(total_records))

    # access via prop
    table_relationship = pipeline._dataset().items

    #
    # check arrow tables
    #

    # full table
    table = table_relationship.arrow()
    assert table.num_rows == total_records

    # chunk
    table = table_relationship.arrow(chunk_size=chunk_size)
    assert set(table.column_names) == set(expected_columns)
    assert table.num_rows == chunk_size

    # check frame amount and items counts
    tables = list(table_relationship.iter_arrow(chunk_size=chunk_size))
    assert [t.num_rows for t in tables] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [t.column(expected_columns[0]).to_pylist() for t in tables])
    assert set(ids) == set(range(total_records))

    # check fetch accessors
    table_relationship = pipeline._dataset().items

    # check accessing one item
    one = table_relationship.fetchone()
    assert one[0] in range(total_records)

    # check fetchall
    fall = table_relationship.fetchall()
    assert len(fall) == total_records
    assert {item[0] for item in fall} == set(range(total_records))

    # check fetchmany
    many = table_relationship.fetchmany(chunk_size)
    assert len(many) == chunk_size

    # check iterfetchmany
    chunks = list(table_relationship.iter_fetch(chunk_size=chunk_size))
    assert [len(chunk) for chunk in chunks] == expected_chunk_counts
    ids = reduce(lambda a, b: a + b, [[item[0] for item in chunk] for chunk in chunks])
    assert set(ids) == set(range(total_records))

    # check that hints are carried over to arrow table
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    if destination_config.destination_type == "bigquery":
        # bigquery does not allow precision configuration..
        expected_decimal_precision = 38
        expected_decimal_precision_2 = 38
    assert (
        table_relationship.arrow().schema.field("decimal").type.precision
        == expected_decimal_precision
    )
    assert (
        table_relationship.arrow().schema.field("other_decimal").type.precision
        == expected_decimal_precision_2
    )

    # simple check that query also works
    tname = pipeline.sql_client().make_qualified_table_name("items")
    query_relationship = pipeline._dataset()(f"select * from {tname} where id < 20")

    # we selected the first 20
    table = query_relationship.arrow()
    assert table.num_rows == 20

    # check join query
    tdname = pipeline.sql_client().make_qualified_table_name("double_items")
    query = (
        f"SELECT i.id, di.double_id FROM {tname} as i JOIN {tdname} as di ON (i.id = di.id) WHERE"
        " i.id < 20 ORDER BY i.id ASC"
    )
    join_relationship = pipeline._dataset()(query)
    table = join_relationship.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]

    # special filesystem sql checks
    if destination_config.destination_type == "filesystem":
        import duckdb
        from dlt.destinations.impl.filesystem.sql_client import FilesystemSqlClient

        # check we can create new tables from the views
        with pipeline.sql_client() as c:
            c.create_views_for_tables({"items": "items", "double_items": "double_items"})
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

        # we create a duckdb with a table an see wether we can add more views
        duck_db_location = "_storage/" + uniq_id()
        external_db = duckdb.connect(duck_db_location)
        external_db.execute("CREATE SCHEMA first;")
        external_db.execute("CREATE SCHEMA second;")
        external_db.execute("CREATE TABLE first.items AS SELECT i FROM range(0, 3) t(i)")
        assert len(external_db.sql("SELECT * FROM first.items").fetchall()) == 3

        # now we can use the filesystemsql client to create the needed views
        fs_client: Any = pipeline.destination_client()
        fs_sql_client = FilesystemSqlClient(
            dataset_name="second",
            fs_client=fs_client,
            duckdb_connection=external_db,
        )
        with fs_sql_client as sql_client:
            sql_client.create_views_for_tables({"items": "referenced_items"})
        assert len(external_db.sql("SELECT * FROM second.referenced_items").fetchall()) == 3000
        assert len(external_db.sql("SELECT * FROM first.items").fetchall()) == 3

        # test creating persistent secrets
        # NOTE: there is some kind of duckdb cache that makes testing persistent secrets impossible
        # because somehow the non-persistent secrets are around as long as the python process runs, even
        # wenn closing the db connection, renaming the db file and reconnecting
        secret_name = f"secret_{uniq_id()}_secret"

        supports_persistent_secrets = (
            destination_config.bucket_url.startswith("s3")
            or destination_config.bucket_url.startswith("az")
            or destination_config.bucket_url.startswith("abfss")
        )

        try:
            with fs_sql_client as sql_client:
                fs_sql_client.create_authentication(persistent=True, secret_name=secret_name)
            # the line below would error if there were no persistent secrets of the given name
            external_db.execute(f"DROP PERSISTENT SECRET {secret_name}")
        except Exception as exc:
            assert (
                not supports_persistent_secrets
            ), f"{destination_config.bucket_url} is expected to support persistent secrets"
            assert "Cannot create persistent secret" in str(exc)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
def test_read_interfaces_sql(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )
    _run_dataset_checks(pipeline, destination_config)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        bucket_exclude=[SFTP_BUCKET],
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
