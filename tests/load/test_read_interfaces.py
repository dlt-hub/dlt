import pytest
import dlt
import os

from dlt import Pipeline
from dlt.common import Decimal

from typing import List
from functools import reduce

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    GCS_BUCKET,
    SFTP_BUCKET,
)
from dlt.destinations import filesystem


def _run_dataset_checks(
    pipeline: Pipeline, destination_config: DestinationTestConfiguration
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
            columns={
                "id": {"data_type": "bigint"},
                # we add a decimal with precision to see wether the hints are preserved
                "decimal": {"data_type": "decimal", "precision": 10, "scale": 3},
                "other_decimal": {"data_type": "decimal", "precision": 12, "scale": 3},
            }
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
            columns={
                "id": {"data_type": "bigint"},
                "double_id": {"data_type": "bigint"},
            }
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
    assert table[0] == (0, 0)
    assert table[5] == (5, 10)
    assert table[10] == (10, 20)

    # special filesystem sql checks
    if destination_config.destination_type == "filesystem":
        import duckdb
        from dlt.destinations.impl.filesystem.sql_client import FilesystemSqlClient

        # check we can create new tables from the views
        with pipeline.sql_client() as c:
            c.create_view_for_tables({"items": "items", "double_items": "double_items"})
            c.execute_sql(
                "CREATE TABLE items_joined AS (SELECT i.id, di.double_id FROM items as i JOIN"
                " double_items as di ON (i.id = di.id));"
            )
            with c.execute_query("SELECT * FROM items_joined ORDER BY id ASC;") as cursor:
                joined_table = cursor.fetchall()
                assert len(joined_table) == total_records
                assert joined_table[0] == (0, 0)
                assert joined_table[5] == (5, 10)
                assert joined_table[10] == (10, 20)

            # inserting values into a view should fail gracefully
            try:
                c.execute_sql("INSERT INTO double_items VALUES (1, 2)")
            except Exception as exc:
                assert "double_items is not an table" in str(exc)

        # we create a second duckdb pipieline and will see if we can make our filesystem views available there
        other_pipeline = dlt.pipeline("other_pipeline", dev_mode=True, destination="duckdb")
        other_db_location = (
            other_pipeline.destination_client().config.credentials.database  #  type: ignore
        )
        other_pipeline.run([1, 2, 3], table_name="items")
        assert len(other_pipeline._dataset().items.fetchall()) == 3

        # TODO: implement these tests
        return

        # now we can use the filesystemsql client to create the needed views
        fs_sql_client = FilesystemSqlClient(
            pipeline.destination_client(),
            dataset_name=other_pipeline.dataset_name,
            duckdb_connection=duckdb.connect(other_db_location),
        )
        fs_sql_client.create_persistent_secrets = True
        with fs_sql_client as sql_client:
            sql_client.create_view_for_tables({"items": "referenced_items"})

        # we now have access to this view on the original dataset
        assert len(other_pipeline._dataset().items.fetchall()) == 3
        assert len(other_pipeline._dataset().referenced_items.fetchall()) == 3000


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
        assert pipeline.destination_client().config.credentials


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
