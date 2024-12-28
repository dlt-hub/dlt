from typing import Any, cast, Tuple, List
import re
import pytest
import dlt
import os

from dlt import Pipeline
from dlt.common import Decimal

from typing import List
from functools import reduce

from dlt.common.storages.file_storage import FileStorage
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    GCS_BUCKET,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)
from dlt.destinations import filesystem
from tests.utils import TEST_STORAGE_ROOT, clean_test_storage
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
from dlt.destinations.dataset.exceptions import (
    ReadableRelationUnknownColumnException,
)
from tests.load.utils import drop_pipeline_data
from dlt.destinations.dataset import dataset as _dataset

EXPECTED_COLUMNS = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]


def _total_records(p: Pipeline) -> int:
    """how many records to load for a given pipeline"""
    if p.destination.destination_type == "dlt.destinations.bigquery":
        return 80
    elif p.destination.destination_type == "dlt.destinations.mssql":
        return 1000
    return 3000


def _chunk_size(p: Pipeline) -> int:
    """chunk size for a given pipeline"""
    if p.destination.destination_type == "dlt.destinations.bigquery":
        return 50
    elif p.destination.destination_type == "dlt.destinations.mssql":
        return 700
    return 2048


def _expected_chunk_count(p: Pipeline) -> List[int]:
    return [_chunk_size(p), _total_records(p) - _chunk_size(p)]


# this also disables autouse_test_storage on function level which destroys some tests here
@pytest.fixture(scope="session")
def autouse_test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(scope="session")
def populated_pipeline(request, autouse_test_storage) -> Any:
    """fixture that returns a pipeline object populated with the example data"""

    destination_config = cast(DestinationTestConfiguration, request.param)

    if (
        destination_config.file_format not in ["parquet", "jsonl"]
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip(
            "Test only works for jsonl and parquet on filesystem destination, given:"
            f" {destination_config.file_format}"
        )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"
    total_records = _total_records(pipeline)

    @dlt.source()
    def source():
        @dlt.resource(
            table_format=destination_config.table_format,
            write_disposition="replace",
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
            table_format=destination_config.table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                "double_id": {"data_type": "bigint"},
                "di_decimal": {"data_type": "decimal", "precision": 7, "scale": 3},
            },
        )
        def double_items():
            yield from [
                {
                    "id": i,
                    "double_id": i * 2,
                    "di_decimal": Decimal("10.433"),
                }
                for i in range(total_records)
            ]

        return [items, double_items]

    # run source
    s = source()
    pipeline.run(s, loader_file_format=destination_config.file_format)

    # in case of delta on gcs we use the s3 compat layer for reading
    # for writing we still need to use the gc authentication, as delta_rs seems to use
    # methods on the s3 interface that are not implemented by gcs
    if destination_config.bucket_url == GCS_BUCKET and destination_config.table_format == "delta":
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        access_pipeline = destination_config.setup_pipeline(
            "read_pipeline", dataset_name="read_test", destination=gcp_bucket
        )

        pipeline.destination = access_pipeline.destination

    # return pipeline to test
    yield pipeline

    # NOTE: we need to drop pipeline data here since we are keeping the pipelines around for the whole module
    drop_pipeline_data(pipeline)


# NOTE: we collect all destination configs centrally, this way the session based
# pipeline population per fixture setup will work and save a lot of time
configs = destinations_configs(
    default_sql_configs=True,
    all_buckets_filesystem_configs=True,
    table_format_filesystem_configs=True,
    bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_explicit_dataset_type_selection(populated_pipeline: Pipeline):
    from dlt.destinations.dataset.dataset import ReadableDBAPIRelation
    from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation

    assert isinstance(
        populated_pipeline.dataset(dataset_type="default").items, ReadableDBAPIRelation
    )
    assert isinstance(populated_pipeline.dataset(dataset_type="ibis").items, ReadableIbisRelation)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_arrow_access(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline.dataset().items
    total_records = _total_records(populated_pipeline)
    chunk_size = _chunk_size(populated_pipeline)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)

    # full table
    table = table_relationship.arrow()
    assert table.num_rows == total_records

    # chunk
    table = table_relationship.arrow(chunk_size=chunk_size)
    assert set(table.column_names) == set(EXPECTED_COLUMNS)
    assert table.num_rows == chunk_size

    # check frame amount and items counts
    tables = list(table_relationship.iter_arrow(chunk_size=chunk_size))
    assert [t.num_rows for t in tables] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [t.column(EXPECTED_COLUMNS[0]).to_pylist() for t in tables])
    assert set(ids) == set(range(total_records))


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_dataframe_access(populated_pipeline: Pipeline) -> None:
    # access via key
    table_relationship = populated_pipeline.dataset()["items"]
    total_records = _total_records(populated_pipeline)
    chunk_size = _chunk_size(populated_pipeline)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)
    skip_df_chunk_size_check = (
        populated_pipeline.destination.destination_type == "dlt.destinations.filesystem"
    )

    # full frame
    df = table_relationship.df()
    assert len(df.index) == total_records

    # chunk
    df = table_relationship.df(chunk_size=chunk_size)
    if not skip_df_chunk_size_check:
        assert len(df.index) == chunk_size

    assert set(df.columns.values) == set(EXPECTED_COLUMNS)

    # iterate all dataframes
    frames = list(table_relationship.iter_df(chunk_size=chunk_size))
    if not skip_df_chunk_size_check:
        assert [len(df.index) for df in frames] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [f[EXPECTED_COLUMNS[0]].to_list() for f in frames])
    assert set(ids) == set(range(total_records))


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_db_cursor_access(populated_pipeline: Pipeline) -> None:
    # check fetch accessors
    table_relationship = populated_pipeline.dataset().items
    total_records = _total_records(populated_pipeline)
    chunk_size = _chunk_size(populated_pipeline)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)

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


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_hint_preservation(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline.dataset(dataset_type="default").items
    # check that hints are carried over to arrow table
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    if populated_pipeline.destination.destination_type == "dlt.destinations.bigquery":
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


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_loads_table_access(populated_pipeline: Pipeline) -> None:
    # check loads table access, we should have one entry
    loads_table = populated_pipeline.dataset()[populated_pipeline.default_schema.loads_table_name]
    assert len(loads_table.fetchall()) == 1


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_row_counts(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline)

    dataset = populated_pipeline.dataset()
    # default is all data tables
    assert set(dataset.row_counts().df().itertuples(index=False, name=None)) == {
        (
            "items",
            total_records,
        ),
        (
            "double_items",
            total_records,
        ),
        (
            "items__children",
            total_records * 2,
        ),
    }
    # get only one data table
    assert set(
        dataset.row_counts(table_names=["items"]).df().itertuples(index=False, name=None)
    ) == {
        (
            "items",
            total_records,
        ),
    }
    # get all dlt tables
    assert set(
        dataset.row_counts(dlt_tables=True, data_tables=False)
        .df()
        .itertuples(index=False, name=None)
    ) == {
        (
            "_dlt_version",
            1,
        ),
        (
            "_dlt_loads",
            1,
        ),
        (
            "_dlt_pipeline_state",
            1,
        ),
    }
    # get them all
    assert set(dataset.row_counts(dlt_tables=True).df().itertuples(index=False, name=None)) == {
        (
            "_dlt_version",
            1,
        ),
        (
            "_dlt_loads",
            1,
        ),
        (
            "_dlt_pipeline_state",
            1,
        ),
        (
            "items",
            total_records,
        ),
        (
            "double_items",
            total_records,
        ),
        (
            "items__children",
            total_records * 2,
        ),
    }


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_sql_queries(populated_pipeline: Pipeline) -> None:
    # simple check that query also works
    tname = populated_pipeline.sql_client().make_qualified_table_name("items")
    query_relationship = populated_pipeline.dataset()(f"select * from {tname} where id < 20")

    # we selected the first 20
    table = query_relationship.arrow()
    assert table.num_rows == 20

    # check join query
    tdname = populated_pipeline.sql_client().make_qualified_table_name("double_items")
    query = (
        f"SELECT i.id, di.double_id FROM {tname} as i JOIN {tdname} as di ON (i.id = di.id) WHERE"
        " i.id < 20 ORDER BY i.id ASC"
    )
    join_relationship = populated_pipeline.dataset()(query)
    table = join_relationship.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_limit_and_head(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline.dataset().items

    assert len(table_relationship.head().fetchall()) == 5
    assert len(table_relationship.limit(24).fetchall()) == 24

    assert len(table_relationship.head().df().index) == 5
    assert len(table_relationship.limit(24).df().index) == 24

    assert table_relationship.head().arrow().num_rows == 5
    assert table_relationship.limit(24).arrow().num_rows == 24


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_column_selection(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline.dataset(dataset_type="default").items
    columns = ["_dlt_load_id", "other_decimal"]
    data_frame = table_relationship.select(*columns).head().df()
    assert [v.lower() for v in data_frame.columns.values] == columns
    assert len(data_frame.index) == 5

    columns = ["decimal", "other_decimal"]
    arrow_table = table_relationship[columns].head().arrow()
    assert arrow_table.column_names == columns
    assert arrow_table.num_rows == 5

    # hints should also be preserved via computed reduced schema
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    if populated_pipeline.destination.destination_type == "dlt.destinations.bigquery":
        # bigquery does not allow precision configuration..
        expected_decimal_precision = 38
        expected_decimal_precision_2 = 38
    assert arrow_table.schema.field("decimal").type.precision == expected_decimal_precision
    assert arrow_table.schema.field("other_decimal").type.precision == expected_decimal_precision_2

    with pytest.raises(ReadableRelationUnknownColumnException):
        arrow_table = table_relationship.select("unknown_column").head().arrow()


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_schema_arg(populated_pipeline: Pipeline) -> None:
    """Simple test to ensure schemas may be selected via schema arg"""

    # if there is no arg, the defautl schema is used
    dataset = populated_pipeline.dataset()
    assert dataset.schema.name == populated_pipeline.default_schema_name
    assert "items" in dataset.schema.tables

    # setting a different schema name will try to load that schema,
    # not find one and create an empty schema with that name
    dataset = populated_pipeline.dataset(schema="unknown_schema")
    assert dataset.schema.name == "unknown_schema"
    assert "items" not in dataset.schema.tables

    # providing the schema name of the right schema will load it
    dataset = populated_pipeline.dataset(schema=populated_pipeline.default_schema_name)
    assert dataset.schema.name == populated_pipeline.default_schema_name
    assert "items" in dataset.schema.tables


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_ibis_expression_relation(populated_pipeline: Pipeline) -> None:
    # NOTE: we could generalize this with a context for certain deps
    import ibis  # type: ignore

    # now we should get the more powerful ibis relation
    dataset = populated_pipeline.dataset()
    total_records = _total_records(populated_pipeline)

    items_table = dataset["items"]
    double_items_table = dataset["double_items"]

    # check full table access
    df = items_table.df()
    assert len(df.index) == total_records

    df = double_items_table.df()
    assert len(df.index) == total_records

    # check limit
    df = items_table.limit(5).df()
    assert len(df.index) == 5

    # check chained expression with join, column selection, order by and limit
    joined_table = (
        items_table.join(double_items_table, items_table.id == double_items_table.id)[
            ["id", "double_id"]
        ]
        .order_by("id")
        .limit(20)
    )
    table = joined_table.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]

    # check aggregate of first 20 items
    agg_table = items_table.order_by("id").limit(20).aggregate(sum_id=items_table.id.sum())
    assert agg_table.fetchone()[0] == reduce(lambda a, b: a + b, range(20))

    # check filtering
    filtered_table = items_table.filter(items_table.id < 10)
    assert len(filtered_table.fetchall()) == 10

    if populated_pipeline.destination.destination_type != "dlt.destinations.duckdb":
        return

    # we check a bunch of expressions without executing them to see that they produce correct sql
    # also we return the keys of the disovered schema columns
    def sql_from_expr(expr: Any) -> Tuple[str, List[str]]:
        query = str(expr.query).replace(populated_pipeline.dataset_name, "dataset")
        columns = list(expr.columns_schema.keys()) if expr.columns_schema else None
        return re.sub(r"\s+", " ", query), columns

    # test all functions discussed here: https://ibis-project.org/tutorials/ibis-for-sql-users
    ALL_COLUMNS = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]

    # selecting two columns
    assert sql_from_expr(items_table.select("id", "decimal")) == (
        'SELECT "t0"."id", "t0"."decimal" FROM "dataset"."items" AS "t0"',
        ["id", "decimal"],
    )

    # selecting all columns
    assert sql_from_expr(items_table) == ('SELECT * FROM "dataset"."items"', ALL_COLUMNS)

    # selecting two other columns via item getter
    assert sql_from_expr(items_table["id", "decimal"]) == (
        'SELECT "t0"."id", "t0"."decimal" FROM "dataset"."items" AS "t0"',
        ["id", "decimal"],
    )

    # adding a new columns
    new_col = (items_table.id * 2).name("new_col")
    assert sql_from_expr(items_table.select("id", "decimal", new_col)) == (
        (
            'SELECT "t0"."id", "t0"."decimal", "t0"."id" * 2 AS "new_col" FROM'
            ' "dataset"."items" AS "t0"'
        ),
        None,
    )

    # mutating table (add a new column computed from existing columns)
    assert sql_from_expr(
        items_table.mutate(double_id=items_table.id * 2).select("id", "double_id")
    ) == (
        'SELECT "t0"."id", "t0"."id" * 2 AS "double_id" FROM "dataset"."items" AS "t0"',
        None,
    )

    # mutating table add new static column
    assert sql_from_expr(
        items_table.mutate(new_col=ibis.literal("static_value")).select("id", "new_col")
    ) == ('SELECT "t0"."id", \'static_value\' AS "new_col" FROM "dataset"."items" AS "t0"', None)

    # check filtering (preserves all columns)
    assert sql_from_expr(items_table.filter(items_table.id < 10)) == (
        'SELECT * FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10',
        ALL_COLUMNS,
    )

    # filtering and selecting a single column
    assert sql_from_expr(items_table.filter(items_table.id < 10).select("id")) == (
        'SELECT "t0"."id" FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10',
        ["id"],
    )

    # check filter "and" condition
    assert sql_from_expr(items_table.filter(items_table.id < 10).filter(items_table.id > 5)) == (
        'SELECT * FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10 AND "t0"."id" > 5',
        ALL_COLUMNS,
    )

    # check filter "or" condition
    assert sql_from_expr(items_table.filter((items_table.id < 10) | (items_table.id > 5))) == (
        'SELECT * FROM "dataset"."items" AS "t0" WHERE ( "t0"."id" < 10 ) OR ( "t0"."id" > 5 )',
        ALL_COLUMNS,
    )

    # check group by and aggregate
    assert sql_from_expr(
        items_table.group_by("id")
        .having(items_table.count() >= 1000)
        .aggregate(sum_id=items_table.id.sum())
    ) == (
        (
            'SELECT "t1"."id", "t1"."sum_id" FROM ( SELECT "t0"."id", SUM("t0"."id") AS "sum_id",'
            ' COUNT(*) AS "CountStar(items)" FROM "dataset"."items" AS "t0" GROUP BY 1 ) AS "t1"'
            ' WHERE "t1"."CountStar(items)" >= 1000'
        ),
        None,
    )

    # sorting and ordering
    assert sql_from_expr(items_table.order_by("id", "decimal").limit(10)) == (
        (
            'SELECT * FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" ASC, "t0"."decimal" ASC'
            " LIMIT 10"
        ),
        ALL_COLUMNS,
    )

    # sort desc and asc
    assert sql_from_expr(items_table.order_by(ibis.desc("id"), ibis.asc("decimal")).limit(10)) == (
        (
            'SELECT * FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" DESC, "t0"."decimal" ASC'
            " LIMIT 10"
        ),
        ALL_COLUMNS,
    )

    # offset and limit
    assert sql_from_expr(items_table.order_by("id").limit(10, offset=5)) == (
        'SELECT * FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" ASC LIMIT 10 OFFSET 5',
        ALL_COLUMNS,
    )

    # join
    assert sql_from_expr(
        items_table.join(double_items_table, items_table.id == double_items_table.id)[
            ["id", "double_id"]
        ]
    ) == (
        (
            'SELECT "t2"."id", "t3"."double_id" FROM "dataset"."items" AS "t2" INNER JOIN'
            ' "dataset"."double_items" AS "t3" ON "t2"."id" = "t3"."id"'
        ),
        None,
    )

    # subqueries
    assert sql_from_expr(
        items_table.filter(items_table.decimal.isin(double_items_table.di_decimal))
    ) == (
        (
            'SELECT * FROM "dataset"."items" AS "t0" WHERE "t0"."decimal" IN ( SELECT'
            ' "t1"."di_decimal" FROM "dataset"."double_items" AS "t1" )'
        ),
        ALL_COLUMNS,
    )

    # topk
    assert sql_from_expr(items_table.decimal.topk(10)) == (
        (
            'SELECT * FROM ( SELECT "t0"."decimal", COUNT(*) AS "CountStar(items)" FROM'
            ' "dataset"."items" AS "t0" GROUP BY 1 ) AS "t1" ORDER BY "t1"."CountStar(items)" DESC'
            " LIMIT 10"
        ),
        None,
    )


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_ibis_dataset_access(populated_pipeline: Pipeline) -> None:
    # NOTE: we could generalize this with a context for certain deps

    from dlt.helpers.ibis import SUPPORTED_DESTINATIONS

    # check correct error if not supported
    if populated_pipeline.destination.destination_type not in SUPPORTED_DESTINATIONS:
        with pytest.raises(NotImplementedError):
            populated_pipeline.dataset().ibis()
        return

    total_records = _total_records(populated_pipeline)
    ibis_connection = populated_pipeline.dataset().ibis()

    map_i = lambda x: x
    if populated_pipeline.destination.destination_type == "dlt.destinations.snowflake":
        map_i = lambda x: x.upper()

    dataset_name = map_i(populated_pipeline.dataset_name)
    table_like_statement = None
    table_name_prefix = ""
    addtional_tables = []

    # clickhouse has no datasets, but table prefixes and a sentinel table
    if populated_pipeline.destination.destination_type == "dlt.destinations.clickhouse":
        table_like_statement = dataset_name + "."
        table_name_prefix = dataset_name + "___"
        dataset_name = None
        addtional_tables = ["dlt_sentinel_table"]

    add_table_prefix = lambda x: table_name_prefix + x

    # just do a basic check to see wether ibis can connect
    assert set(ibis_connection.list_tables(database=dataset_name, like=table_like_statement)) == {
        add_table_prefix(map_i(x))
        for x in (
            [
                "_dlt_loads",
                "_dlt_pipeline_state",
                "_dlt_version",
                "double_items",
                "items",
                "items__children",
            ]
            + addtional_tables
        )
    }

    items_table = ibis_connection.table(add_table_prefix(map_i("items")), database=dataset_name)
    assert items_table.count().to_pandas() == total_records
    ibis_connection.disconnect()


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_standalone_dataset(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline)

    # check dataset factory
    dataset = _dataset(
        destination=populated_pipeline.destination, dataset_name=populated_pipeline.dataset_name
    )
    # verfiy that sql client and schema are lazy loaded
    assert not dataset._schema
    assert not dataset._sql_client
    table_relationship = dataset.items
    table = table_relationship.fetchall()
    assert len(table) == total_records

    # check that schema is loaded by name
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
            schema=populated_pipeline.default_schema_name,
        ),
    )
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that schema is not loaded when wrong name given
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
            schema="wrong_schema_name",
        ),
    )
    assert "items" not in dataset.schema.tables
    assert dataset.schema.name == "wrong_schema_name"

    # check that schema is loaded if no schema name given
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
        ),
    )
    assert dataset.schema.name == populated_pipeline.default_schema_name
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that there is no error when creating dataset without schema table
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name="unknown_dataset",
        ),
    )
    assert dataset.schema.name == "unknown_dataset"
    assert "items" not in dataset.schema.tables

    # NOTE: this breaks the following test, it will need to be fixed somehow
    # create a newer schema with different name and see wether this is loaded
    from dlt.common.schema import Schema
    from dlt.common.schema import utils

    other_schema = Schema("some_other_schema")
    other_schema.tables["other_table"] = utils.new_table("other_table")

    populated_pipeline._inject_schema(other_schema)
    populated_pipeline.default_schema_name = other_schema.name
    with populated_pipeline.destination_client() as client:
        client.update_stored_schema()

    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
        ),
    )
    assert dataset.schema.name == "some_other_schema"
    assert "other_table" in dataset.schema.tables
