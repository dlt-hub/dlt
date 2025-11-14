from typing import Any, cast, Tuple, List
import re
import pytest
import dlt
import os

from dlt import Pipeline
from dlt.common import Decimal

from typing import List
from functools import reduce

from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableFormat

from dlt.common.utils import uniq_id
from dlt.extract.source import DltSource
from dlt.dataset.exceptions import LineageFailedException

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)
from tests.utils import (
    preserve_module_environ,
    auto_module_test_storage,
    auto_module_test_run_context,
)
from tests.load.utils import drop_pipeline_data

EXPECTED_COLUMNS = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]


def _total_records(destination_type: str) -> int:
    """how many records to load for a given pipeline"""
    if destination_type == "dlt.destinations.bigquery":
        return 80
    elif destination_type == "dlt.destinations.mssql":
        return 1000
    return 3000


def _chunk_size(destination_type: str) -> int:
    """chunk size for a given pipeline"""
    if destination_type == "dlt.destinations.bigquery":
        return 50
    elif destination_type == "dlt.destinations.mssql":
        return 700
    return 2048


def _expected_chunk_count(p: Pipeline) -> List[int]:
    destination_type = p.destination.destination_type
    chunk_size = _chunk_size(destination_type)
    total_records = _total_records(destination_type)

    return [
        chunk_size,
        total_records - chunk_size,
    ]


def create_test_source(destination_type: str, table_format: TTableFormat) -> DltSource:
    total_records = _total_records(destination_type)

    # TODO: this test should test ALL data types using our standard fixture
    #       step 1 would be to just let it run and see we do not have exceptions

    @dlt.source()
    def source():
        @dlt.resource(
            table_format=table_format,
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
            table_format=table_format,
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

        @dlt.resource(
            table_format=table_format,
            write_disposition="replace",
            columns={"id": {"data_type": "bigint"}, "other_id": {"data_type": "bigint"}},
        )
        def orderable_in_chain():
            yield from [
                {
                    "id": int(i / 2),
                    "other_id": i,
                }
                for i in range(total_records)
            ]

        return [items, double_items, orderable_in_chain]

    return source()


@pytest.fixture(scope="module")
def populated_pipeline(
    request, auto_module_test_storage, preserve_module_environ, auto_module_test_run_context
) -> Any:
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
    # run source
    s = create_test_source(pipeline.destination.destination_type, destination_config.table_format)
    pipeline.run(s, loader_file_format=destination_config.file_format)
    print(pipeline.last_trace.last_normalize_info)
    # create a second schema in the pipeline
    # NOTE: that generates additional load package and then another one for the state
    # NOTE: "aleph" schema is now the newest schema in the dataset and we assume that later in the tests
    # TODO: we need some kind of idea for multi-schema datasets
    pipeline.run([1, 2, 3], table_name="digits", schema=Schema("aleph"))
    print(pipeline.last_trace.last_normalize_info)

    # return pipeline to test
    try:
        yield pipeline
    finally:
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
def test_str_and_repr_on_dataset_and_relation(populated_pipeline: Pipeline) -> None:
    # no need to test on all destinations
    if populated_pipeline.destination.destination_type != "dlt.destinations.duckdb":
        pytest.skip("Only duckdb is supported for this test")

    dataset_ = populated_pipeline.dataset()

    def _replace_variable_content(s: str) -> str:
        # replace dataset name
        s = s.replace(dataset_.dataset_name, "dataset_name")
        # replace destination config
        dest_config = str(populated_pipeline.dataset().destination_client.config)
        s = s.replace(dest_config, "<destination_config>")
        return s

    # dataset
    assert (
        _replace_variable_content(str(dataset_))
        == "Dataset `dataset_name` at `duckdb[<destination_config>]` with tables in dlt schema"
        " `source`:\nitems, double_items, orderable_in_chain, items__children"
    )

    dataset_repr = _replace_variable_content(repr(dataset_))
    assert dataset_repr.startswith("<dlt.dataset(dataset_name='dataset_name',")

    # relation
    relation = dataset_("SELECT id, decimal FROM items")
    assert _replace_variable_content(str(relation)) == """Relation query:
  SELECT
    "items"."id" AS "id",
    "items"."decimal" AS "decimal"
  FROM "dataset_name"."items" AS "items"
Columns:
  id bigint
  decimal decimal
"""
    relation_repr = _replace_variable_content(repr(relation))
    assert relation_repr.startswith(
        "<dlt.Relation(dataset='<dlt.dataset(dataset_name=\\'dataset_name\\'"
    )
    assert '"items"."decimal" AS "decimal"' in relation_repr


@pytest.mark.no_load
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_fetchscalar(populated_pipeline: Pipeline) -> None:
    assert populated_pipeline.dataset()(
        "SELECT COUNT(*) FROM items"
    ).fetchscalar() == _total_records(populated_pipeline.destination.destination_type)

    # test error if more than one row is returned and we use scalar
    with pytest.raises(ValueError) as ex:
        populated_pipeline.dataset().table("items").fetchscalar()
    assert "got more than one row" in str(ex.value)

    with pytest.raises(ValueError) as ex:
        populated_pipeline.dataset()["items"].limit(1).limit(1).fetchscalar()
    assert "got 1 row with 5 columns" in str(ex.value)


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
    total_records = _total_records(populated_pipeline.destination.destination_type)
    chunk_size = _chunk_size(populated_pipeline.destination.destination_type)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)

    # full table
    table = table_relationship.arrow()
    assert table.num_rows == total_records
    assert set(table.column_names) == set(EXPECTED_COLUMNS)

    # chunk
    table = table_relationship.arrow(chunk_size=chunk_size)
    assert set(table.column_names) == set(EXPECTED_COLUMNS)
    # NOTE: chunksize is unpredictable on snowflake
    if populated_pipeline.destination.destination_type != "dlt.destinations.snowflake":
        assert table.num_rows == chunk_size

    # check frame amount and items counts
    tables = list(table_relationship.iter_arrow(chunk_size=chunk_size))
    if populated_pipeline.destination.destination_type != "dlt.destinations.snowflake":
        assert [t.num_rows for t in tables] == expected_chunk_counts

    # check all items are present, this MUST also be true for snowflake
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
    total_records = _total_records(populated_pipeline.destination.destination_type)
    chunk_size = _chunk_size(populated_pipeline.destination.destination_type)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)
    skip_df_chunk_size_check = populated_pipeline.destination.destination_type in [
        "dlt.destinations.filesystem",
        "dlt.destinations.snowflake",
        "dlt.destinations.ducklake",  # vector size seems to not be consistent, typically 700
    ]

    # full frame
    df = table_relationship.df()
    assert len(df.index) == total_records
    assert set(df.columns.values) == set(EXPECTED_COLUMNS)

    # TODO: snowflake does not follow a chunk size, make and exception (accept range), same for arrow
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
    total_records = _total_records(populated_pipeline.destination.destination_type)
    chunk_size = _chunk_size(populated_pipeline.destination.destination_type)
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
    table_relationship = populated_pipeline.dataset().items
    # check that hints are carried over to arrow table
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    if populated_pipeline.destination.destination_type in [
        "dlt.destinations.bigquery",
        "dlt.destinations.snowflake",
    ]:
        # bigquery does not allow precision configuration..
        expected_decimal_precision = 38
        expected_decimal_precision_2 = 38

    # NOTE: pyarrow 19 exposes decimal64 type and duckdb 1.3 is using it for low precision decimals
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
    # check loads table access, we should have 3 entires
    # - first source (default schema)
    # - additional schema (digits)
    # - state update send in separate package to default schema
    loads_table = populated_pipeline.dataset()[populated_pipeline.default_schema.loads_table_name]
    assert len(loads_table.fetchall()) == 3


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_row_counts(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline.destination.destination_type)

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
        (
            "orderable_in_chain",
            total_records,
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
            2,
        ),
        (
            "_dlt_loads",
            3,
        ),
        (
            "_dlt_pipeline_state",
            2,
        ),
    }
    # get them all
    assert set(dataset.row_counts(dlt_tables=True).df().itertuples(index=False, name=None)) == {
        (
            "_dlt_version",
            2,
        ),
        (
            "_dlt_loads",
            3,
        ),
        (
            "_dlt_pipeline_state",
            2,
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
        (
            "orderable_in_chain",
            total_records,
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
    dataset_name = populated_pipeline.dataset_name

    # simple check that query also works
    query_relationship = populated_pipeline.dataset()("select * from items where id < 20")
    query_from_query_function = populated_pipeline.dataset().query(
        "select * from items where id < 20"
    )

    assert query_relationship.sqlglot_expression == query_from_query_function.sqlglot_expression

    # we selected the first 20
    table = query_relationship.arrow()
    assert table.num_rows == 20

    # check join query
    query = (
        "SELECT i.id, di.double_id FROM items as i JOIN double_items as di ON (i.id = di.id) WHERE"
        " i.id < 20 ORDER BY i.id ASC"
    )
    join_relationship = populated_pipeline.dataset()(query)
    table = join_relationship.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]

    # check query with explicit dataset, query is in "schema" identifier space
    query = (
        f"SELECT i.id, di.double_id FROM {dataset_name}.items as i JOIN {dataset_name}.double_items"
        " as di ON (i.id = di.id) WHERE i.id < 20 ORDER BY i.id ASC"
    )

    join_relationship = populated_pipeline.dataset()(query)
    table = join_relationship.fetchall()
    assert len(table) == 20

    # check statement that is not a select query
    query = f"CREATE TABLE {dataset_name}.test (id bigint)"
    with pytest.raises(LineageFailedException) as exc:
        populated_pipeline.dataset()(query).df()
    assert "is not a SELECT statement." in str(exc.value)

    # we also get an error in raw query mode
    with pytest.raises(ValueError) as exc2:
        populated_pipeline.dataset()(query, _execute_raw_query=True).df()
    assert "Must be an SQL SELECT statement" in str(exc2.value)

    # we only test the following for duckdb
    if populated_pipeline.destination.destination_type != "dlt.destinations.duckdb":
        return

    # test various query stages
    # raw query has no aliases
    assert (
        join_relationship.sqlglot_expression.sql("duckdb").replace(dataset_name, "dataset_name")
        == "SELECT i.id, di.double_id FROM dataset_name.items AS i JOIN dataset_name.double_items"
        " AS di ON (i.id = di.id) WHERE i.id < 20 ORDER BY i.id ASC"
    )

    # TODO move these tests to the `dlt.destination.queries::normalize_query()`
    # TODO modify `dlt.dataset.lineage::compute_columns_schema()` to return the normalized query instead of a tuple
    # qualified query has aliases
    # assert (
    #     join_relationship._qualified_query.sql("duckdb").replace(dataset_name, "dataset_name")
    #     == "SELECT i.id AS id, di.double_id AS double_id FROM dataset_name.items AS i JOIN"
    #     " dataset_name.double_items AS di ON (i.id = di.id) WHERE i.id < 20 ORDER BY i.id ASC"
    # )

    # TODO move these tests to the `dlt.destination.queries::normalize_query()`
    # normalized has quoted indentifiers
    # assert (
    #     join_relationship._normalized_query.sql("duckdb").replace(dataset_name, "dataset_name")
    #     == 'SELECT "i"."id" AS "id", "di"."double_id" AS "double_id" FROM "dataset_name"."items" AS'
    #     ' "i" JOIN "dataset_name"."double_items" AS "di" ON ("i"."id" = "di"."id") WHERE'
    #     ' "i"."id" < 20 ORDER BY "i"."id" ASC'
    # )


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_limit_and_head(populated_pipeline: Pipeline) -> None:
    dataset_ = populated_pipeline.dataset()

    # test sql_client lifecycle
    assert dataset_._opened_sql_client is None

    table_relationship = dataset_.items

    assert len(table_relationship.head().fetchall()) == 5
    # no client remains
    assert dataset_._opened_sql_client is None

    assert len(table_relationship.limit(24).fetchall()) == 24
    assert dataset_._opened_sql_client is None

    assert len(table_relationship.head().df().index) == 5
    assert dataset_._opened_sql_client is None

    assert len(table_relationship.limit(24).df().index) == 24
    assert dataset_._opened_sql_client is None

    assert table_relationship.head().arrow().num_rows == 5
    assert dataset_._opened_sql_client is None

    assert table_relationship.limit(24).arrow().num_rows == 24
    assert dataset_._opened_sql_client is None

    limit_relationship = table_relationship.limit(24)
    for data_ in limit_relationship.iter_fetch(6):
        assert len(data_) == 6
        # client stays open
        assert limit_relationship._opened_sql_client is not None

    # run multiple requests on one connection
    with dataset_ as d_:
        limit_relationship = table_relationship.limit(24)
        for _data in limit_relationship.iter_fetch(6):
            # client stays open
            assert limit_relationship._opened_sql_client is not None
            assert (
                limit_relationship._opened_sql_client.native_connection
                == d_._opened_sql_client.native_connection
            )

        other_relationship = table_relationship.limit(10)
        for _data in other_relationship.iter_fetch(6):
            assert other_relationship._opened_sql_client is not None
            assert (
                other_relationship._opened_sql_client.native_connection
                == d_._opened_sql_client.native_connection
            )

    # connection closed
    assert dataset_._opened_sql_client is None

    chunk_size = _chunk_size(populated_pipeline.destination.destination_type)
    list(table_relationship.iter_fetch(chunk_size=chunk_size))
    assert dataset_._opened_sql_client is None


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_dataset_client_caching_and_connection_handling(populated_pipeline: Pipeline) -> None:
    # no clients exist yet
    dataset = populated_pipeline.dataset()
    assert dataset._opened_sql_client is None
    assert dataset._sql_client is None

    with dataset as dataset_:
        # test sql_client lifecycle
        assert dataset_._opened_sql_client is not None

        first_encountered_opened_sql_client = dataset_._opened_sql_client

        # "regular" clients are not created yet as never used
        assert dataset_._sql_client is None

        # cached sql client is used
        table_relationship = dataset_.items
        assert dataset_._opened_sql_client is not None
        assert dataset_._opened_sql_client == first_encountered_opened_sql_client

        assert len(table_relationship.head().fetchall()) == 5
        assert dataset_._opened_sql_client is not None
        assert dataset_._opened_sql_client == first_encountered_opened_sql_client

        # connection is kept
        assert dataset_._opened_sql_client.native_connection is not None

        for data_ in table_relationship.limit(24).iter_fetch(6):
            assert len(data_) == 6
            # connection kept open
            assert dataset_._opened_sql_client.native_connection is not None

    # connection closed
    assert dataset_._opened_sql_client is None

    # we do something that activates the "regular" caching
    dataset_.items.fetchall()
    assert dataset_._sql_client is not None
    assert dataset_._opened_sql_client is None

    # open again
    with dataset_:
        assert dataset_._opened_sql_client is not None
        assert len(table_relationship.head().fetchall()) == 5
        assert dataset_._opened_sql_client is not None
        # connection is kept
        assert dataset_._opened_sql_client.native_connection is not None

        # the opened client is different from the "regular" one
        assert dataset_._sql_client != dataset_._opened_sql_client
        # and different from the last one
        assert dataset_._opened_sql_client != first_encountered_opened_sql_client

        with pytest.raises(AssertionError):
            with dataset_:
                pass
    assert dataset_._opened_sql_client is None

    # check that if the schema needs to be fetched, no opened client is left
    dataset_._schema = None
    assert dataset_.schema
    assert dataset_._opened_sql_client is None


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_column_selection(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline.dataset().items
    columns = ["_dlt_load_id", "other_decimal"]
    data_frame = table_relationship.select(*columns).head().df()
    assert list(data_frame.columns.values) == columns
    assert len(data_frame.index) == 5

    # test single column indexer
    arrow_table = table_relationship["other_decimal"].limit(1).arrow()
    assert arrow_table.column_names == ["other_decimal"]
    assert arrow_table.num_rows == 1

    # test multiple column indexer
    columns = ["decimal", "other_decimal"]
    arrow_table = table_relationship[columns].head().arrow()
    assert arrow_table.column_names == columns
    assert arrow_table.num_rows == 5

    # TODO: fix those for bigquery and snowflake which use native cursor and does not fit into our schema 100#
    # this is really good test, we should make a strict test for arrow reading for all destinations
    # hints should also be preserved via computed reduced schema
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    expected_decimal_scale = 3
    # bigquery and snowflake take arrow tables via native cursor and they mange precision
    # we should probably cast arrow tables to our schema in cursors
    if populated_pipeline.destination.destination_type in [
        "dlt.destinations.bigquery",
        "dlt.destinations.snowflake",
    ]:
        expected_decimal_precision = 38
        expected_decimal_precision_2 = 38

    if populated_pipeline.destination.destination_type == "dlt.destinations.bigquery":
        expected_decimal_scale = 9

    assert arrow_table.schema.field("decimal").type.scale == expected_decimal_scale
    assert arrow_table.schema.field("other_decimal").type.scale == expected_decimal_scale

    assert arrow_table.schema.field("decimal").type.precision == expected_decimal_precision
    assert arrow_table.schema.field("other_decimal").type.precision == expected_decimal_precision_2

    with pytest.raises(LineageFailedException):
        arrow_table = table_relationship.select("unknown_column").head().arrow()


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_chained_column_selection(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline.dataset().items

    columns = ["_dlt_load_id", "other_decimal"]
    data_frame = table_relationship.select(*columns).head().df()
    assert list(data_frame.columns.values) == columns

    data_frame = table_relationship.select(*columns).select(*columns).head().df()
    assert list(data_frame.columns.values) == columns

    data_frame = table_relationship.select(*columns).select(*["other_decimal"]).head().df()
    assert list(data_frame.columns.values) == ["other_decimal"]

    data_frame = table_relationship.select(*columns).select(*["decimal"]).head().df()
    assert list(data_frame.columns.values) == ["decimal"]


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_order_by(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline.destination.destination_type)
    table_relationship = populated_pipeline.dataset().items

    asc_ids = [row[0] for row in table_relationship.order_by("id", "asc").limit(20).fetchall()]
    assert asc_ids == list(range(20))

    desc_ids = [row[0] for row in table_relationship.order_by("id", "desc").limit(20).fetchall()]
    assert desc_ids == list(range(total_records - 1, total_records - 21, -1))

    chained = [row[0] for row in table_relationship.order_by("id").limit(5).select("id").fetchall()]
    assert chained == list(range(5))

    chained_relation = populated_pipeline.dataset().orderable_in_chain
    chained_order_by = [
        row
        for row in chained_relation.order_by("id", "asc")
        .order_by("other_id", "desc")
        .limit(5)
        .fetchall()
    ]

    assert [row[0] for row in chained_order_by] == [0, 0, 1, 1, 2]
    assert [row[1] for row in chained_order_by] == [1, 0, 3, 2, 5]


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_where(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline.destination.destination_type)
    items = populated_pipeline.dataset().items

    eq_rows = items.where("id", "eq", 10).fetchall()
    assert len(eq_rows) == 1 and eq_rows[0][0] == 10

    ne_rows = items.filter("id", "ne", 0).fetchall()
    assert total_records - 1 == len(ne_rows)

    gt_rows = items.where("id", "gt", 2).fetchall()
    assert total_records - 3 == len(gt_rows)

    lt_rows = items.filter("id", "lt", 5).fetchall()
    assert 5 == len(lt_rows)

    gte_rows = items.where("id", "gte", 5).fetchall()
    lte_rows = items.filter("id", "lte", "5").fetchall()
    assert total_records - 5 == len(gte_rows)
    assert 6 == len(lte_rows)

    in_ids = [r[0] for r in (items.where("id", "in", [3, 1, 7]).order_by("id").fetchall())]
    assert in_ids == [1, 3, 7]

    not_in_rows = items.filter("id", "not_in", [0, 1, 2]).fetchall()
    assert total_records - 3 == len(not_in_rows)

    with pytest.raises(ValueErrorWithKnownValues) as py_exc:
        not_in_rows = items.filter("id", "wrong", [0, 1, 2]).fetchall()

    assert "Received invalid value `operator=wrong`" in py_exc.value.args[0]

    assert total_records - 3 == len(not_in_rows)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_where_expr_or_str(populated_pipeline: Pipeline) -> None:
    items = populated_pipeline.dataset().items
    orderable_in_chain = populated_pipeline.dataset().orderable_in_chain
    total_records = _total_records(populated_pipeline.destination.destination_type)

    filtered_items_sql = items.where("id < 10").fetchall()
    assert len(filtered_items_sql) == 10
    assert all(row[0] < 10 for row in filtered_items_sql)

    load_id = items.select("_dlt_load_id").max().fetchscalar()
    # NOTE: query below tests dremio wrong MAX behavior where strings are casted to decimals, we locked dremio container to 25.0 tag
    # f'SELECT MAX(CONCAT(\'_\', "_dlt_load_id")) AS "_col_0" FROM "nas"."{populated_pipeline.dataset_name}"."items" AS "items"')
    all_items = items.where(f"_dlt_load_id = '{load_id}'").fetchall()
    assert len(all_items) == total_records

    filtered_items_range = items.where("id >= 5 AND id <= 15").fetchall()
    assert len(filtered_items_range) == 11  # ids 5 through 15 inclusive
    assert all(5 <= row[0] <= 15 for row in filtered_items_range)

    import sqlglot.expressions as sge

    # Create a sqlglot expression: id = 42
    expr = sge.EQ(
        this=sge.Column(this=sge.to_identifier("id", quoted=True)),
        expression=sge.Literal.number("42"),
    )
    filtered_items_expr = items.where(expr).fetchall()
    assert len(filtered_items_expr) == 1
    assert filtered_items_expr[0][0] == 42

    # Test combination with other methods
    combined_result = items.where("id < 100").limit(5).fetchall()
    assert len(combined_result) == 5
    assert all(row[0] < 100 for row in combined_result)

    combined_result = orderable_in_chain.where("id = 1").select("other_id").max().fetchscalar()
    assert 3 == combined_result

    combined_result = orderable_in_chain.where("id = 1").select("other_id").min().fetchscalar()
    assert 2 == combined_result


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_min_max(populated_pipeline: Pipeline) -> None:
    items = populated_pipeline.dataset().items
    total_records = _total_records(populated_pipeline.destination.destination_type)

    max_id = items.select("id").max().fetchscalar()
    assert max_id == total_records - 1

    min_id = items.select("id").min().fetchscalar()
    assert min_id == 0

    with pytest.raises(ValueError) as py_exc:
        min_id = items.select("id", "decimal").min().fetchscalar()

    assert "min() requires a query with exactly one select expression." in py_exc.value.args[0]


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_unknown_table_access(populated_pipeline: Pipeline) -> None:
    match = "Table `unknown_table` not found"
    dataset = populated_pipeline.dataset()

    # missing attribute should raise Attribute error
    with pytest.raises(AttributeError, match=match):
        dataset.unknown_table

    # missing key should raise KeyError
    with pytest.raises(KeyError, match=match):
        dataset["unknown_table"]

    with pytest.raises(ValueError, match=match):
        dataset.table("unknown_table")


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

    # if there is no arg, the default schema is used
    dataset = populated_pipeline.dataset()
    assert dataset.schema.name == populated_pipeline.default_schema_name
    assert "items" in dataset.schema.tables

    # if setting a different schema, default schema with dataset name will be used
    populated_pipeline.dataset(schema="source")
    assert dataset.schema.name == "source"
    assert "items" in dataset.schema.tables

    # explicit schema object is OK
    dataset = populated_pipeline.dataset(schema=Schema("unknown_schema"))
    assert dataset.schema.name == "unknown_schema"
    assert "items" not in dataset.schema.tables

    # providing the schema name of the right schema will load it
    dataset = populated_pipeline.dataset(schema="aleph")
    assert dataset.schema.name == "aleph"
    assert "digits" in dataset.schema.tables
    dataset.digits.fetchall()


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
    import ibis

    # now we should get the more powerful ibis relation
    dataset = populated_pipeline.dataset()
    total_records = _total_records(populated_pipeline.destination.destination_type)

    items_table = dataset.table("items").to_ibis()
    double_items_table = dataset.table("double_items").to_ibis()

    # check full table access
    df = dataset(items_table).df()
    assert len(df.index) == total_records

    df = dataset(double_items_table).df()
    assert len(df.index) == total_records

    # check limit
    df = dataset(items_table).limit(5).df()
    assert len(df.index) == 5

    # check chained expression with join, column selection, order by and limit
    joined_table = (
        items_table.join(double_items_table, items_table.id == double_items_table.id)[
            ["id", "double_id"]
        ]
        .order_by("id")
        .limit(20)
    )
    relation = dataset(joined_table)
    table = relation.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]

    # take the same data using dlt backend
    joined_table = joined_table.order_by("id")
    joined_table.head(10).to_pandas()
    arr_1 = joined_table.head(10).to_pyarrow()

    # create relation from query and convert it to ibis
    # NOTE: mssql / synapse dialect can't deal with double order by (.order_by("id", "asc"))
    # but ibis expressions can (see above order_by("id"))
    joined_table_from_relation = relation.to_ibis()
    # print(joined_table_from_relation)
    joined_table_from_relation.head(10).to_pandas()
    arr_2 = joined_table_from_relation.head(10).to_pyarrow()
    assert arr_1 == arr_2
    # NOTE: identical column names both from snowflake, clickhouse and other destinations
    assert arr_1.to_pylist() == [
        {"id": 0, "double_id": 0},
        {"id": 1, "double_id": 2},
        {"id": 2, "double_id": 4},
        {"id": 3, "double_id": 6},
        {"id": 4, "double_id": 8},
        {"id": 5, "double_id": 10},
        {"id": 6, "double_id": 12},
        {"id": 7, "double_id": 14},
        {"id": 8, "double_id": 16},
        {"id": 9, "double_id": 18},
    ]

    # verify computed columns
    assert relation.columns == relation._ipython_key_completions_() == ["id", "double_id"]

    # check aggregate of first 20 items
    agg_query = items_table.order_by("id").limit(20).aggregate(sum_id=items_table.id.sum())
    agg_relation = dataset(agg_query)
    assert agg_relation.fetchone()[0] == reduce(lambda a, b: a + b, range(20))
    assert agg_relation.columns == agg_relation._ipython_key_completions_() == ["sum_id"]

    # check filtering
    filtered_table = items_table.filter(items_table.id < 10)
    assert len(dataset(filtered_table).fetchall()) == 10

    if populated_pipeline.destination.destination_type != "dlt.destinations.duckdb":
        return

    # we check a bunch of expressions without executing them to see that they produce correct sql
    # also we return the keys of the discovered schema columns
    def sql_from_expr(expr: Any) -> Tuple[str, List[str]]:
        relation = dataset(expr)
        query = str(relation.to_sql()).replace(populated_pipeline.dataset_name, "dataset")
        columns = relation.columns if relation.columns else None
        return re.sub(r"\s+", " ", query), columns

    # test all functions discussed here: https://ibis-project.org/tutorials/ibis-for-sql-users
    ALL_COLUMNS = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]

    # selecting two columns
    assert sql_from_expr(items_table.select("id", "decimal")) == (
        'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal" FROM "dataset"."items" AS "t0"',
        ["id", "decimal"],
    )

    # selecting all columns (star schema expanded, columns aliased)
    # TODO: fix tests
    assert sql_from_expr(items_table) == (
        (
            'SELECT "items"."id" AS "id", "items"."decimal" AS "decimal", "items"."other_decimal"'
            ' AS "other_decimal", "items"."_dlt_load_id" AS "_dlt_load_id", "items"."_dlt_id" AS'
            ' "_dlt_id" FROM "dataset"."items" AS "items"'
        ),
        ALL_COLUMNS,
    )

    # selecting two other columns via item getter
    assert sql_from_expr(items_table["id", "decimal"]) == (
        'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal" FROM "dataset"."items" AS "t0"',
        ["id", "decimal"],
    )

    # adding a new columns
    new_col = (items_table.id * 2).name("new_col")
    assert sql_from_expr(items_table.select("id", "decimal", new_col)) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."id" * 2 AS "new_col" FROM'
            ' "dataset"."items" AS "t0"'
        ),
        ["id", "decimal", "new_col"],
    )

    # mutating table (add a new column computed from existing columns)
    assert sql_from_expr(
        items_table.mutate(double_id=items_table.id * 2).select("id", "double_id")
    ) == (
        'SELECT "t0"."id" AS "id", "t0"."id" * 2 AS "double_id" FROM "dataset"."items" AS "t0"',
        ["id", "double_id"],
    )

    # mutating table add new static column
    assert sql_from_expr(
        items_table.mutate(new_col=ibis.literal("static_value")).select("id", "new_col")
    ) == (
        'SELECT "t0"."id" AS "id", \'static_value\' AS "new_col" FROM "dataset"."items" AS "t0"',
        ["id", "new_col"],
    )

    # check filtering (preserves all columns)
    assert sql_from_expr(items_table.filter(items_table.id < 10)) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."other_decimal" AS'
            ' "other_decimal", "t0"."_dlt_load_id" AS "_dlt_load_id", "t0"."_dlt_id" AS "_dlt_id"'
            ' FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10'
        ),
        ALL_COLUMNS,
    )

    # filtering and selecting a single column
    assert sql_from_expr(items_table.filter(items_table.id < 10).select("id")) == (
        'SELECT "t0"."id" AS "id" FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10',
        ["id"],
    )

    # check filter "and" condition
    assert sql_from_expr(items_table.filter(items_table.id < 10).filter(items_table.id > 5)) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."other_decimal" AS'
            ' "other_decimal", "t0"."_dlt_load_id" AS "_dlt_load_id", "t0"."_dlt_id" AS "_dlt_id"'
            ' FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10 AND "t0"."id" > 5'
        ),
        ALL_COLUMNS,
    )

    # check filter "or" condition
    assert sql_from_expr(items_table.filter((items_table.id < 10) | (items_table.id > 5))) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."other_decimal" AS'
            ' "other_decimal", "t0"."_dlt_load_id" AS "_dlt_load_id", "t0"."_dlt_id" AS "_dlt_id"'
            ' FROM "dataset"."items" AS "t0" WHERE ("t0"."id" < 10) OR ("t0"."id" > 5)'
        ),
        ALL_COLUMNS,
    )

    # check group by and aggregate
    assert sql_from_expr(
        items_table.group_by("id")
        .having(items_table.count() >= 1000)
        .aggregate(sum_id=items_table.id.sum())
    ) == (
        (
            'SELECT "t1"."id" AS "id", "t1"."sum_id" AS "sum_id" FROM (SELECT "t0"."id" AS "id",'
            ' SUM("t0"."id") AS "sum_id", COUNT(*) AS "CountStar(items)" FROM "dataset"."items" AS'
            ' "t0" GROUP BY "t0"."id") AS "t1" WHERE "t1"."CountStar(items)" >= 1000'
        ),
        ["id", "sum_id"],
    )

    # sorting and ordering
    assert sql_from_expr(items_table.order_by("id", "decimal").limit(10)) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."other_decimal" AS'
            ' "other_decimal", "t0"."_dlt_load_id" AS "_dlt_load_id", "t0"."_dlt_id" AS "_dlt_id"'
            ' FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" ASC, "t0"."decimal" ASC LIMIT 10'
        ),
        ALL_COLUMNS,
    )

    # sort desc and asc
    assert sql_from_expr(items_table.order_by(ibis.desc("id"), ibis.asc("decimal")).limit(10)) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."other_decimal" AS'
            ' "other_decimal", "t0"."_dlt_load_id" AS "_dlt_load_id", "t0"."_dlt_id" AS "_dlt_id"'
            ' FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" DESC, "t0"."decimal" ASC LIMIT 10'
        ),
        ALL_COLUMNS,
    )

    # offset and limit
    assert sql_from_expr(items_table.order_by("id").limit(10, offset=5)) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."other_decimal" AS'
            ' "other_decimal", "t0"."_dlt_load_id" AS "_dlt_load_id", "t0"."_dlt_id" AS "_dlt_id"'
            ' FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" ASC LIMIT 10 OFFSET 5'
        ),
        ALL_COLUMNS,
    )

    # join
    assert sql_from_expr(
        items_table.join(double_items_table, items_table.id == double_items_table.id)[
            ["id", "double_id"]
        ]
    ) == (
        (
            'SELECT "t2"."id" AS "id", "t3"."double_id" AS "double_id" FROM "dataset"."items" AS'
            ' "t2" INNER JOIN "dataset"."double_items" AS "t3" ON "t2"."id" = "t3"."id"'
        ),
        ["id", "double_id"],
    )

    # subqueries
    assert sql_from_expr(
        items_table.filter(items_table.decimal.isin(double_items_table.di_decimal))
    ) == (
        (
            'SELECT "t0"."id" AS "id", "t0"."decimal" AS "decimal", "t0"."other_decimal" AS'
            ' "other_decimal", "t0"."_dlt_load_id" AS "_dlt_load_id", "t0"."_dlt_id" AS "_dlt_id"'
            ' FROM "dataset"."items" AS "t0" WHERE "t0"."decimal" IN (SELECT "t1"."di_decimal" AS'
            ' "di_decimal" FROM "dataset"."double_items" AS "t1")'
        ),
        ALL_COLUMNS,
    )

    # topk
    assert sql_from_expr(items_table.decimal.topk(10)) == (
        (
            'SELECT "t1"."decimal" AS "decimal", "t1"."decimal_count" AS "decimal_count" FROM'
            ' (SELECT "t0"."decimal" AS "decimal", COUNT(*) AS "decimal_count" FROM'
            ' "dataset"."items" AS "t0" GROUP BY "t0"."decimal") AS "t1" ORDER BY'
            ' "t1"."decimal_count" DESC LIMIT 10'
        ),
        ["decimal", "decimal_count"],
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

    # make sure the not implemented error is raised if the ibis backend can't be created
    try:
        ds_ = populated_pipeline.dataset()
        ibis_connection = ds_.ibis(read_only=True)
    except NotImplementedError:
        pytest.skip("ibis not implemented for this destination")
    except Exception as e:
        pytest.fail(f"Unexpected error raised: {e}")

    try:
        # garbage collect: we want all destructors to be fired
        import gc

        del ds_
        for _ in range(3):
            gc.collect()

        total_records = _total_records(populated_pipeline.destination.destination_type)

        map_i = lambda x: x
        if populated_pipeline.destination.destination_type == "dlt.destinations.snowflake":
            map_i = lambda x: x.upper()

        dataset_name = map_i(populated_pipeline.dataset_name)
        table_like_statement = None
        table_name_prefix = ""
        additional_tables = []

        # clickhouse has no datasets, but table prefixes and a sentinel table
        if populated_pipeline.destination.destination_type == "dlt.destinations.clickhouse":
            table_like_statement = dataset_name + "."
            table_name_prefix = dataset_name + "___"
            dataset_name = None
            additional_tables += ["dlt_sentinel_table"]

        # filesystem uses duckdb and views to map know tables. for other ibis will list
        # all available tables so both schemas tables are visible
        if populated_pipeline.destination.destination_type != "dlt.destinations.filesystem":
            # from aleph schema
            additional_tables += ["digits"]

        add_table_prefix = lambda x: table_name_prefix + x

        # databricks can't list tables (looks like internal ibis bug)
        if populated_pipeline.destination.destination_type != "dlt.destinations.databricks":
            # just do a basic check to see wether ibis can connect
            assert set(
                ibis_connection.list_tables(database=dataset_name, like=table_like_statement)
            ) == {
                add_table_prefix(map_i(x))
                for x in (
                    [
                        "_dlt_loads",
                        "_dlt_pipeline_state",
                        "_dlt_version",
                        "double_items",
                        "items",
                        "items__children",
                        "orderable_in_chain",
                    ]
                    + additional_tables
                )
            }

        table_name = add_table_prefix(map_i("items"))
        items_table = ibis_connection.table(table_name, database=dataset_name)
        assert items_table.count().to_pandas() == total_records

        # some of the destinations allow to set default schema/dataset
        try:
            items_table = ibis_connection.tables[table_name]
            assert items_table.count().to_pandas() == total_records
        except KeyError:
            if populated_pipeline.destination.destination_type not in ["dlt.destinations.mssql"]:
                raise
    finally:
        ibis_connection.disconnect()


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
@pytest.mark.skip("enable when we standardize behavior for non existing datasets")
def test_ibis_no_dataset(populated_pipeline: Pipeline) -> None:
    try:
        ds = populated_pipeline.dataset()
        ds._dataset_name = "no_dataset_" + uniq_id(4)
        ibis_connection = ds.ibis(read_only=True)
        ibis_connection.list_tables()
    except NotImplementedError:
        pytest.skip("ibis not implemented for this destination")
    except Exception as e:
        print(e)
        pass
    else:
        ibis_connection.disconnect()
        pytest.fail("Exception expected on opening non exiting dataset")


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_standalone_dataset(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline.destination.destination_type)

    # check dataset factory
    dataset = dlt.dataset(
        destination=populated_pipeline.destination,
        dataset_name=populated_pipeline.dataset_name,
        # use name otherwise aleph schema is loaded
        schema=populated_pipeline.default_schema_name,
    )

    # dataset.schema is only set once first accessed
    assert dataset._schema == populated_pipeline.default_schema_name
    dataset.schema
    assert isinstance(dataset.schema, dlt.Schema)

    # verify that sql client is lazily loaded
    assert not dataset._opened_sql_client
    table_relationship = dataset.items
    table = table_relationship.fetchall()
    assert len(table) == total_records
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that schema is not loaded when wrong name given
    dataset = dlt.dataset(
        destination=populated_pipeline.destination,
        dataset_name=populated_pipeline.dataset_name,
        schema="wrong_schema_name",
    )
    assert "items" not in dataset.schema.tables
    assert dataset.schema.name == "wrong_schema_name"

    # check that schema is loaded if no schema name given
    dataset = dlt.dataset(
        destination=populated_pipeline.destination,
        dataset_name=populated_pipeline.dataset_name,
    )
    # aleph is a secondary schema in the pipeline but because it was stored second
    # will be retrieved by default
    assert dataset.schema.name == "aleph"
    assert dataset.schema.tables["digits"]["write_disposition"] == "append"

    # check that there is no error when creating dataset without schema table
    dataset = dlt.dataset(
        destination=populated_pipeline.destination,
        dataset_name="unknown_dataset",
    )
    assert dataset.schema.name == "unknown_dataset"
    assert "items" not in dataset.schema.tables

    # NOTE: this breaks the following test, it will need to be fixed somehow
    # create a newer schema with different name and see whether this is loaded
    from dlt.common.schema import Schema
    from dlt.common.schema import utils

    other_schema = Schema("some_other_schema")
    other_schema.tables["other_table"] = utils.new_table("other_table")

    populated_pipeline._inject_schema(other_schema)
    populated_pipeline.default_schema_name = other_schema.name  # type: ignore[assignment]
    with populated_pipeline.destination_client() as client:
        client.update_stored_schema()

    dataset = dlt.dataset(
        destination=populated_pipeline.destination,
        dataset_name=populated_pipeline.dataset_name,
    )
    assert dataset.schema.name == "some_other_schema"
    assert "other_table" in dataset.schema.tables


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    configs,
    ids=lambda x: x.name,
)
def test_read_not_materialized_table(destination_config: DestinationTestConfiguration):
    @dlt.source
    def two_tables():
        @dlt.resource(
            columns=[{"name": "id", "data_type": "bigint", "nullable": True, "primary_key": True}],
            write_disposition="append",
            table_format=destination_config.table_format,
        )
        def table_1():
            yield {"id": 1}

        @dlt.resource(
            columns=[{"name": "id", "data_type": "bigint", "nullable": True}],
            write_disposition="replace",
            table_format=destination_config.table_format,
        )
        def table_3(make_data=False):
            return
            yield

        return table_1, table_3

    pipeline = destination_config.setup_pipeline(
        "test_pipeline_upfront_tables_two_loads",
        dataset_name="test_pipeline_upfront_tables_two_loads",
        dev_mode=True,
    )

    # create table without any data in it and try to access it. destination should not know this table
    # expected behavior is that table is not found
    schema = two_tables().discover_schema()

    # now we use this schema but load just one resource
    source = two_tables()
    # push state, table 3 not created
    pipeline.run(source.table_3, schema=schema, **destination_config.run_kwargs)

    with pytest.raises(DestinationUndefinedEntity):
        pipeline.dataset().table_3.fetchall()

    # now set table_3 so it has seen data
    with pipeline.dataset() as dataset_:
        schema = dataset_.schema
        schema.tables["table_3"]["x-normalizer"] = {"seen-data": True}

    # forces sql_client to map views. but data does not exist so must raise same exceptions
    with pytest.raises(DestinationUndefinedEntity):
        pipeline.dataset().table_3.fetchall()


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_naming_convention_propagation(destination_config: DestinationTestConfiguration):
    destination_ = destination_config.destination_factory(
        naming_convention="tests.common.cases.normalizers.title_case"
    )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="Read_test", dev_mode=True, destination=destination_
    )

    s = create_test_source(destination_config.destination_type, destination_config.table_format)
    pipeline.run(s, loader_file_format=destination_config.file_format)

    dataset_ = pipeline.dataset()
    df = dataset_.ItemS.df()
    assert df.columns.tolist()[0] == "ID"
    with dataset_.sql_client as client:
        assert client.dataset_name.startswith("Read_test")
        tables = client.native_connection.sql("SHOW TABLES;")
        assert "ItemS" in str(tables)
