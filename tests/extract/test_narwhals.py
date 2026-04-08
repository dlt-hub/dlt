import os

import pytest

import dlt
from dlt.extract.utils import get_data_item_format

polars = pytest.importorskip("polars")


@pytest.fixture(autouse=True)
def switch_to_fifo():
    """most of the following tests rely on the old default fifo next item mode"""
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"
    yield
    del os.environ["EXTRACT__NEXT_ITEM_MODE"]


def test_polars_to_arrow_conversion():
    """polars_to_arrow converts both DataFrame and LazyFrame"""
    from dlt.common.libs.narwhals import df_to_arrow

    df = polars.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    arrow_tbl = df_to_arrow(df)
    assert arrow_tbl.num_rows == 3
    assert arrow_tbl.column_names == ["a", "b"]

    # LazyFrame auto-collected
    lf = df.lazy().filter(polars.col("a") > 1)
    arrow_tbl = df_to_arrow(lf)
    assert arrow_tbl.num_rows == 2


def test_get_data_item_format_polars():
    """get_data_item_format returns 'arrow' for polars frames"""
    df = polars.DataFrame({"id": [1]})
    assert get_data_item_format(df) == "arrow"
    assert get_data_item_format([df]) == "arrow"

    lf = df.lazy()
    assert get_data_item_format(lf) == "arrow"
    assert get_data_item_format([lf]) == "arrow"


def test_wrap_additional_type_polars():
    """Polars frames are wrapped in a list by wrap_additional_type"""
    from dlt.extract.wrappers import wrap_additional_type

    df = polars.DataFrame({"id": [1]})
    assert wrap_additional_type(df) == [df]

    lf = df.lazy()
    assert wrap_additional_type(lf) == [lf]


def test_resource_yields_polars_dataframe():
    """Basic end-to-end: yield a polars DataFrame from a resource"""

    @dlt.resource
    def my_data():
        yield polars.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_basic", destination="duckdb", full_refresh=True
    )
    pipeline.run(my_data())

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM my_data") as cur:
            assert cur.fetchone()[0] == 3
        with c.execute_query("SELECT id, name FROM my_data ORDER BY id") as cur:
            rows = cur.fetchall()
            assert rows == [(1, "a"), (2, "b"), (3, "c")]


def test_resource_yields_polars_lazyframe():
    """Yield a polars LazyFrame - should be auto-collected"""

    @dlt.resource
    def lazy_data():
        lf = polars.LazyFrame({"id": [10, 20], "val": ["x", "y"]})
        yield lf.filter(polars.col("id") >= 10)

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_lazy", destination="duckdb", full_refresh=True
    )
    pipeline.run(lazy_data())

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM lazy_data") as cur:
            assert cur.fetchone()[0] == 2


def test_resource_yields_multiple_polars_frames():
    """Multiple yields of polars DataFrames"""

    @dlt.resource
    def multi_data():
        yield polars.DataFrame({"id": [1, 2]})
        yield polars.DataFrame({"id": [3, 4]})

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_multi", destination="duckdb", full_refresh=True
    )
    pipeline.run(multi_data())

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM multi_data") as cur:
            assert cur.fetchone()[0] == 4


def test_resource_polars_empty_dataframe():
    """Empty polars DataFrame should not crash"""

    @dlt.resource
    def empty_data():
        yield polars.DataFrame({"id": polars.Series([], dtype=polars.Int64)})

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_empty", destination="duckdb", full_refresh=True
    )
    pipeline.run(empty_data())


def test_polars_incremental():
    """Polars DataFrames work with dlt.sources.incremental"""

    @dlt.resource(primary_key="id")
    def events(updated_at=dlt.sources.incremental("updated_at")):
        yield polars.DataFrame(
            {
                "id": [1, 2, 3],
                "updated_at": [1, 2, 3],
            }
        )

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_inc", destination="duckdb", full_refresh=True
    )
    pipeline.run(events())

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM events") as cur:
            assert cur.fetchone()[0] == 3

    # second run: only new/equal rows should appear
    @dlt.resource(primary_key="id")
    def events2(updated_at=dlt.sources.incremental("updated_at")):
        yield polars.DataFrame(
            {
                "id": [3, 4, 5],
                "updated_at": [3, 4, 5],
            }
        )

    pipeline.run(events2().with_name("events"))

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM events") as cur:
            count = cur.fetchone()[0]
            # should have original 3 + new 2 (id=4,5), id=3 deduplicated
            assert count == 5


def test_polars_lazyframe_incremental():
    """Polars LazyFrames work with incremental"""

    @dlt.resource(primary_key="id")
    def lazy_events(updated_at=dlt.sources.incremental("updated_at")):
        lf = polars.LazyFrame(
            {
                "id": [1, 2, 3],
                "updated_at": [10, 20, 30],
            }
        )
        yield lf

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_lazy_inc", destination="duckdb", full_refresh=True
    )
    pipeline.run(lazy_events())

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM lazy_events") as cur:
            assert cur.fetchone()[0] == 3


def test_polars_write_disposition_replace():
    """Test replace write disposition with polars"""

    @dlt.resource(write_disposition="replace")
    def replaceable():
        yield polars.DataFrame({"id": [1, 2, 3]})

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_replace", destination="duckdb", full_refresh=True
    )
    pipeline.run(replaceable())
    pipeline.run(replaceable())

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM replaceable") as cur:
            assert cur.fetchone()[0] == 3


def test_polars_schema_inference():
    """Column types from polars map correctly"""

    @dlt.resource
    def typed_data():
        yield polars.DataFrame(
            {
                "int_col": polars.Series([1, 2], dtype=polars.Int64),
                "float_col": polars.Series([1.5, 2.5], dtype=polars.Float64),
                "str_col": polars.Series(["a", "b"], dtype=polars.Utf8),
                "bool_col": polars.Series([True, False], dtype=polars.Boolean),
            }
        )

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_types", destination="duckdb", full_refresh=True
    )
    pipeline.run(typed_data())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT int_col, float_col, str_col, bool_col FROM typed_data ORDER BY int_col"
        ) as cur:
            rows = cur.fetchall()
            assert rows[0] == (1, 1.5, "a", True)
            assert rows[1] == (2, 2.5, "b", False)


def test_polars_nested_struct():
    """Polars structs convert through arrow correctly"""

    @dlt.resource
    def struct_data():
        yield polars.DataFrame(
            {
                "id": [1, 2],
                "nested": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
            }
        )

    pipeline = dlt.pipeline(
        pipeline_name="polars_test_struct", destination="duckdb", full_refresh=True
    )
    pipeline.run(struct_data())

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT count(*) FROM struct_data") as cur:
            assert cur.fetchone()[0] == 2
