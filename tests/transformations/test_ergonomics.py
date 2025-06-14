from typing import Generator

import pytest
import sqlglot
import pandas as pd
import ibis
import narwhals as nw
import polars as pl
from ibis import ir
from sqlglot import exp

import dlt
from dlt.extract.hints import SqlModel
from dlt.transformations.ergonomics import transformation
from dlt.transformations.transformation import DltTransformationResource


@dlt.resource
def items(n_times: int = 1):
    rows = (
        {"id": 0, "value": "foo"},
        {"id": 1, "value": "bar"},
        {"id": 2, "value": "baz"},
    )
    for _ in range(n_times):
        yield from rows


# def test_standalone_lazy_execution():
#     """Lazy transformation functions should return the query expression"""
#     query = total_value_length_lazy()
#     assert query == "SELECT SUM(LENGTH(value)) AS total_length FROM items"


@pytest.fixture
def executed_extract_and_load_pipeline() -> Generator[dlt.Pipeline, None, None]:
    extract_pipeline = dlt.pipeline("el", destination="duckdb")
    extract_pipeline.run([items()])
    yield extract_pipeline


@pytest.fixture
def extract_and_load_dataset(executed_extract_and_load_pipeline: dlt.Pipeline) -> dlt.Dataset:
    dataset = executed_extract_and_load_pipeline.dataset()
    yield dataset


def test_dataset_access(executed_extract_and_load_pipeline: dlt.Pipeline):
    dataset_name = "el_dataset"
    dataset = dlt.Dataset(
        destination=executed_extract_and_load_pipeline.destination,
        dataset_name=dataset_name,
    )
    assert dataset_name == dataset.name
    assert dataset.table("items") is not None
    with pytest.raises(KeyError):
        dataset.table("unknown_table")


def test_dataset_table_access(extract_and_load_dataset: dlt.Dataset):
    relation = extract_and_load_dataset.table("items")
    assert isinstance(relation, dlt.destinations.dataset.relation.ReadableDBAPIRelation)

    ibis_table = extract_and_load_dataset.table("items", type_="ibis")
    assert isinstance(ibis_table, ir.Table)


def test_lazy_sql_transformation():
    """The function decorated  with `@dlt.transformation` returns
    something useful for development outside `pipeline.run()`
    """

    # NOTE this is the "streamlined" transformation decorator
    # @transformation
    def lazy_sql_transformation() -> Generator[str, None, None]:
        """Eager transformation using raw SQL"""
        yield "SELECT SUM(LENGTH(value)) AS total_length FROM items"

    # this is a non-decorated transformation function
    query_generator = lazy_sql_transformation()
    raw_queries = list(query_generator)
    assert len(raw_queries) == 1
    assert raw_queries[0] == "SELECT SUM(LENGTH(value)) AS total_length FROM items"

    # decorated function
    transformation_resource = transformation(lazy_sql_transformation)
    assert isinstance(transformation_resource, DltTransformationResource)

    sql_models = list(transformation_resource)
    assert len(sql_models) == 1
    assert isinstance(sql_models[0], SqlModel)
    assert isinstance(sql_models[0].query, exp.Select)

    assert sqlglot.parse_one(raw_queries[0]) == sql_models[0].query


def test_lazy_ibis_transformation(extract_and_load_dataset: dlt.Dataset):
    """Ibis is special because it needs tables to produce queries.
    This a unique constraint for lazy transformations.

    Tables can be bound or unbound. We produce unbound tables from
    the dlt schema instead of poking the destination. We pass these
    values using a `dlt.Dataset` argument
    """

    # @transformation
    def lazy_ibis_transformation(dataset: dlt.Dataset) -> Generator[ir.Table, None, None]:
        yield (
            dataset.table("items", type_="ibis")
            .mutate(total_length=ibis._.value.length().sum())
            .select("total_length")
        )

    query_generator = lazy_ibis_transformation(extract_and_load_dataset)
    raw_queries = list(query_generator)
    assert len(raw_queries) == 1
    assert isinstance(raw_queries[0], ir.Table)

    # decorated function
    transformation_resource = transformation(lazy_ibis_transformation)
    assert isinstance(transformation_resource, DltTransformationResource)

    sql_models = list(transformation_resource(extract_and_load_dataset))
    assert len(sql_models) == 1
    assert isinstance(sql_models[0], SqlModel)
    assert isinstance(sql_models[0].query, exp.Select)

    raw_query_sql_via_ibis = str(ibis.to_sql(raw_queries[0]))
    sqlglot_via_ibis = sqlglot.parse_one(raw_query_sql_via_ibis)
    # NOTE queries are not equal, should check results for equivalence
    # assert sqlglot_via_ibis == sql_models[0].query


def test_lazy_narwhals_transformation(extract_and_load_dataset: dlt.Dataset):
    """Narwhals allows to use the Polars API to transform Ibis data.
    Therefore, we still have the constraint of passing the dataset
    """

    # @transformation
    def lazy_narhwals_transformation(dataset: dlt.Dataset) -> Generator[nw.LazyFrame, None, None]:
        # NOTE Narwhals-Ibis integration is difficult to debug + lazy + unbound makes it harder
        yield (
            dataset.table("items", type_="ibis_narwhals").select(
                nw.col("value").str.len_chars().sum().alias("total_length")
            )
        )

    query_generator = lazy_narhwals_transformation(extract_and_load_dataset)
    raw_queries = list(query_generator)
    assert len(raw_queries) == 1
    assert isinstance(raw_queries[0], nw.LazyFrame)

    # decorated function
    transformation_resource = transformation(lazy_narhwals_transformation)
    assert isinstance(transformation_resource, DltTransformationResource)

    sql_models = list(transformation_resource(extract_and_load_dataset))
    assert len(sql_models) == 1
    assert isinstance(sql_models[0], SqlModel)
    assert isinstance(sql_models[0].query, exp.Select)

    ibis_equivalent = (
        extract_and_load_dataset.table("items", type_="ibis")
        .mutate(total_length=ibis._.value.length().sum())
        .select("total_length")
    )
    # TODO those are not directly equivalent because narwhals uses polars
    # query optimization engine; check result equivalence
    # assert ibis_equivalent == raw_queries[0].to_native()
    # ibis_to_sqlglot_equivalent = ...
    # assert ibis_to_sqlglot_equivalent == sql_models[0].query


def test_eager_pandas_transformation(extract_and_load_dataset: dlt.Dataset):
    """Eager transformations necessarily take a dataset input. If they don't
    require a dataset input, it's semantically a `@dlt.resource`.

    User can manage size by loading data in chunks. Typically, it would
    return
    """

    def eager_pandas_transformation(dataset: dlt.Dataset) -> Generator[pd.DataFrame, None, None]:
        df = dataset.table("items", type_="pandas")
        total_length = df["value"].str.len().sum()
        yield pd.DataFrame([{"total_length": total_length}])

    df_generator = eager_pandas_transformation(extract_and_load_dataset)
    raw_dfs = list(df_generator)
    assert len(raw_dfs) == 1
    assert isinstance(raw_dfs[0], pd.DataFrame)
    pd.testing.assert_frame_equal(raw_dfs[0], pd.DataFrame([{"total_length": 9}]))

    # decorated function
    transformation_resource = transformation(eager_pandas_transformation)
    assert isinstance(transformation_resource, DltTransformationResource)

    data_chunks = list(transformation_resource(extract_and_load_dataset))
    assert len(data_chunks) == 1
    assert isinstance(data_chunks[0], pd.DataFrame)
    pd.testing.assert_frame_equal(data_chunks[0], raw_dfs[0])


def test_eager_narwhals_transformation(extract_and_load_dataset: dlt.Dataset):
    """Narwhals can be used eagerly and leverage several backends using a single API
    for example: pandas, polars, daft, cuDF, Modin.

    Note that it's the same query as LazyFrame Narwhals in this case.
    """

    def eager_narwhals_transformation(dataset: dlt.Dataset) -> Generator[nw.DataFrame, None, None]:
        yield (
            dataset.table("items", type_="polars_narwhals").select(
                nw.col("value").str.len_chars().sum().alias("total_length")
            )
        )

    df_generator = eager_narwhals_transformation(extract_and_load_dataset)
    raw_dfs = list(df_generator)
    assert len(raw_dfs) == 1
    assert isinstance(raw_dfs[0], nw.DataFrame)
    assert pl.DataFrame({"total_length": 9}).equals(raw_dfs[0].to_native())

    # decorated function
    transformation_resource = transformation(eager_narwhals_transformation)
    assert isinstance(transformation_resource, DltTransformationResource)

    data_chunks = list(transformation_resource(extract_and_load_dataset))
    assert len(data_chunks) == 1
    assert isinstance(data_chunks[0], pl.DataFrame)  # native format is retrieved
    assert data_chunks[0].equals(raw_dfs[0].to_native())


def test_eager_chunked_pandas_transformation():
    """This showcases how we can manage memory by chunking the loaded data.

    We need to ingest many more rows. From trial-and-error, 1 chunk has 2048 rows in this case.
    This may be DuckDB-specific and dependent on the number of columns and their type
    """
    extract_pipeline = dlt.pipeline("el", destination="duckdb")
    # we load 3000 rows, so enough for two chunks
    extract_pipeline.run([items(n_times=1000)])
    extract_and_load_dataset = extract_pipeline.dataset()

    def eager_pandas_transformation(dataset: dlt.Dataset) -> Generator[pd.DataFrame, None, None]:
        # TODO we could improve the API, such that chunking is defined outside the function
        # i.e., call `eager_pandas_transformation()` in a loop instead of having the loop inside
        for chunk in dataset.iter_table("items", type_="pandas", chunk_size=1):
            total_length = chunk["value"].str.len().sum()
            yield pd.DataFrame([{"total_length": total_length}])

    df_generator = eager_pandas_transformation(extract_and_load_dataset)
    raw_dfs = list(df_generator)
    assert len(raw_dfs) == 2
    assert all(isinstance(df, pd.DataFrame) for df in raw_dfs)
    pd.testing.assert_frame_equal(raw_dfs[0], pd.DataFrame([{"total_length": 6144}]))
    pd.testing.assert_frame_equal(raw_dfs[1], pd.DataFrame([{"total_length": 2856}]))

    # decorated function
    transformation_resource = transformation(eager_pandas_transformation)
    assert isinstance(transformation_resource, DltTransformationResource)

    data_chunks = list(transformation_resource(extract_and_load_dataset))
    assert len(data_chunks) == 2
    assert all(isinstance(data_chunk, pd.DataFrame) for data_chunk in data_chunks)
    pd.testing.assert_frame_equal(data_chunks[0], raw_dfs[0])
    pd.testing.assert_frame_equal(data_chunks[1], raw_dfs[1])


def test_eager_chunked_narwhals_transformation(extract_and_load_dataset: dlt.Dataset):
    """This showcases how we can manage memory by chunking the loaded data.

    Contrary to `.iter_df()`, with `.iter_arrow()` chunk_size is the number of records
    """

    def eager_polars_transformation(dataset: dlt.Dataset) -> Generator[nw.DataFrame, None, None]:
        for chunk in dataset.iter_table("items", type_="polars_narwhals", chunk_size=1):
            yield (chunk.select(nw.col("value").str.len_chars().sum().alias("total_length")))

    df_generator = eager_polars_transformation(extract_and_load_dataset)
    raw_dfs = list(df_generator)
    assert len(raw_dfs) == 3
    assert all(isinstance(df, nw.DataFrame) for df in raw_dfs)
    assert pl.DataFrame({"total_length": 3}).equals(raw_dfs[0].to_native())
    assert pl.DataFrame({"total_length": 3}).equals(raw_dfs[1].to_native())

    # decorated function
    transformation_resource = transformation(eager_polars_transformation)
    assert isinstance(transformation_resource, DltTransformationResource)

    data_chunks = list(transformation_resource(extract_and_load_dataset))
    assert len(data_chunks) == 3
    # we get polars type
    assert all(isinstance(data_chunk, pl.DataFrame) for data_chunk in data_chunks)
    assert data_chunks[0].equals(raw_dfs[0].to_native())
    assert data_chunks[1].equals(raw_dfs[1].to_native())
