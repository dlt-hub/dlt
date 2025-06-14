# flake8: noqa
import dlt
import pandas as pd
import pytest

from dlt.destinations.dataset import ReadableDBAPIDataset
from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)


@pytest.fixture(scope="function")
def pipeline() -> dlt.Pipeline:
    pipeline_name = "dataset_snippets_test"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, destination="duckdb", dataset_name="dataset_snippets_data"
    )

    pipeline.run(fruitshop_source())

    return pipeline


@pytest.fixture(scope="function")
def dataset(pipeline: dlt.Pipeline) -> ReadableDBAPIDataset:
    return pipeline.dataset()


def quick_start_example_snippet(pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START quick_start_example
    # Assuming you have a Pipeline object named 'pipeline'. You can create one with the dlt cli: dlt init fruitshop duckdb
    # and you have loaded the data of the fruitshop example source into the destination
    # the tables available in the destination are:
    # - customers
    # - inventory
    # - purchases

    # Step 1: Get the readable dataset from the pipeline
    dataset = pipeline.dataset()

    # Step 2: Access a table as a ReadableRelation
    customers_relation = dataset.customers  # Or dataset["customers"]

    # Step 3: Fetch the entire table as a Pandas DataFrame
    df = customers_relation.df()

    # Alternatively, fetch as a PyArrow Table
    arrow_table = customers_relation.arrow()
    # @@@DLT_SNIPPET_END quick_start_example


def getting_started_snippet(pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START getting_started
    # Get the readable dataset from the pipeline
    dataset = pipeline.dataset()

    # print the row counts of all tables in the destination as dataframe
    print(dataset.row_counts().df())
    # @@@DLT_SNIPPET_END getting_started


def accessing_tables_snippet(dataset: ReadableDBAPIDataset) -> None:
    # @@@DLT_SNIPPET_START accessing_tables
    # Using attribute access
    customers_relation = dataset.customers

    # Using item access
    customers_relation = dataset["customers"]
    # @@@DLT_SNIPPET_END accessing_tables


def fetch_entire_table_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers

    # @@@DLT_SNIPPET_START fetch_entire_table_df
    df = customers_relation.df()
    # @@@DLT_SNIPPET_END fetch_entire_table_df

    # @@@DLT_SNIPPET_START fetch_entire_table_arrow
    arrow_table = customers_relation.arrow()
    # @@@DLT_SNIPPET_END fetch_entire_table_arrow

    # @@@DLT_SNIPPET_START fetch_entire_table_fetchall
    items_list = customers_relation.fetchall()
    # @@@DLT_SNIPPET_END fetch_entire_table_fetchall


def iterating_chunks_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers
    # @@@DLT_SNIPPET_START iterating_df_chunks
    for df_chunk in customers_relation.iter_df(chunk_size=5):
        # Process each DataFrame chunk
        pass
    # @@@DLT_SNIPPET_END iterating_df_chunks

    # @@@DLT_SNIPPET_START iterating_arrow_chunks
    for arrow_chunk in customers_relation.iter_arrow(chunk_size=5):
        # Process each PyArrow chunk
        pass
    # @@@DLT_SNIPPET_END iterating_arrow_chunks

    # @@@DLT_SNIPPET_START iterating_fetch_chunks
    for items_chunk in customers_relation.iter_fetch(chunk_size=5):
        # Process each chunk of tuples
        pass
    # @@@DLT_SNIPPET_END iterating_fetch_chunks


def row_counts_snippet(dataset: ReadableDBAPIDataset) -> None:
    # @@@DLT_SNIPPET_START row_counts
    # print the row counts of all tables in the destination as dataframe
    print(dataset.row_counts().df())

    # or as tuples
    print(dataset.row_counts().fetchall())
    # @@@DLT_SNIPPET_END row_counts


def context_manager_snippet(dataset: ReadableDBAPIDataset) -> None:
    # @@@DLT_SNIPPET_START context_manager

    # the dataset context manager will keep the connection open
    # and close it after the with block is exited
    with dataset as dataset_:
        print(dataset.customers.limit(50).arrow())
        print(dataset.purchases.arrow())

    # @@@DLT_SNIPPET_END context_manager


def limiting_records_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers
    # @@@DLT_SNIPPET_START limiting_records
    # Get the first 50 items as a PyArrow table
    arrow_table = customers_relation.limit(50).arrow()
    # @@@DLT_SNIPPET_END limiting_records

    # @@@DLT_SNIPPET_START head_records
    df = customers_relation.head().df()
    # @@@DLT_SNIPPET_END head_records


def select_columns_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers
    # @@@DLT_SNIPPET_START select_columns
    # Select only 'id' and 'name' columns
    items_list = customers_relation.select("id", "name").fetchall()

    # Alternate notation with brackets
    items_list = customers_relation[["id", "name"]].fetchall()

    # Only get one column
    items_list = customers_relation[["name"]].fetchall()
    # @@@DLT_SNIPPET_END select_columns


def chain_operations_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers

    # @@@DLT_SNIPPET_START chain_operations
    # Select columns and limit the number of records
    arrow_table = customers_relation.select("id", "name").limit(50).arrow()
    # @@@DLT_SNIPPET_END chain_operations


def ibis_expressions_snippet(pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START ibis_expressions
    # now that ibis is installed, we can get a dataset with ibis relations
    dataset = pipeline.dataset()

    # get two relations
    customers_relation = dataset["customers"]
    purchases_relation = dataset["purchases"]

    # join them using an ibis expression
    joined_relation = customers_relation.join(
        purchases_relation, customers_relation.id == purchases_relation.customer_id
    )

    # now we can use the ibis expression to filter the data
    filtered_relation = joined_relation.filter(purchases_relation.quantity > 1)

    # we can inspect the query that will be used to read the data
    print(filtered_relation.query)

    # and finally fetch the data as a pandas dataframe, the same way we would do with a normal relation
    df = filtered_relation.df()

    # a few more examples

    # get all customers from berlin and london
    customers_relation.filter(customers_relation.city.isin(["berlin", "london"])).df()

    # limit and offset
    customers_relation.limit(10, offset=5).arrow()

    # mutate columns by adding a new colums that always is 10 times the value of the id column
    customers_relation.mutate(new_id=customers_relation.id * 10).df()

    # sort asc and desc
    import ibis

    customers_relation.order_by(ibis.desc("id"), ibis.asc("city")).limit(10)

    # group by and aggregate
    customers_relation.group_by("city").having(customers_relation.count() >= 3).aggregate(
        sum_id=customers_relation.id.sum()
    ).df()

    # subqueries
    customers_relation.filter(customers_relation.city.isin(["berlin", "london"])).df()
    # @@@DLT_SNIPPET_END ibis_expressions


def fetch_one_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers
    # @@@DLT_SNIPPET_START fetch_one
    record = customers_relation.fetchone()
    # @@@DLT_SNIPPET_END fetch_one


def fetch_many_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers
    # @@@DLT_SNIPPET_START fetch_many
    records = customers_relation.fetchmany(10)
    # @@@DLT_SNIPPET_END fetch_many


def iterating_with_limit_and_select_snippet(dataset: ReadableDBAPIDataset) -> None:
    customers_relation = dataset.customers
    # @@@DLT_SNIPPET_START iterating_with_limit_and_select
    # Dataframes
    for df_chunk in customers_relation.select("id", "name").limit(100).iter_df(chunk_size=20): ...

    # Arrow tables
    for arrow_table in (
        customers_relation.select("id", "name").limit(100).iter_arrow(chunk_size=20)
    ): ...

    # Python tuples
    for records in customers_relation.select("id", "name").limit(100).iter_fetch(chunk_size=20):
        # Process each modified DataFrame chunk
        ...
    # @@@DLT_SNIPPET_END iterating_with_limit_and_select


def custom_sql_snippet(dataset: ReadableDBAPIDataset) -> None:
    # @@@DLT_SNIPPET_START custom_sql
    # Join 'customers' and 'purchases' tables
    custom_relation = dataset(
        "SELECT * FROM customers JOIN purchases ON customers.id = purchases.customer_id"
    )
    arrow_table = custom_relation.arrow()
    # @@@DLT_SNIPPET_END custom_sql


def loading_to_pipeline_snippet(dataset: ReadableDBAPIDataset) -> None:
    # @@@DLT_SNIPPET_START loading_to_pipeline
    # Create a readable relation with a limit of 1m rows
    limited_customers_relation = dataset.customers.limit(1_000_000)

    # Create a new pipeline
    other_pipeline = dlt.pipeline(pipeline_name="other_pipeline", destination="duckdb")

    # We can now load these 1m rows into this pipeline in 10k chunks
    other_pipeline.run(
        limited_customers_relation.iter_arrow(chunk_size=10_000), table_name="limited_customers"
    )
    # @@@DLT_SNIPPET_END loading_to_pipeline
