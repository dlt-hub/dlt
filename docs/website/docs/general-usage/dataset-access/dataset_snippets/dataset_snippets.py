# flake8: noqa
import dlt
import pytest

from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
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
def dataset(pipeline: dlt.Pipeline) -> dlt.Dataset:
    return pipeline.dataset()


@pytest.fixture(scope="function")
def default_dataset(pipeline: dlt.Pipeline) -> dlt.Dataset:
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
    customers_relation = dataset.table("customers")

    # Step 3: Fetch the entire table as a Pandas DataFrame
    df = customers_relation.df()  # or customers_relation.df(chunk_size=50)

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


def accessing_tables_snippet(dataset: dlt.Dataset) -> None:
    # @@@DLT_SNIPPET_START accessing_tables
    # Using `table` method`
    customers_relation = dataset.table("customers")

    # Using item access
    customers_relation = dataset["customers"]
    # @@@DLT_SNIPPET_END accessing_tables


def fetch_entire_table_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")

    # @@@DLT_SNIPPET_START fetch_entire_table_df
    df = customers_relation.df()
    # @@@DLT_SNIPPET_END fetch_entire_table_df

    # @@@DLT_SNIPPET_START fetch_entire_table_arrow
    arrow_table = customers_relation.arrow()
    # @@@DLT_SNIPPET_END fetch_entire_table_arrow

    # @@@DLT_SNIPPET_START fetch_entire_table_fetchall
    items_list = customers_relation.fetchall()
    # @@@DLT_SNIPPET_END fetch_entire_table_fetchall


def iterating_chunks_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")
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


def row_counts_snippet(dataset: dlt.Dataset) -> None:
    # @@@DLT_SNIPPET_START row_counts
    # print the row counts of all tables in the destination as dataframe
    print(dataset.row_counts().df())

    # or as tuples
    print(dataset.row_counts().fetchall())
    # @@@DLT_SNIPPET_END row_counts


def context_manager_snippet(dataset: dlt.Dataset) -> None:
    # @@@DLT_SNIPPET_START context_manager

    # the dataset context manager will keep the connection open
    # and close it after the with block is exited
    with dataset:
        print(dataset.table("customers").limit(50).arrow())
        print(dataset.table("purchases").arrow())

    # @@@DLT_SNIPPET_END context_manager


def limiting_records_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")
    # @@@DLT_SNIPPET_START limiting_records
    # Get the first 50 items as a PyArrow table
    arrow_table = customers_relation.limit(50).arrow()
    # @@@DLT_SNIPPET_END limiting_records

    # @@@DLT_SNIPPET_START head_records
    df = customers_relation.head().df()
    # @@@DLT_SNIPPET_END head_records


def select_columns_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")
    # @@@DLT_SNIPPET_START select_columns
    # Select only 'id' and 'name' columns
    items_list = customers_relation.select("id", "name").fetchall()

    # Alternate notation with brackets
    items_list = customers_relation[["id", "name"]].fetchall()

    # Only get one column
    items_list = customers_relation[["name"]].fetchall()
    # @@@DLT_SNIPPET_END select_columns


def order_by_snippet(default_dataset: dlt.Dataset) -> None:
    customers_relation = default_dataset.table("customers")
    # @@@DLT_SNIPPET_START order_by
    # Order by 'id'
    ordered_list = customers_relation.order_by("id").fetchall()
    # @@@DLT_SNIPPET_END order_by


def filter_snippet(default_dataset: dlt.Dataset) -> None:
    customers_relation = default_dataset.table("customers")
    # @@@DLT_SNIPPET_START filter
    # Filter by 'id'
    filtered = customers_relation.where("id", "in", [3, 1, 7]).fetchall()

    # Filter with raw SQL string
    filtered = customers_relation.where("id = 1").fetchall()

    # Filter with sqlglot expression
    import sqlglot.expressions as sge

    expr = sge.EQ(
        this=sge.Column(this=sge.to_identifier("id", quoted=True)),
        expression=sge.Literal.number("7"),
    )
    filtered = customers_relation.where(expr).fetchall()
    # @@@DLT_SNIPPET_END filter


def aggregate_snippet(default_dataset: dlt.Dataset) -> None:
    customers_relation = default_dataset.table("customers")
    # @@@DLT_SNIPPET_START aggregate

    # Get max 'id'
    max_id = customers_relation.select("id").max().fetchscalar()

    # Get min 'id'
    min_id = customers_relation.select("id").min().fetchscalar()

    # @@@DLT_SNIPPET_END aggregate


def chain_operations_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")

    # @@@DLT_SNIPPET_START chain_operations
    # Select columns and limit the number of records
    arrow_table = customers_relation.select("id", "name").limit(50).arrow()
    # @@@DLT_SNIPPET_END chain_operations


def ibis_expressions_snippet(pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START ibis_expressions
    # now that ibis is installed, we can get ibis unbound tables from the dataset
    dataset = pipeline.dataset()

    # get two table expressions
    customers_expression = dataset.table("customers").to_ibis()
    purchases_expression = dataset.table("purchases").to_ibis()

    # join them using an ibis expression
    join_expression = customers_expression.join(
        purchases_expression, customers_expression.id == purchases_expression.customer_id
    )

    # now we can use the ibis expression to filter the data
    filtered_expression = join_expression.filter(purchases_expression.quantity > 1)

    # we can pass the expression back to the dataset to get a relation that can be executed
    relation = dataset(filtered_expression)
    # and we can inspect the query that will be used to read the data
    print(relation)

    # and finally fetch the data as a pandas dataframe, the same way we would do with a normal relation
    print(relation.df())

    # a few more examples

    # get all customers from berlin and london and load them as a dataframe
    expr = customers_expression.filter(customers_expression.city.isin(["berlin", "london"]))
    print(dataset(expr).df())

    # limit and offset, then load as an arrow table
    expr = customers_expression.limit(10, offset=5)
    print(dataset(expr).arrow())

    # mutate columns by adding a new colums that always is 10 times the value of the id column
    expr = customers_expression.mutate(new_id=customers_expression.id * 10)
    print(dataset(expr).df())

    # sort asc and desc
    import ibis

    expr = customers_expression.order_by(ibis.desc("id"), ibis.asc("city")).limit(10)
    print(dataset(expr).df())

    # group by and aggregate
    expr = (
        customers_expression.group_by("city")
        .having(customers_expression.count() >= 3)
        .aggregate(sum_id=customers_expression.id.sum())
    )
    print(dataset(expr).df())

    # subqueries
    expr = customers_expression.filter(customers_expression.city.isin(["berlin", "london"]))
    print(dataset(expr).df())
    # @@@DLT_SNIPPET_END ibis_expressions


def fetch_one_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")
    # @@@DLT_SNIPPET_START fetch_one
    record = customers_relation.fetchone()
    # @@@DLT_SNIPPET_END fetch_one


def fetch_many_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")
    # @@@DLT_SNIPPET_START fetch_many
    records = customers_relation.fetchmany(10)
    # @@@DLT_SNIPPET_END fetch_many


def iterating_with_limit_and_select_snippet(dataset: dlt.Dataset) -> None:
    customers_relation = dataset.table("customers")
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


def custom_sql_snippet(dataset: dlt.Dataset) -> None:
    # @@@DLT_SNIPPET_START custom_sql
    # Join 'customers' and 'purchases' tables and filter by quantity
    query = """
    SELECT *  
        FROM customers 
    JOIN purchases 
        ON customers.id = purchases.customer_id
    WHERE purchases.quantity > 1
    """
    joined_relation = dataset(query)
    # @@@DLT_SNIPPET_END custom_sql


def loading_to_pipeline_snippet(dataset: dlt.Dataset) -> None:
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
