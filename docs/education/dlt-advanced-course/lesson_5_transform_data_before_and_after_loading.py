import marimo

__generated_with = "0.14.10"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        # Transforming and filtering the data [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_5_transform_data_before_and_after_loading.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_5_transform_data_before_and_after_loading.ipynb)

        In this lesson, we will take a look at various ways of doing data transformations and filtering of the data during and after the ingestion.

        dlt provides several ways of doing it during the ingestion:
        1. With custom query (applicable for `sql_database` source).
        2. With dlt special functions (`add_map` and `add_filter`).
        3. Via `@dlt.transformers`.
        4. With `pipeline.dataset()`.

        Let's review and compare those methods.
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ##  What you’ll learn:

        - How to limit rows at the source with SQL queries.
        - How to apply custom Python logic per record.
        - How to write transformations using functional and declarative APIs.
        - How to access and query your loaded data using `.dataset()`.
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ## Setup and initial Load
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        We will be using the `sql_database` source as an example and will connect to the public [MySQL RFam](https://www.google.com/url?q=https%3A%2F%2Fwww.google.com%2Furl%3Fq%3Dhttps%253A%252F%252Fdocs.rfam.org%252Fen%252Flatest%252Fdatabase.html) database. The RFam database contains publicly accessible scientific data on RNA structures.

        Let's perform an initial load:
        """)
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install -U "dlt[sql_database, duckdb]"
    # !pip install pymysql
    return


@app.cell
def _():
    import dlt

    from dlt.sources.sql_database import sql_database

    source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["family", "genome"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="sql_database_pipeline",
        destination="duckdb",
        dataset_name="sql_data",
    )
    load_info = pipeline.run(source)
    print(load_info)
    return dlt, pipeline, sql_database


@app.cell
def _(pipeline):
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM genome") as table:
            genome = table.df()
    genome
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        You can check your data count using `sql_client`:
        """)
    return


@app.cell
def _(pipeline):
    with pipeline.sql_client() as client_1:
        with client_1.execute_query("SELECT COUNT(*) AS total_rows FROM genome") as table_1:
            print(table_1.df())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ## **1. Filtering the data during the ingestion with `query_adapter_callback`**
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        Imagine a use-case where we're only interested in getting the genome data for bacterias. In this case, ingesting the whole `genome` table would be an unnecessary use of time and compute resources.
        """)
    return


@app.cell
def _(pipeline):
    with pipeline.sql_client() as client_2:
        with client_2.execute_query(
            "SELECT COUNT(*) AS total_rows FROM genome WHERE kingdom='bacteria'"
        ) as table_2:
            print(table_2.df())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        When ingesting data using the `sql_database` source, dlt runs a `SELECT` statement in the back, and using the `query_adapter_callback` parameter makes it possible to pass a `WHERE` clause inside the underlying `SELECT` statement.

        In this example, only the table `genome` is filtered on the column `kingdom`
        """)
    return


@app.cell
def _():
    from dlt.sources.sql_database.helpers import Table, SelectAny, SelectClause

    def query_adapter_callback(query: SelectAny, table: Table) -> SelectAny:
        if table.name == "genome":
            # Only select rows where the column kingdom has value "bacteria"
            return query.where(table.c.kingdom == "bacteria")
        # Use the original query for other tables
        return query

    return SelectAny, SelectClause, Table, query_adapter_callback


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        Attach it:
        """)
    return


@app.cell
def _(dlt, query_adapter_callback, sql_database):
    source_1 = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["genome"],
        query_adapter_callback=query_adapter_callback,
    )
    pipeline_1 = dlt.pipeline(
        pipeline_name="sql_database_pipeline_filtered",
        destination="duckdb",
        dataset_name="sql_data",
    )
    load_info_1 = pipeline_1.run(source_1, write_disposition="replace")
    print(pipeline_1.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        In the snippet above we created an SQL VIEW in your source database and extracted data from it. In that case, dlt will infer all column types and read data in shape you define in a view without any further customization.

        If creating a view is not feasible, you can fully rewrite the automatically generated query with an extended version of `query_adapter_callback`:
        """)
    return


@app.cell
def _(SelectAny, SelectClause, Table, dlt, sql_database):
    import sqlalchemy as sa

    def query_adapter_callback_1(query: SelectAny, table: Table) -> SelectClause:
        if table.name == "genome":
            return sa.text(f"SELECT * FROM {table.fullname} WHERE kingdom='bacteria'")
        return query

    source_2 = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["genome", "clan"],
        query_adapter_callback=query_adapter_callback_1,
    )
    pipeline_2 = dlt.pipeline(
        pipeline_name="sql_database_pipeline_filtered",
        destination="duckdb",
        dataset_name="sql_data",
    )
    load_info_2 = pipeline_2.run(source_2, write_disposition="replace")
    print(load_info_2)
    return (pipeline_2,)


@app.cell
def _(pipeline_2):
    with pipeline_2.sql_client() as client_3:
        with client_3.execute_query(
            "SELECT COUNT(*) AS total_rows, MAX(_dlt_load_id) as latest_load_id FROM clan"
        ) as table_3:
            print("Table clan:")
            print(table_3.df())
            print("\n")
        with client_3.execute_query(
            "SELECT COUNT(*) AS total_rows, MAX(_dlt_load_id) as latest_load_id FROM genome"
        ) as table_3:
            print("Table genome:")
            print(table_3.df())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ## **2. Transforming the data after extract and before load**
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        Since dlt is a Python library, it gives you a lot of control over the extracted data.

        You can attach any number of transformations that are evaluated on an item-per-item basis to your resource. The available transformation types:

        * `map` - transform the data item (resource.add_map).
        * `filter` - filter the data item (resource.add_filter).
        * `yield map` - a map that returns an iterator (so a single row may generate many rows - resource.add_yield_map).
        * `limit` - limits the number of records processed by a resource. Useful for testing or reducing data volume during development.

        For example, if we wanted to anonymize sensitive data before it is loaded into the destination, then we can write a python function for it and apply it to source or resource using the `.add_map()` method.

        [dlt documentation.](https://dlthub.com/docs/general-usage/resource#filter-transform-and-pivot-data)
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### Using `add_map`
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        In the table `clan`, we notice that there is a column `author` that we would like to anonymize.
        """)
    return


@app.cell
def _(pipeline_2):
    with pipeline_2.sql_client() as client_4:
        with client_4.execute_query("SELECT DISTINCT author FROM clan LIMIT 5") as table_4:
            print("Table clan:")
            print(table_4.df())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        We write a function in python that anonymizes a string
        """)
    return


@app.cell
def _():
    import hashlib
    from dlt.common.typing import TDataItem

    def pseudonymize_name(row: TDataItem) -> TDataItem:
        """
        Pseudonymization is a deterministic type of PII-obscuring.
        Its role is to allow identifying users by their hash,
        without revealing the underlying info.
        """
        # add a constant salt to generate
        salt = "WI@N57%zZrmk#88c"
        salted_string = row["author"] + salt
        sh = hashlib.sha256()
        sh.update(salted_string.encode())
        hashed_string = sh.digest().hex()
        row["author"] = hashed_string
        return row

    return TDataItem, hashlib, pseudonymize_name


@app.cell
def _(dlt, pseudonymize_name, sql_database):
    pipeline_3 = dlt.pipeline(
        pipeline_name="sql_database_pipeline_anonymized",
        destination="duckdb",
        dataset_name="sql_data",
    )
    source_3 = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam", table_names=["clan"]
    )
    source_3.clan.add_map(pseudonymize_name)
    info = pipeline_3.run(source_3)
    print(info)
    return (pipeline_3,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        After the pipeline has run, we can observe that the author column has been anonymized.
        """)
    return


@app.cell
def _(pipeline_3):
    with pipeline_3.sql_client() as client_5:
        with client_5.execute_query("SELECT DISTINCT author FROM clan LIMIT 5") as table_5:
            print("Table clan:")
            clan = table_5.df()
    clan
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        **Note:** If you're using the `pyarrow` or `connectorx` backend, the data is not processed item-by-item. Instead they're processed in batches, therefore your function should be adjusted. For example, for PyArrow chunks the function could be changed as follows:
        """)
    return


@app.cell
def _(dlt, hashlib, sql_database):
    import pyarrow as pa
    import pyarrow.compute as pc

    def pseudonymize_name_pyarrow(table: pa.Table) -> pa.Table:
        """
        Pseudonymizes the 'author' column in a PyArrow Table.
        """
        salt = "WI@N57%zZrmk#88c"
        df = table.to_pandas()
        df["author"] = (
            df["author"]
            .astype(str)
            .apply(lambda x: hashlib.sha256((x + salt).encode()).hexdigest())
        )
        new_table = pa.Table.from_pandas(df)
        return new_table

    pipeline_4 = dlt.pipeline(
        pipeline_name="sql_database_pipeline_anonymized1",
        destination="duckdb",
        dataset_name="sql_data",
    )
    source_4 = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["clan"],
        backend="pyarrow",
    )
    source_4.clan.add_map(pseudonymize_name_pyarrow)
    info_1 = pipeline_4.run(source_4)
    print(info_1)
    return (pipeline_4,)


@app.cell
def _(pipeline_4):
    with pipeline_4.sql_client() as client_6:
        with client_6.execute_query("SELECT DISTINCT author FROM clan LIMIT 5") as table_6:
            print("Table clan:")
            print(table_6.df())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### `add_map` vs `add_yield_map`

        The difference between `add_map` and `add_yield_map` matters when a transformation returns multiple records from a single input.

        #### **`add_map`**
        - Use `add_map` when you want to transform each item into exactly one item.
        - Think of it like modifying or enriching a row.
        - You use a regular function that returns one modified item.
        - Great for adding fields or changing structure.

        #### Example



        """)
    return


@app.cell
def _(TDataItem, dlt):
    from dlt.common.typing import TDataItems

    @dlt.resource
    def resource() -> TDataItems:
        yield [{"name": "Alice"}, {"name": "Bob"}]

    def add_greeting(item: TDataItem) -> TDataItem:
        item["greeting"] = f"Hello, {item['name']}!"
        return item

    resource.add_map(add_greeting)

    for row in resource():
        print(row)
    return (TDataItems,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        #### **`add_yield_map`**
        - Use `add_yield_map` when you want to turn one item into multiple items, or possibly no items.
        - Your function is a generator that uses yield.
        - Great for pivoting nested data, flattening lists, or filtering rows.

        #### Example

        """)
    return


@app.cell
def _(TDataItem, TDataItems, dlt):
    @dlt.resource
    def resource_1() -> TDataItems:
        yield [
            {"name": "Alice", "hobbies": ["reading", "chess"]},
            {"name": "Bob", "hobbies": ["cycling"]},
        ]

    def expand_hobbies(item: TDataItem) -> TDataItem:
        for hobby in item["hobbies"]:
            yield {"name": item["name"], "hobby": hobby}

    resource_1.add_yield_map(expand_hobbies)
    for row_1 in resource_1():
        print(row_1)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### Using `add_filter`
        `add_filter` function can be used similarly. The difference is that `add_filter` expects a function that returns a boolean value for each item. For example, to implement the same filtering we did with a query callback, we can use:
        """)
    return


@app.cell
def _(dlt, sql_database):
    import time

    source_5 = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam", table_names=["genome"]
    )
    pipeline_5 = dlt.pipeline(
        pipeline_name="sql_database_pipeline_filtered",
        destination="duckdb",
        dataset_name="sql_data",
    )
    source_5.genome.add_filter(lambda item: item["kingdom"] == "bacteria")
    load_info_3 = pipeline_5.run(source_5, write_disposition="replace")
    print(pipeline_5.last_trace)
    return (pipeline_5,)


@app.cell
def _(pipeline_5):
    with pipeline_5.sql_client() as client_7:
        with client_7.execute_query(
            "SELECT COUNT(*) AS total_rows, MAX(_dlt_load_id) as latest_load_id FROM genome"
        ) as table_7:
            print("Table genome:")
            genome_count = table_7.df()
    genome_count
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### Question 1:

        What is a `total_rows` in the example above?
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### Using `add_limit`

        If your resource loads thousands of pages of data from a REST API or millions of rows from a database table, you may want to sample just a fragment of it in order to quickly see the dataset with example data and test your transformations, etc.

        To do this, you limit how many items will be yielded by a resource (or source) by calling the `add_limit` method. This method will close the generator that produces the data after the limit is reached.
        """)
    return


@app.cell
def _(dlt, sql_database):
    source_6 = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["genome"],
        chunk_size=10,
    )
    pipeline_6 = dlt.pipeline(
        pipeline_name="sql_database_pipeline_filtered",
        destination="duckdb",
        dataset_name="sql_data",
    )
    source_6.genome.add_limit(1)
    load_info_4 = pipeline_6.run(source_6, write_disposition="replace")
    print(pipeline_6.last_trace)
    return (pipeline_6,)


@app.cell
def _(pipeline_6):
    with pipeline_6.sql_client() as client_8:
        with client_8.execute_query("SELECT * FROM genome") as table_8:
            genome_limited = table_8.df()
    genome_limited
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ## **3. Transforming data with `@dlt.transformer`**

        The main purpose of transformers is to create children tables with additional data requests, but they can also be used for data transformations especially if you want to keep the original data as well.
        """)
    return


@app.cell
def _(TDataItem, TDataItems, dlt, sql_database):
    @dlt.transformer()
    def batch_stats(items: TDataItems) -> TDataItem:
        """
        Pseudonymization is a deterministic type of PII-obscuring.
        Its role is to allow identifying users by their hash,
        without revealing the underlying info.
        """
        yield {
            "batch_length": len(items),
            "max_length": max([item["total_length"] for item in items]),
        }

    genome_resource = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam", chunk_size=10000
    ).genome
    pipeline_7 = dlt.pipeline(
        pipeline_name="sql_database_pipeline_with_transformers1",
        destination="duckdb",
        dataset_name="sql_data",
        dev_mode=True,
    )
    info_2 = pipeline_7.run([genome_resource, genome_resource | batch_stats])
    print(pipeline_7.last_trace)
    return (pipeline_7,)


@app.cell
def _(pipeline_7):
    with pipeline_7.sql_client() as client_9:
        with client_9.execute_query("SELECT * FROM batch_stats") as table_9:
            res = table_9.df()
    res
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ## **4. Transforming data after the load**

        Another possibility for data transformation is transforming data after the load. dlt provides several way of doing it:

        * using `sql_client`,
        * via `.dataset()` and ibis integration,
        * via [dbt integration](https://dlthub.com/docs/dlt-ecosystem/transformations/dbt/).

        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### SQL client

        You already saw examples of using dlt's SQL client. dlt gives you an opportunity to connect to your destination and execute any SQL query.
        """)
    return


@app.cell
def _(pipeline_7):
    with pipeline_7.sql_client() as client_10:
        client_10.execute_sql(
            " CREATE OR REPLACE TABLE genome_length AS\n            SELECT\n               "
            " SUM(total_length) AS total_total_length,\n                AVG(total_length) AS"
            " average_total_length\n            FROM\n                genome\n    "
        )
        with client_10.execute_query("SELECT * FROM genome_length") as table_10:
            genome_length = table_10.df()
    genome_length
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### Accessing loaded data with `pipeline.dataset()`

        Use `pipeline.dataset()` to inspect and work with your data in Python after loading.

        """)
    return


@app.cell
def _(pipeline_7):
    dataset = pipeline_7.dataset()
    dataset.row_counts().df()
    return (dataset,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        Note that `row_counts` didn't return the new table `genome_length`,
        """)
    return


@app.cell
def _(dataset):
    # Access as pandas
    df = dataset["genome"].df()
    df
    return


@app.cell
def _(dataset):
    # Access as Arrow
    arrow_table = dataset["genome_length"].arrow()
    arrow_table
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        You can also filter, limit, and select columns:
        """)
    return


@app.cell
def _(dataset):
    df_1 = dataset["genome"].select("kingdom", "ncbi_id").limit(10).df()
    df_1
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        To iterate over large data:
        """)
    return


@app.cell
def _(dataset):
    for chunk in dataset["genome"].iter_df(chunk_size=500):
        print(chunk.head())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        For more advanced users, this interface supports **Ibis expressions**, joins, and subqueries.
        """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ### Ibis integration

        Ibis is a powerful portable Python dataframe library. Learn more about what it is and how to use it in the [official documentation](https://ibis-project.org/).

        [dlt provides a way to use Ibis expressions natively](https://dlthub.com/docs/general-usage/dataset-access/ibis-backend) with a lot of destinations. Supported ones are:
        * Snowflake
        * DuckDB
        * MotherDuck
        * Postgres
        * Redshift
        * Clickhouse
        * MSSQL (including Synapse)
        * BigQuery
        """)
    return


app._unparsable_cell(
    r"""
    !pip install ibis-framework[duckdb]
    """,
    name="_",
)


@app.cell
def _(pipeline_7):
    dataset_1 = pipeline_7.dataset()
    dataset_name = pipeline_7.dataset_name
    ibis_connection = dataset_1.ibis()
    print(ibis_connection.list_tables(database=dataset_name))
    table_11 = ibis_connection.table("batch_stats", database=dataset_name)
    print(table_11.limit(2).execute())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
        ✅ ▶ Proceed to the [next lesson](https://colab.research.google.com/drive/1XT1xUIQIWj0nPWOmTixThgdXzi4vudce#forceEdit=true&sandboxMode=true)!
        """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
