import marimo

__generated_with = "0.14.10"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Building custom sources using SQL Databases [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_2_custom_sources_sql_databases_.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_2_custom_sources_sql_databases_.ipynb)

        This lesson covers building flexible and powerful custom sources using the `sql_database` verified source.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ![Lesson_2_Custom_sources_SQL_Databases_img1](https://storage.googleapis.com/dlt-blog-images/dlt-advanced-course/Lesson_2_Custom_sources_SQL_Databases_img1.png)
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""

        ## What you will learn

        - How to build a custom pipeline using SQL sources
        - How to use `query_adapter_callback`, `table_adapter_callback`, and `type_adapter_callback`
        - How to load only new data with incremental loading

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Setup & install dlt:
        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install pymysql duckdb dlt
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Step 1: Load data from SQL Databases
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        We’ll use the [Rfam MySQL public DB](https://docs.rfam.org/en/latest/database.html) and load it into DuckDB:
        """
    )
    return


@app.cell
def _():
    from typing import Any
    from dlt.sources.sql_database import sql_database
    import dlt

    _source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["family"],
    )
    pipeline = dlt.pipeline(
        pipeline_name="sql_database_example",
        destination="duckdb",
        dataset_name="sql_data",
        dev_mode=True,
    )
    load_info = pipeline.run(_source)
    print(load_info)
    return Any, dlt, pipeline, sql_database


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Explore the `family` table:
        """
    )
    return


@app.cell
def _(pipeline):
    pipeline.dataset().family.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Step 2: Customize SQL queries with `query_adapter_callback`

        You can fully rewrite or modify the SQL SELECT statement per table.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Filter rows using a WHERE clause
        """
    )
    return


@app.cell
def _():
    from sqlalchemy import text
    from dlt.sources.sql_database.helpers import SelectClause, Table

    def query_adapter_callback(query: SelectClause, table: Table) -> SelectClause:
        return text(f"SELECT * FROM {table.fullname} WHERE rfam_id like '%bacteria%'")

    return Table, query_adapter_callback


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        To be able to use `sql_database` and not have to declare the connection string each time, we save it as an environment variable. This can also (should preferably) be done in `secrets.toml`
        """
    )
    return


@app.cell
def _():
    import os

    os.environ[
        "SOURCES__SQL_DATABASE__CREDENTIALS"
    ] = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    return


@app.cell
def _(query_adapter_callback, sql_database):
    filtered_resource = sql_database(
        query_adapter_callback=query_adapter_callback, table_names=["family"]
    )
    return (filtered_resource,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Let's save this filtered data:
        """
    )
    return


@app.cell
def _(filtered_resource, pipeline):
    _info = pipeline.run(filtered_resource, table_name="bacterias")
    print(_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Explore the data:
        """
    )
    return


@app.cell
def _(pipeline):
    pipeline.dataset().bacterias.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **Question 1**:

        How many rows are present in the `bacterias` table?

        >Answer this question and select the correct option in the homework Quiz.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Step 3: Modify table schema with `table_adapter_callback`

        Add columns, change types, or transform schema using this hook.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Example: Add computed column `max_timestamp`
        """
    )
    return


@app.cell
def _(Any, Table):
    import sqlalchemy as sa

    def add_max_timestamp(table: Table) -> Any:
        max_ts = sa.func.greatest(table.c.created, table.c.updated).label(
            "max_timestamp"
        )
        subq = sa.select(*table.c, max_ts).subquery()
        return subq

    return add_max_timestamp, sa


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Use it with `sql_table`:
        """
    )
    return


@app.cell
def _(add_max_timestamp, dlt, pipeline):
    from dlt.sources.sql_database import sql_table

    table = sql_table(
        table="family",
        table_adapter_callback=add_max_timestamp,
        incremental=dlt.sources.incremental("max_timestamp"),
    )
    _info = pipeline.run(table, table_name="family_with_max_timestamp")
    print(_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Let's check out if this column exists!
        """
    )
    return


@app.cell
def _(pipeline):
    pipeline.dataset().family_with_max_timestamp.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Step 4: Adapt column data types with `type_adapter_callback`

        When the default types don’t match what you want in the destination, you can remap them.

        Let's look at the schema that has already been loaded:
        """
    )
    return


@app.cell
def _(pipeline):
    schema = pipeline.default_schema.to_dict()["tables"]["family"]["columns"]
    for _column in schema:
        print(schema[_column]["name"], ":", schema[_column]["data_type"])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Lets change `hmm_lambda` from decimal to float.

        💡 Quick fyi: The `float` data type is:
        - Fast and uses less space
        - But it's approximate — you may get 0.30000000000000004 instead of 0.3
        - Bad for money, great for probabilities, large numeric ranges, scientific values
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Example: Change data types
        """
    )
    return


@app.cell
def _(Any, sa):
    from sqlalchemy.types import Float

    def type_adapter_callback(sql_type: Any) -> Any:
        if isinstance(sql_type, sa.Numeric):
            return Float
        return sql_type

    return (type_adapter_callback,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Use it with `sql_database`:
        """
    )
    return


@app.cell
def _(pipeline, sql_database, type_adapter_callback):
    new_source = sql_database(
        type_adapter_callback=type_adapter_callback, table_names=["family"]
    )
    _info = pipeline.run(new_source, table_name="type_changed_family")
    print(_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        👀 Can you see how the column data types have changed?
        """
    )
    return


@app.cell
def _(pipeline):
    schema1 = pipeline.default_schema.to_dict()["tables"]["family"]["columns"]
    schema2 = pipeline.default_schema.to_dict()["tables"]["type_changed_family"][
        "columns"
    ]
    _column = "trusted_cutoff"
    print(
        "For table 'family':",
        schema1[_column]["name"],
        ":",
        schema1[_column]["data_type"],
    )
    print(
        "For table 'type_changed_family':",
        schema2[_column]["name"],
        ":",
        schema2[_column]["data_type"],
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **Question 2**:

        How many columns had their type changed in the `type_changed_family` table?

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Step 5: Incremental loads with `sql_database`
        Track only new rows using a timestamp or ID column.

        We'll also be looking at where these incremental values are stored.

        Hint: they are stored in [dlt state](https://dlthub.com/docs/general-usage/state).
        """
    )
    return


@app.cell
def _():
    import json

    with open(
        "/var/dlt/pipelines/sql_database_example/state.json", "r", encoding="utf-8"
    ) as _f:
        _data = json.load(_f)
    _data["sources"]["sql_database"]["resources"]["family"]["incremental"].keys()
    return (json,)


@app.cell
def _(dlt, pipeline, sql_database):
    import pendulum

    _source = sql_database(table_names=["family"])
    _source.family.apply_hints(
        incremental=dlt.sources.incremental(
            "updated", initial_value=pendulum.datetime(2024, 1, 1)
        )
    )
    _info = pipeline.run(_source)
    print(_info)
    return


@app.cell
def _(json):
    with open(
        "/var/dlt/pipelines/sql_database_example/state.json", "r", encoding="utf-8"
    ) as _f:
        _data = json.load(_f)
    _data["sources"]["sql_database"]["resources"]["family"]["incremental"].keys()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## **Rename tables for `sql_database` source**


        """
    )
    return


@app.cell
def _(dlt, sql_database):
    _source = sql_database(table_names=["family"])
    for _resource_name, resource in _source.resources.items():
        resource.apply_hints(table_name=f"xxxx__{resource.name}")
    pipeline_1 = dlt.pipeline(
        pipeline_name="sql_db_prefixed_tables",
        destination="duckdb",
        dataset_name="renamed_tables",
    )
    print(pipeline_1.run(_source))
    pipeline_1.dataset().row_counts().df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ✅ ▶ Proceed to the [next lesson](https://colab.research.google.com/drive/1P8pOw9C6J9555o2jhZydESVuVb-3z__y#forceEdit=true&sandboxMode=true)!
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
