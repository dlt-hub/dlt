# /// script
# dependencies = [
#     "dlt[duckdb]",
#     "numpy",
#     "pandas",
#     "sqlalchemy",
# ]
# ///

import marimo

__generated_with = "0.17.4"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Quick Start** 👩‍💻🚀 [![Open in molab](https://marimo.io/molab-shield.svg)](https://molab.marimo.io/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_1_quick_start.py) [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_1_quick_start.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_1_quick_start.ipynb)

    **Here, you will learn:**
    - What is dlt?
    - How to run a simple pipeline with toy data.
    - How to explore the loaded data using:
      - DuckDB connection
      - dlt's sql_client
      - dlt datasets
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **What is dlt?**

    In today's data-driven world, organizations often grapple with the challenge of efficiently **extracting, transforming,** and **loading** (ETL) data from various, often messy, data sources into well-structured, live datasets. This process can be complex, time-consuming, and prone to errors, especially when dealing with large volumes of data or nested data structures.

    Enter **dlt**, an **open-source Python library** designed to simplify and streamline this process. **dlt can load data from** a wide range of **sources** including REST APIs, SQL databases, cloud storage, and Python data structures, among others. It offers a lightweight interface that **infers schemas** and **data types**, **normalizes** the data, and handles **nested data** structures, making it easy to use, flexible, and scalable.

    Moreover, dlt supports a variety of **popular destinations** and allows for the addition of custom destinations to create **reverse ETL** pipelines. It can be deployed **anywhere Python runs**, be it on Airflow, serverless functions, or any other cloud deployment of your choice. With features like **schema evolution**, **data contracts** and **incremental loading**, dlt also automates pipeline maintenance, saving valuable time and resources.

    In essence, dlt is a powerful tool that simplifies the ETL process, making it more efficient and less error-prone. It allows data teams to **focus** on leveraging the data and driving value, while ensuring effective **governance** through timely notifications of any changes.

    [Learn more about dlt here](https://dlthub.com/docs/intro) and in this course!
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_1_Quick_start_img1](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_1_Quick_start_img1.png)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ## **Installation**

    > **Note**: We recommend working within a virtual environment when creating Python projects. This way, all the dependencies for your current project will be isolated from packages in other projects.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Read more about DuckDB as a destination [here](https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb)."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ##  **Run a simple pipeline with toy data**
    For educational purposes, let’s start with a simple pipeline using a small dataset — Pokémon data represented as a list of Python dictionaries.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""1. Define a list of Python dictionaries, which will be your toy data:"""
    )
    return


@app.cell
def _():
    # Sample data containing pokemon details
    data = [
        {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
        {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
        {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
    ]
    return (data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""2. Import `dlt` and create a simple pipeline:""")
    return


@app.cell
def _():
    import dlt

    # Set pipeline name, destination, and dataset name
    pipeline = dlt.pipeline(
        pipeline_name="quick_start",
        destination="duckdb",
        dataset_name="mydata",
    )
    return dlt, pipeline


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""3. Run your pipeline and print the load info:""")
    return


@app.cell
def _(data, pipeline):
    # Run the pipeline with data and table name
    _load_info = pipeline.run(data, table_name='pokemon')
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    > **What just happened?**
    > The first run of a pipeline will scan the data that goes through it and generate a schema. To convert nested data into a relational format, dlt flattens dictionaries and unpacks nested lists into sub-tables.
    >
    > For this example, `dlt` created a schema called 'mydata' with the table 'pokemon' in it and stored it in DuckDB.
    >
    >For detailed instructions on running a pipeline, see the documentation [here](https://dlthub.com/docs/walkthroughs/run-a-pipeline).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Quick start was really quick, huh? It seems like some kind of magic happened.

    We don't believe in magic! Let's start from the beginning, what is a `dlt` Pipeline?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---

    ## **What is a `dlt` Pipeline?**

    A [pipeline](https://dlthub.com/docs/general-usage/pipeline) is a connection that moves data from your Python code to a destination. The pipeline accepts dlt sources or resources, as well as generators, async generators, lists, and any iterables. Once the pipeline runs, all resources are evaluated and the data is loaded at the destination.
    """)
    return


@app.cell
def _(dlt):
    another_pipeline = dlt.pipeline(
        pipeline_name="resource_source",
        destination="duckdb",
        dataset_name="mydata",
        dev_mode=True,
    )
    return (another_pipeline,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    You instantiate a pipeline by calling the `dlt.pipeline` function with the following arguments:
    * **`pipeline_name`**: This is the name you give to your pipeline. It helps you track and monitor your pipeline, and also helps to bring back its state and data structures for future runs. If you don't provide a name, dlt will use the name of the Python file you're running as the pipeline name.
    * **`destination`**: a name of the destination to which dlt will load the data. It may also be provided to the run method of the pipeline.
    * **`dataset_name`**: This is the name of the group of tables (or dataset) where your data will be sent. You can think of a dataset like a folder that holds many files, or a schema in a relational database. You can also specify this later when you run or load the pipeline. If you don't provide a name, it will default to the name of your pipeline.
    * **`dev_mode`**: If you set this to True, dlt will add a timestamp to your dataset name every time you create a pipeline. This means a new dataset will be created each time you create a pipeline.

    There are additional arguments for advanced use, but we’ll skip them for now.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---

    ## **Run method**

    To load the data, you call the `run()` method and pass your data in the data argument.
    """)
    return


@app.cell
def _(another_pipeline, data):
    # Run the pipeline and print load info
    _load_info = another_pipeline.run(data, table_name='pokemon')
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Commonly used arguments:

    * **`data`** (the first argument) may be a dlt source, resource, generator function, or any Iterator or Iterable (i.e., a list or the result of the map function).
    * **`write_disposition`** controls how to write data to a table. Defaults to the value "append".
      * `append` will always add new data at the end of the table.
      * `replace` will replace existing data with new data.
      * `skip` will prevent data from loading.
      * `merge` will deduplicate and merge data based on `primary_key` and `merge_key` hints.
    * **`table_name`**: specified in cases when the table name cannot be inferred, i.e., from the resources or name of the generator function.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ## **Explore the loaded data**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ### **(1) DuckDB Connection**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Start a connection to your database using a native `duckdb` connection and see which tables were generated:"""
    )
    return


@app.cell
def _(another_pipeline):
    import duckdb

    # A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

    # Connect to the DuckDB database
    conn = duckdb.connect(f"{another_pipeline.pipeline_name}.duckdb")

    # Set search path to the dataset
    conn.sql(f"SET search_path = '{another_pipeline.dataset_name}'")

    # Describe the dataset
    conn.sql("DESCRIBE").df()
    return (conn,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    You can see:
    -  `pokemon` table,

    and 3 special `dlt` tables (we will discuss them later):
    - `_dlt_loads`,
    - `_dlt_pipeline_state`,
    - `_dlt_version`.

    Let's execute a query to get all data from the `pokemon` table:
    """)
    return


@app.cell
def _(conn):
    # Fetch all data from 'pokemon' as a DataFrame
    table = conn.sql("SELECT * FROM pokemon").df()

    # Display the DataFrame
    table
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ### **(2) `dlt`'s [sql_client](https://dlthub.com/docs/general-usage/dataset-access/sql-client)**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Most dlt destinations (even filesystem) use an implementation of the `SqlClientBase` class to connect to the physical destination to which your data is loaded. You can access the SQL client of your destination via the `sql_client` method on your pipeline.

    Start a connection to your database with `pipeline.sql_client()` and execute a query to get all data from the `pokemon` table:
    """)
    return


@app.cell
def _(another_pipeline):
    # Query data from 'pokemon' using the SQL client
    with another_pipeline.sql_client() as client:
        with client.execute_query('SELECT * FROM pokemon') as cursor:
            data_1 = cursor.df()
    # Display the data
    data_1
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ### **(3) dlt [datasets](https://dlthub.com/docs/general-usage/dataset-access/dataset)**

    Here's an example of how to retrieve data from a pipeline and load it into a Pandas DataFrame or a PyArrow Table.
    """)
    return


@app.cell
def _(another_pipeline):
    dataset = another_pipeline.dataset()
    dataset.pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    # **Exercise 1**

    Using the code from the previous cell, fetch the data from the `pokemon` table into a dataframe and count the number of columns in the table `pokemon`.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""**Use this number to answer the question in the Quiz LearnWorlds Form.**"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_1_Quick_start_img2.jpeg](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_1_Quick_start_img2.jpeg)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""✅ ▶ Proceed to the [next lesson](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_2_dlt_sources_and_resources_create_first_dlt_pipeline.ipynb)!"""
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()

