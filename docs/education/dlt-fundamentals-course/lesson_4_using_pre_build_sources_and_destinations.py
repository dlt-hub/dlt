import marimo

__generated_with = "0.16.4"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Recap of [Lesson 3](https://colab.research.google.com/drive/1-jVNzMJTRYHhbRlXgGFlhMwdML1L9zMx#forceEdit=true&sandboxMode=true) 👩‍💻🚀**

    1. Used pagination for RestAPIs.
    2. Used authentication for RestAPIs.
    3. Tried dlt RESTClient.
    4. Used environment variables to handle both secrets & configs.
    5. Learned how to add values to `secrets.toml` or `config.toml`.
    6. Used `secrets.toml` ENV variable special for Colab.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    # **`dlt`’s pre-built Sources and Destinations** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_4_using_pre_build_sources_and_destinations.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_4_using_pre_build_sources_and_destinations.ipynb)



    **Here, you will learn:**
    - How to initialize verified sources;
    - Built-in `rest_api` source.
    - Built-in `sql_database` source.
    - Built-in `filesystem` source.
    - How to switch between destinations.

    ---

    Our verified sources are the simplest way to get started with building your stack. Choose from any of our fully customizable 30+ pre-built sources, such as any SQL database, Google Sheets, Salesforce and others.

    With our numerous destinations you can load data to a local database, warehouse or a data lake. Choose from Snowflake, Databricks and more.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_4_Using_pre_build_sources_and_destinations_img1](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations_img1.png)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Existing verified sources**
    To use an [existing verified source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/), just run the `dlt init` command.



    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    There's a base project for each `dlt` verified source + destination combination, which you can adjust according to your needs.

    These base project can be initialized with a simple command:

    ```
    dlt init <verified-source> <destination>
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Step 0: Install dlt""")
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # # (use marimo's built-in package management features instead) !pip install dlt[duckdb]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    List all verified sources:

    """)
    return


app._unparsable_cell(
    r"""
    !dlt init --list-sources
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This command shows all available verified sources and their short descriptions. For each source, it checks if your local `dlt` version requires an update and prints the relevant warning.

    Consider an example of a pipeline for the GitHub API:

    ```
    Available dlt single file templates:
    ---
    arrow: The Arrow Pipeline Template will show how to load and transform arrow tables.
    dataframe: The DataFrame Pipeline Template will show how to load and transform pandas dataframes.
    debug: The Debug Pipeline Template will load a column with each datatype to your destination.
    default: The Intro Pipeline Template contains the example from the docs intro page
    fruitshop: The Default Pipeline Template provides a simple starting point for your dlt pipeline

    ---> github_api: The Github API templates provides a starting

    point to read data from REST APIs with REST Client helper
    requests: The Requests Pipeline Template provides a simple starting point for a dlt pipeline with the requests library
    ```

    ### Step 1. Initialize the source

    This command will initialize the pipeline example with GitHub API as the source and DuckBD as the destination:
    """)
    return


app._unparsable_cell(
    r"""
    !dlt --non-interactive init github_api duckdb
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Now, check  your files on the left side bar. It should contain all the necessary files to run your GitHub API -> DuckDB pipeline:
    * `.dlt` folder for `secrets.toml` and `config.toml`;
    * pipeline script `github_api_pipeline.py`;
    * requirements.txt;
    * `.gitignore`.
    """)
    return


app._unparsable_cell(
    r"""
    !ls -a
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    What you would normally do with the project:
    - Add your credentials and define configurations
    - Adjust the pipeline script as needed
    - Run the pipeline script

    > In certain cases, you can adjust the verified source code.
    """)
    return


app._unparsable_cell(
    r"""
    !cat github_api_pipeline.py
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""From the code we can see that this pipeline loads **only "issues" endpoint**, you can adjust this code as you wish: add new endpoints, add additional logic, add transformations, etc."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Step 2. Add credentials

    In Colab is more convenient to use ENVs. In the previous lesson you learned how to configure dlt resource via environment variable.

    In the pipeline above we can see that `access_token` variable is `dlt.secrets.value`, it means we should configure this variable.

    ```python
    @dlt.resource(write_disposition="replace")
    def github_api_resource(access_token: Optional[str] = dlt.secrets.value):
      ...
    ```
    """)
    return


@app.cell
def _():
    import os
    from google.colab import userdata

    os.environ["SOURCES__ACCESS_TOKEN"] = userdata.get("SECRET_KEY")
    return (os,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Step 3. Run the pipeline""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's run the pipeline!""")
    return


app._unparsable_cell(
    r"""
    !python github_api_pipeline.py
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    From the pipeline output we can take pipeline information like pipeline_name, dataset_name, destination path, etc.


    > Pipeline **github_api_pipeline** load step completed in 1.23 seconds
    1 load package(s) were loaded to destination duckdb and into dataset **github_api_data**
    The duckdb destination used duckdb:////content/**github_api_pipeline.duckdb** location to store data
    Load package 1733848559.8195539 is LOADED and contains no failed jobs

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 4: Explore your data

    Let's explore what tables were created in duckdb.
    """)
    return


@app.cell
def _():
    import duckdb

    conn = duckdb.connect("github_api_pipeline.duckdb")
    conn.sql("SET search_path = 'github_api_data'")
    conn.sql("DESCRIBE").df()
    return (conn,)


@app.cell
def _(conn):
    data_table = conn.sql("SELECT * FROM github_api_resource").df()
    data_table
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# **Built-in sources: RestAPI, SQL database & Filesystem**""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **[RestAPI source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic)**

    `rest_api` is a generic source that you can use to create a `dlt` source from a REST API using a declarative configuration. The majority of REST APIs behave in a similar way; this `dlt` source attempts to provide a declarative way to define a `dlt` source for those APIs.

    Using a [declarative configuration](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic#source-configuration), you can define:

    - the API endpoints to pull data from,
    - their relationships,
    - how to handle pagination,
    - authentication.

    dlt will take care of the rest: **unnesting the data, inferring the schema**, etc., and **writing to the destination**

    In previous lesson you've already met Rest API Client. `dlt`’s **[RESTClient](https://dlthub.com/docs/general-usage/http/rest-client)** is the **low level abstraction** that powers the REST API Source.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Initialize `rest_api` template
    You can initialize `rest_api` **template** using `init` command:
    """)
    return


app._unparsable_cell(
    r"""
    !yes | dlt init rest_api duckdb
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    In the `rest_api_pipeline.py` script you will find sources for GitHub API and for PokeAPI, which were defined using `rest_api` source and `RESTAPIConfig`.

    Since the `rest_api` source is a **built-in source**, you don't have to initialize it. You can **import** it from `dlt.sources` and use it immediately.


    ### Example

    Here's a simplified example of how to configure the REST API source to load `issues` and issue `comments` from GitHub API:


    """)
    return


@app.cell
def _():
    import dlt
    from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
    from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com",
            "auth": {
                "token": dlt.secrets[
                    "sources.access_token"
                ],  # <--- we already configured access_token above
            },
            "paginator": "header_link",  # <---- set up paginator type
        },
        "resources": [  # <--- list resources
            {
                "name": "issues",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/issues",
                    "params": {
                        "state": "open",
                    },
                },
            },
            {
                "name": "issue_comments",  # <-- here we declare dlt.transformer
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/issues/{issue_number}/comments",
                    "params": {
                        "issue_number": {
                            "type": (
                                "resolve"
                            ),  # <--- use type 'resolve' to resolve {issue_number} for transformer
                            "resource": "issues",
                            "field": "number",
                        },
                    },
                },
            },
            {
                "name": "contributors",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/contributors",
                },
            },
        ],
    }

    github_source = rest_api_source(config)

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
        dev_mode=True,
    )

    load_info = pipeline.run(github_source)
    print(load_info)
    return dlt, pipeline


@app.cell
def _(pipeline):
    pipeline.dataset().issues.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Exercise 1: Run rest_api source**

    Explore the cells above and answer the question below using `sql_client` or `pipeline.dataset()`.

    #### Question
    How many columns has the `issues` table?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ###  **Exercise 2: Create dlt source with rest_api**

    Add `contributors` endpoint for dlt repository to the `rest_api` configuration:
    - resource name is "contributors"
    - endpoint path : "repos/dlt-hub/dlt/contributors"
    - no parameters

    #### Question
    How many columns has the `contributors` table?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ## **[SQL Databases source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/)**

    SQL databases are management systems (DBMS) that store data in a structured format, commonly used for efficient and reliable data retrieval.

    The `sql_database` verified source loads data to your specified destination using one of the following backends:
    * SQLAlchemy,
    * PyArrow,
    * pandas,
    * ConnectorX.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Initialize `sql_database` template

    Initialize dlt template for `sql_database` using `init` command:
    """)
    return


app._unparsable_cell(
    r"""
    !yes | dlt init sql_database duckdb
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""The `sql_database` source is also a **built-in source**, you don't have to initialize it, just **import** it from `dlt.sources`."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Example

    The example below will show you how you can use dlt to load data from a SQL Database (PostgreSQL, MySQL, SQLight, Oracle, IBM DB2, etc.) into destination.

    To make it easy to reproduce, we will be loading data from the [public MySQL RFam database](https://docs.rfam.org/en/latest/database.html) into a local DuckDB instance.
    """)
    return


@app.cell
def _():
    # (use marimo's built-in package management features instead) !pip install pymysql
    return


@app.cell
def _(dlt):
    from dlt.sources.sql_database import sql_database

    source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam", table_names=["family"]
    )
    pipeline_1 = dlt.pipeline(
        pipeline_name="sql_database_example",
        destination="duckdb",
        dataset_name="sql_data",
        dev_mode=True,
    )
    load_info_1 = pipeline_1.run(source)
    print(load_info_1)
    return (sql_database,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Exercise 3: Run sql_database source**

    Explore the cells above and answer the question below using `sql_client` or `pipeline.dataset()`.

    #### Question
    How many columns does the `family` table have?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ## **[Filesystem source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem/)**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The filesystem source allows seamless loading of files from the following locations:

    * AWS S3
    * Google Cloud Storage
    * Google Drive
    * Azure Blob Storage
    * remote filesystem (via SFTP)
    * local filesystem

    The filesystem source natively supports CSV, Parquet, and JSONL files and allows customization for loading any type of structured file.


    **How filesystem source works**

    The Filesystem source doesn't just give you an easy way to load data from both remote and local files — it also comes with a powerful set of tools that let you customize the loading process to fit your specific needs.

    Filesystem source loads data in two steps:

    1. It accesses the files in your remote or local file storage **without** actually **reading** the content yet. At this point, you can filter files by metadata or name. You can also set up incremental loading to load only new files.
    2. The **transformer** **reads** the files' content and yields the records. At this step, you can filter out the actual data, enrich records with metadata from files, or perform incremental loading based on the file content.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Initialize `filesystem` template

    Initialize dlt template for `filesystem` using `init` command:
    """)
    return


app._unparsable_cell(
    r"""
    !yes | dlt init filesystem duckdb
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""The `filesystem` source is also a **built-in source**, you don't have to initialize it, just **import** it from `dlt.sources`."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Example

    To illustrate how this **built-in source** works, we first download some file to the local (Colab) filesystem.
    """)
    return


app._unparsable_cell(
    r"""
    !mkdir -p local_data && wget -O local_data/userdata.parquet https://www.timestored.com/data/sample/userdata.parquet
    """,
    name="_",
)


@app.cell
def _(dlt):
    from dlt.sources.filesystem import filesystem, read_parquet

    filesystem_resource = filesystem(bucket_url="/content/local_data", file_glob="**/*.parquet")
    filesystem_pipe = filesystem_resource | read_parquet()
    pipeline_2 = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
    load_info_2 = pipeline_2.run(filesystem_pipe.with_name("userdata"))
    # We load the data into the table_name table
    print(load_info_2)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Exercise 4: Run filesystem source**

    Explore the cells above and answer the question below using `sql_client` or `pipeline.dataset()`.

    #### Question
    How many columns does the `userdata` table have?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""How to configure **Cloud Storage** you can read in the official [dlt documentation](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem/basic#configuration)."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Built-in Destinations**

    https://dlthub.com/docs/dlt-ecosystem/destinations/
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_4_Using_pre_build_sources_and_destinations_img2](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations_img2.png)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ##  **Exploring `dlt` destinations**

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    TBH this is a matter of simply going through the [documentation](https://dlthub.com/docs/dlt-ecosystem/destinations/) 👀, but to sum it up:
    - Most likely the destination where you want to load data is already a `dlt` integration that undergoes several hundred automated tests every day.
    - If not, you can simply define a custom destination and still be able to benefit from most `dlt`-specific features. FYI, custom destinations will be covered in the next Advanced course, so we expect you to come back for the second part...
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Choosing a destination**

    Switching between destinations in dlt is incredibly straightforward—simply modify the `destination` parameter in your pipeline configuration. For example:
    """)
    return


@app.cell
def _(dlt):
    pipeline_3 = dlt.pipeline(
        pipeline_name="data_pipeline", destination="duckdb", dataset_name="data"
    )
    pipeline_3 = dlt.pipeline(
        pipeline_name="data_pipeline", destination="bigquery", dataset_name="data"
    )  # <--- to test pipeline locally  # <--- to run pipeline in production
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This flexibility allows you to easily transition from local development to production-grade environments.


    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Filesystem destination**

    The `filesystem` destination enables you to load data into **files stored locally** or in **cloud storage** solutions, making it an excellent choice for lightweight testing, prototyping, or file-based workflows.

    Below is an **example** demonstrating how to use the `filesystem` destination to load data in **Parquet** format:

    * Step 1: Set up a local bucket or cloud directory for storing files

    """)
    return


@app.cell
def _(os):
    os.environ["BUCKET_URL"] = "/content"
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""* Step 2: Define the data source""")
    return


@app.cell
def _(dlt, sql_database):
    source_1 = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam", table_names=["family"]
    )
    pipeline_4 = dlt.pipeline(
        pipeline_name="fs_pipeline", destination="filesystem", dataset_name="fs_data"
    )
    load_info_3 = pipeline_4.run(source_1, loader_file_format="parquet")
    print(
        load_info_3
    )  # <--- change destination to 'filesystem'  # <--- choose a file format: parquet, csv or jsonl
    return pipeline_4, source_1


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Look at the files:""")
    return


app._unparsable_cell(
    r"""
    ! ls fs_data/family
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Look at the loaded data:""")
    return


@app.cell
def _(pipeline_4):
    # explore loaded data
    pipeline_4.dataset().family.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Table formats: [Delta tables & Iceberg](https://dlthub.com/docs/dlt-ecosystem/destinations/delta-iceberg)**

    dlt supports writing **Delta** and **Iceberg** tables when using the `filesystem` destination.

    **How it works:**

    dlt uses the `deltalake` and `pyiceberg` libraries to write Delta and Iceberg tables, respectively. One or multiple Parquet files are prepared during the extract and normalize steps. In the load step, these Parquet files are exposed as an Arrow data structure and fed into `deltalake` or `pyiceberg`.
    """)
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # # (use marimo's built-in package management features instead) !pip install "dlt[pyiceberg]"
    return


@app.cell
def _(pipeline_4, source_1):
    load_info_4 = pipeline_4.run(source_1, loader_file_format="parquet", table_format="iceberg")
    print(load_info_4)  # <--- choose a table format: delta or iceberg
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Note:**

    Open source version of dlt supports basic functionality for **iceberg**, but the dltHub team is currently working on an **extended** and **more powerful** integration with iceberg.

    [Join the waiting list to learn more about dlt+ and Iceberg.](https://info.dlthub.com/waiting-list)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # **Spoiler: Custom Sources & Destinations**

    `dlt` tried to simplify as much as possible both the process of creating sources ([RestAPI Client](https://dlthub.com/docs/general-usage/http/rest-client), [rest_api source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)) and [custom destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/destination).

    We will look at this topic in more detail in the next Advanced course.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""✅ ▶ Proceed to the [next lesson](https://colab.research.google.com/drive/1Zf24gIVMNNj9j-gtXFl8p0orI9ttySDn#forceEdit=true&sandboxMode=true)!"""
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
