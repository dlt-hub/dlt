import marimo

__generated_with = "0.14.10"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # **Recap of [Lesson 3](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_3_pagination_and_authentication_and_dlt_configuration.ipynb) 👩‍💻🚀**

        1. Used pagination with REST APIs.
        2. Applied authentication for REST APIs.
        3. Tried the dlt `RESTClient`.
        4. Used environment variables to manage secrets and configuration.
        5. Learned how to add values to `secrets.toml` and `config.toml`.
        6. Used the special `secrets.toml` environment variable setup for Colab.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        # **`dlt`’s pre-built Sources and Destinations** [![Open with marimo](https://marimo.io/shield.svg)](https://marimo.app/github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_4_using_pre_build_sources_and_destinations.py) [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_4_using_pre_build_sources_and_destinations.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_4_using_pre_build_sources_and_destinations.ipynb)


        **Here, you will learn:**
        - How to initialize verified sources.
        - The built-in `rest_api` source.
        - The built-in `sql_database` source.
        - The built-in `filesystem` source.
        - How to switch between destinations.

        ---

        Our verified sources are the simplest way to start building your stack. Choose from any of our fully customizable 30+ pre-built sources, such as SQL databases, Google Sheets, Salesforce, and more.

        With our numerous destinations, you can load data into a local database, data warehouse, or data lake. Choose from Snowflake, Databricks, and many others.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ![Lesson_4_Using_pre_build_sources_and_destinations_img1](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations_img1.png)
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # **Existing verified sources**
        To use an [existing verified source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/), just run the `dlt init` command.



        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""

        There's a base project for each `dlt` verified source + destination combination, which you can adjust according to your needs.

        These base project can be initialized with a simple command:

        ```
        dlt init <verified-source> <destination>
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Step 0: Install `dlt`
        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install dlt[duckdb]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        List all verified sources:

        """
    )
    return


app._unparsable_cell(
    r"""
    !dlt init --list-sources
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        This command shows all available verified sources and their short descriptions. For each source, it checks if your local `dlt` version requires an update and prints the relevant warning.

        Consider an example pipeline for the GitHub API:

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

        This command will initialize the pipeline example with the GitHub API as the source and DuckBD as the destination:
        """
    )
    return


app._unparsable_cell(
    r"""
    !dlt --non-interactive init github_api duckdb
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Now, check  your files on the left side bar. It should contain all the necessary files to run your GitHub API -> DuckDB pipeline:

        - The `.dlt` folder containing `secrets.toml` and `config.toml`
        - The pipeline script `github_api_pipeline.py`
        - `requirements.txt`
        - `.gitignore`
        """
    )
    return


app._unparsable_cell(
    r"""
    !ls -a
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        What you would normally do with the project:
        - Add your credentials and define configurations
        - Adjust the pipeline script as needed
        - Run the pipeline script

        > If needed, you can adjust the verified source code.
        """
    )
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
        r"""
        From the code, we can see that this pipeline loads **only the `"issues"` endpoint**.
        You can adjust this code as needed: add new endpoints, include additional logic, apply transformations, and more.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Step 2. Add credentials

        In Colab, it is more convenient to use environment variables. In the previous lesson, you learned how to configure a `dlt` resource using an environment variable.

        In the pipeline above, the `access_token` parameter is set to `dlt.secrets.value`, which means you need to configure this variable:


        ```python
        @dlt.resource(write_disposition="replace")
        def github_api_resource(access_token: Optional[str] = dlt.secrets.value):
          ...
        ```
        """
    )
    return


@app.cell
def _():
    import os
    from google.colab import userdata

    os.environ["SOURCES__ACCESS_TOKEN"] = userdata.get("SECRET_KEY")
    return (os,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Step 3. Run the pipeline
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Let's run the pipeline!
        """
    )
    return


app._unparsable_cell(
    r"""
    !python github_api_pipeline.py
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        From the pipeline output, we can get information such as the pipeline name, dataset name, destination path, and more.

        > Pipeline **github_api_pipeline** load step completed in 1.23 seconds
        > 1 load package was loaded to the DuckDB destination and into the dataset **github_api_data**.
        > The DuckDB destination used `duckdb:////content/**github_api_pipeline.duckdb**` as the storage location.
        > Load package `1733848559.8195539` is **LOADED** and contains no failed jobs.


        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Step 4: Explore your data

        Let's explore what tables were created in the destination.
        """
    )
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
    mo.md(
        r"""
        # **Built-in sources: RestAPI, SQL database & Filesystem**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## **[RestAPI source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic)**

        `rest_api` is a generic source that lets you create a `dlt` source from any REST API using a declarative configuration. Since most REST APIs follow similar patterns, this source provides a convenient way to define your integration declaratively.

        Using a [declarative configuration](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic#source-configuration), you can specify:

        - the API endpoints to pull data from,
        - their relationships,
        - how to handle pagination,
        - authentication.

        `dlt` handles the rest for you: **unnesting the data, inferring the schema**, and **writing it to the destination**.

        In the previous lesson, you already used the REST API Client. `dlt`’s **[RESTClient](https://dlthub.com/docs/general-usage/http/rest-client)** is the **low-level abstraction** that powers the RestAPI source.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Initialize the `rest_api` template

        You can initialize the `rest_api` **template** using the `init` command:

        """
    )
    return


app._unparsable_cell(
    r"""
    !yes | dlt init rest_api duckdb
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        In the `rest_api_pipeline.py` script, you will find sources for both the GitHub API and the PokeAPI, defined using the `rest_api` source and `RESTAPIConfig`.

        Since the `rest_api` source is a **built-in source**, you don't need to initialize it. You can simply **import** it from `dlt.sources` and start using it.

        ### Example

        Here is a simplified example of how to configure the REST API source to load `issues` and issue `comments` from the GitHub API:

        """
    )
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

    rest_api_pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
        dev_mode=True,
    )

    load_info = rest_api_pipeline.run(github_source)
    print(load_info)
    return (dlt,)


@app.cell
def _(pipeline):
    pipeline.dataset().issues.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **Exercise 1: Run `rest_api` source**

        Explore the cells above and answer the question below using `sql_client` or `pipeline.dataset()`.

        #### **Question**
        How many columns does the `issues` table have?
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **Exercise 2: Create a dlt source with `rest_api`**

        Add the `contributors` endpoint for the `dlt` repository to the `rest_api` configuration:

        - Resource name: **"contributors"**
        - Endpoint path: **"repos/dlt-hub/dlt/contributors"**
        - No parameters

        #### **Question**
        How many columns does the `contributors` table have?

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ## **[SQL Databases source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/)**

        SQL databases are management systems (DBMS) that store data in a structured format, commonly used for efficient and reliable data retrieval.

        The `sql_database` verified source loads data to your specified destination using one of the following backends:
        * SQLAlchemy,
        * PyArrow,
        * pandas,
        * ConnectorX.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Initialize the `sql_database` template

        Initialize the `dlt` template for `sql_database` using the `init` command:

        """
    )
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
        r"""
        The `sql_database` source is also a **built-in source**, you don't have to initialize it, just **import** it from `dlt.sources`.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Example

        The example below shows how you can use dlt to load data from a SQL database (PostgreSQL, MySQL, SQLite, Oracle, IBM DB2, etc.) into a destination.

        To make it easy to reproduce, we will load data from the [public MySQL Rfam database](https://docs.rfam.org/en/latest/database.html) into a local DuckDB instance.
        """
    )
    return


app._unparsable_cell(
    r"""
    !pip install pymysql
    """,
    name="_",
)


@app.cell
def _(dlt):
    from dlt.sources.sql_database import sql_database

    sql_source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["family"],
    )
    sql_db_pipeline = dlt.pipeline(
        pipeline_name="sql_database_example",
        destination="duckdb",
        dataset_name="sql_data",
        dev_mode=True,
    )
    load_info_1 = sql_db_pipeline.run(sql_source)
    print(load_info_1)
    return (sql_database,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **Exercise 3: Run `sql_database` source**

        Explore the cells above and answer the question below using `sql_client` or `pipeline.dataset()`.

        #### **Question**
        How many columns does the `family` table have?
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ## **[Filesystem source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem/)**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
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
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Initialize the `filesystem` template

        Initialize the dlt template for `filesystem` using the `init` command:

        """
    )
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
        r"""
        The `filesystem` source is also a **built-in source**, you don't have to initialize it, just **import** it from `dlt.sources`.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Example

        To illustrate how this **built-in source** works, we first download some file to the local (Colab) filesystem.
        """
    )
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

    filesystem_resource = filesystem(
        bucket_url="/content/local_data", file_glob="**/*.parquet"
    )
    filesystem_pipe = filesystem_resource | read_parquet()
    fs_pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
    load_info_2 = fs_pipeline.run(filesystem_pipe.with_name("userdata"))
    print(load_info_2)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **Exercise 4: Run `filesystem` source**

        Explore the cells above and answer the question below using `sql_client` or `pipeline.dataset()`.

        #### **Question**
        How many columns does the `userdata` table have?

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        You can read how to configure **Cloud Storage** in the official
        [dlt documentation](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem/basic#configuration).

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # [**Built-in Destinations**](https://dlthub.com/docs/dlt-ecosystem/destinations/)

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ![Lesson_4_Using_pre_build_sources_and_destinations_img2](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations_img2.png)
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ##  **Exploring `dlt` destinations**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        To be honest, this is simply a matter of going through the
        [documentation](https://dlthub.com/docs/dlt-ecosystem/destinations/) 👀, but to sum it up:

        - Most likely, the destination where you want to load data is already a `dlt` integration that undergoes several hundred automated tests every day.
        - If not, you can define a custom destination and still benefit from most `dlt`-specific features.
          *FYI: custom destinations will be covered in the next Advanced course — so we expect you to come back for part two…*

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## **Choosing a destination**

        Switching between destinations in `dlt` is incredibly straightforward. Simply modify the `destination` parameter in your pipeline configuration. For example:
        """
    )
    return


@app.cell
def _(dlt):
    data_pipeline = dlt.pipeline(
        pipeline_name="data_pipeline",
        destination="duckdb",  # <--- to test pipeline locally
        dataset_name="data",
    )
    print(data_pipeline.destination.destination_type)

    data_pipeline = dlt.pipeline(
        pipeline_name="data_pipeline",
        destination="bigquery",  # <--- to run pipeline in production
        dataset_name="data",
    )
    print(data_pipeline.destination.destination_type)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        This flexibility allows you to easily transition from local development to production-grade environments.


        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## **Filesystem destination**

        The `filesystem` destination enables you to load data into **files stored locally** or in **cloud storage** solutions, making it an excellent choice for lightweight testing, prototyping, or file-based workflows.

        Below is an **example** demonstrating how to use the `filesystem` destination to load data in **Parquet** format:

        * Step 1: Set up a local bucket or cloud directory for storing files

        """
    )
    return


@app.cell
def _(os):
    os.environ["BUCKET_URL"] = "/content"
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        * Step 2: Define the data source
        """
    )
    return


@app.cell
def _(dlt, sql_database):
    source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["family"],
    )
    pipeline = dlt.pipeline(
        pipeline_name="fs_pipeline", destination="filesystem", dataset_name="fs_data"
    )
    load_info_3 = pipeline.run(source, loader_file_format="parquet")
    print(load_info_3)
    return pipeline, source


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Look at the files:
        """
    )
    return


app._unparsable_cell(
    r"""
    ! ls fs_data/family
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Look at the loaded data:
        """
    )
    return


@app.cell
def _(pipeline):
    # explore loaded data
    pipeline.dataset().family.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **Table formats: [Delta tables & Iceberg](https://dlthub.com/docs/dlt-ecosystem/destinations/delta-iceberg)**

        dlt supports writing **Delta** and **Iceberg** tables when using the `filesystem` destination.

        **How it works:**

        dlt uses the `deltalake` and `pyiceberg` libraries to write Delta and Iceberg tables, respectively. One or multiple Parquet files are prepared during the extract and normalize steps. In the load step, these Parquet files are exposed as an Arrow data structure and fed into `deltalake` or `pyiceberg`.
        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install "dlt[pyiceberg]"
    return


@app.cell
def _(pipeline, source):
    load_info_4 = pipeline.run(
        source, loader_file_format="parquet", table_format="iceberg"
    )
    print(load_info_4)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **Note:**

        The open-source version of dlt supports basic functionality for **Iceberg**, but the dltHub team is currently working on an **extended** and **more powerful** Iceberg integration.

        [Join the waiting list to learn more about dlt+ and Iceberg.](https://info.dlthub.com/waiting-list)

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # **Spoiler: Custom Sources & Destinations**

        `dlt` aims to simplify the process of creating both custom sources
        ([REST API Client](https://dlthub.com/docs/general-usage/http/rest-client),
        [`rest_api` source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api))
        and [custom destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/destination).

        We will explore this topic in more detail in the next Advanced course.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ✅ ▶ Proceed to the [next lesson](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_5_write_disposition_and_incremental_loading.ipynb)!
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
