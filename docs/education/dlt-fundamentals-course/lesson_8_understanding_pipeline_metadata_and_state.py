import marimo

__generated_with = "0.14.10"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # **Recap of [Lesson 7](https://colab.research.google.com/drive/1LokUcM5YSazdq5jfbkop-Z5rmP-39y4r#forceEdit=true&sandboxMode=true) 👩‍💻🚀**

        1. Learned what a schema is.
        2. Explored schema settings and components.
        3. Learned how to retrieve a dlt pipeline schema.
        4. Learned how to adjust the schema.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---

        # **Understanding Pipeline Metadata and State** 👻📄 [![Open with marimo](https://marimo.io/shield.svg)](https://marimo.app/github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_8_understanding_pipeline_metadata_and_state.ipynb) [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_8_understanding_pipeline_metadata_and_state.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_8_understanding_pipeline_metadata_and_state.ipynb)


        **Here, you will learn or brush up on:**
        - What pipeline metadata is
        - Exploring pipeline metadata from load info
        - Exploring pipeline metadata from trace
        - Exploring pipeline metadata from state
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ##  **Pipeline Metadata**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **Metadata** is essentially *data about data*.

        **Pipeline metadata** is data about your data pipeline. This is useful when you want to know things like:

        - When your pipeline first ran
        - When your pipeline last ran
        - Information about your source or destination
        - Processing time
        - Custom metadata you add yourself
        - And much more!
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ![Lesson_8_Understanding_Pipeline_Metadata_and_State_img1](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_8_Understanding_Pipeline_Metadata_and_State_img1.jpeg)
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        `dlt` allows you to view all this metadata through various options!

        This notebook will walk you through those options, namely:

        - Load info
        - Trace
        - State
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Let's load some GitHub data into DuckDB to inspect the pipeline metadata in different ways.
        First, we need to install `dlt` with DuckDB:

        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install "dlt[duckdb]"
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Define a `dlt` resource that fetches Pull Requests and wrap it in a `dlt` source:
        """
    )
    return


@app.cell
def _():
    from typing import Iterable
    import dlt
    from dlt.extract import DltResource
    from dlt.common.typing import TDataItems
    from dlt.sources.helpers import requests
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

    import os
    from google.colab import userdata

    os.environ["SOURCES__SECRET_KEY"] = userdata.get("SECRET_KEY")

    @dlt.source
    def github_source(secret_key: str = dlt.secrets.value) -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=secret_key),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource
        def github_pulls(
            cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "updated_at", initial_value="2024-12-01"
            )
        ) -> TDataItems:
            params = {"since": cursor_date.last_value, "status": "open"}
            for page in client.paginate("repos/dlt-hub/dlt/pulls", params=params):
                yield page

        return github_pulls

    # define new dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="duckdb",
        dataset_name="github_data",
    )

    # run the pipeline with the new resource
    load_info = pipeline.run(github_source())
    print(load_info)
    return (
        BearerTokenAuth,
        DltResource,
        HeaderLinkPaginator,
        Iterable,
        RESTClient,
        TDataItems,
        dlt,
        load_info,
        os,
        pipeline,
        userdata,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ##  **Load info**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        `Load Info:` This is a collection of useful information about the recently loaded data. It includes details like the pipeline and dataset name, destination information, and a list of loaded packages with their statuses, file sizes, types, and error messages (if any).

        `Load Package:` A load package is a collection of jobs with data for specific tables, generated during each execution of the pipeline. Each package is uniquely identified by a `load_id`.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **(0) CLI**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        From the [`Inspecting & Adjusting Schema`](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_7_inspecting_and_adjusting_schema.ipynb) section, we've already learned that we can see which schema changes a load package introduced with the command:

        ```
        dlt pipeline -v <pipeline_name> load-package
        ```

        The verbose flag only shows schema changes, so if we run it **without** the flag, we will still see the most recent load package info:
        """
    )
    return


app._unparsable_cell(
    r"""
    !dlt pipeline github_pipeline load-package
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        The `load_id` of a particular package is added to the top data tables (parent tables) and to the special `_dlt_loads` table with a status of `0` when the load process is fully completed. The `_dlt_loads` table tracks completed loads and allows chaining transformations on top of them.

        We can also view load package info for a specific `load_id` (replace the value with the one output above):

        """
    )
    return


app._unparsable_cell(
    r"""
    !dlt pipeline github_pipeline load-package 1741348101.3398592
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **(0) Python**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        From the [`Inspecting & Adjusting Schema`](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_7_inspecting_and_adjusting_schema.ipynb) section, we've also learned that a schema can be accessed with:

        ```python
        print(load_info.load_packages[0].schema)
        ```
        Similarly, if we drop the schema part, we will get the load package info:
        """
    )
    return


@app.cell
def _(load_info):
    print(load_info.load_packages[0])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        which has the following public methods and attributes:
        """
    )
    return


@app.cell
def _(load_info):
    # This code snippet just prints out the public methoda and attributes of the schema object in load info
    all_attributes_methods = dir(load_info.load_packages[0])
    public_attributes_methods = [
        attr for attr in all_attributes_methods if not attr.startswith("_")
    ]

    print(f"{'Attribute/Method':<50} {'Type':<10}")
    print("-" * 40)
    for attr in public_attributes_methods:
        attr_value = getattr(load_info.load_packages[0], attr)
        if callable(attr_value):
            print(f"{attr:<50} {'method':<10}")
        else:
            print(f"{attr:<50} {'attribute':<10}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ##  **Trace**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
         `Trace`: A trace is a detailed record of the execution of a pipeline. It provides rich information on the pipeline processing steps: **extract**, **normalize**, and **load**. It also shows the last `load_info`.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **(0) CLI**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        You can access the pipeline trace using the command:


        ```
        dlt pipeline <pipeline_name> trace
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Try running it on the github issues pipeline:
        """
    )
    return


app._unparsable_cell(
    r"""
    !dlt pipeline github_pipeline trace
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **(0) Python**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        We can also print out the trace in code:
        """
    )
    return


@app.cell
def _(pipeline):
    # print human friendly trace information
    print(pipeline.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Separately receive the extract stage info:
        """
    )
    return


@app.cell
def _(pipeline):
    # print human friendly trace information
    print(pipeline.last_trace.last_extract_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        As well as the normalization stage info with:
        """
    )
    return


@app.cell
def _(pipeline):
    # print human friendly normalization information
    print(pipeline.last_trace.last_normalize_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        How many rows of data were normalized:
        """
    )
    return


@app.cell
def _(pipeline):
    # access row counts dictionary of normalize info
    print(pipeline.last_trace.last_normalize_info.row_counts)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        And finally the load stage info:
        """
    )
    return


@app.cell
def _(pipeline):
    # print human friendly load information
    print(pipeline.last_trace.last_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ##  **State**

        [`The pipeline state`](https://dlthub.com/docs/general-usage/state) is a Python dictionary that lives alongside your data. You can store values in it during a pipeline run, and then retrieve them in the next pipeline run. It's used for tasks like preserving the "last value" or similar loading checkpoints, and it gets committed atomically with the data. The state is stored locally in the pipeline working directory and is also stored at the destination for future runs.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **When to use pipeline state**
        - `dlt` uses the state internally to implement last value incremental loading. This use case should cover around 90% of your needs to use the pipeline state.
        - Store a list of already requested entities if the list is not much bigger than 100k elements.
        - Store large dictionaries of last values if you are not able to implement it with the standard incremental construct.
        - Store the custom fields dictionaries, dynamic configurations and other source-scoped state.

        **When not to use pipeline state**

        Do not use `dlt` state when it may grow to millions of elements.
        For example, storing modification timestamps for millions of user records is a bad idea.
        In that case, you could:

        - Store the state in DynamoDB, Redis, etc., keeping in mind that if the extract stage fails, you may end up with invalid state.
        - Use your loaded data as the state. `dlt` exposes the current pipeline via `dlt.current.pipeline()`, from which you can obtain a `sql_client` and load the data you need. If you choose this approach, try to process your user records in batches.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **(0) CLI**

        """
    )
    return


app._unparsable_cell(
    r"""
    !dlt pipeline -v github_pipeline info
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **(1) Python**

        """
    )
    return


@app.cell
def _():
    import json

    def read_state(filepath: str) -> str:
        with open(filepath, "r", encoding="utf-8") as file:
            data = json.load(file)
            pretty_json = json.dumps(data, indent=4)
            return pretty_json

    return (read_state,)


@app.cell
def _(read_state):
    # stored in your default pipelines folder
    print(read_state("/var/dlt/pipelines/github_pipeline/state.json"))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **Modify State**

        The pipeline state is a Python dictionary that lives alongside your data; you can store values in it and, on the next pipeline run, request them back.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ####  **(0) Resource state**

        You can **read** and **write** the state in your resources using:

        ```python
        dlt.current.resource_state().get()
        ```
        and

        ```python
        dlt.current.resource_state().setdefault(key, value)
        ```
        """
    )
    return


@app.cell
def _(
    BearerTokenAuth,
    DltResource,
    HeaderLinkPaginator,
    Iterable,
    RESTClient,
    TDataItems,
    dlt,
    os,
    userdata,
):
    os.environ["SOURCES__SECRET_KEY"] = userdata.get("SECRET_KEY")

    @dlt.source
    def github_source_1(secret_key: str = dlt.secrets.value) -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=secret_key),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource
        def github_pulls(
            cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "updated_at", initial_value="2024-12-01"
            )
        ) -> TDataItems:
            dlt.current.resource_state().setdefault(
                "new_key", ["first_value", "second_value"]
            )
            params = {"since": cursor_date.last_value, "status": "open"}
            for page in client.paginate("repos/dlt-hub/dlt/pulls", params=params):
                yield page

        return github_pulls

    pipeline_1 = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="duckdb",
        dataset_name="github_data",
    )
    load_info_1 = pipeline_1.run(github_source_1())
    print(load_info_1)
    return


@app.cell
def _(read_state):
    print(read_state("/var/dlt/pipelines/github_pipeline/state.json"))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        In the state, you will see the new items:
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ![Lesson_8_Understanding_Pipeline_Metadata_and_State_img2](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_8_Understanding_Pipeline_Metadata_and_State_img2.png)
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        You can modify any item in the state dict:

        ```python
        new_keys = dlt.current.resource_state().setdefault("new_key", ["first_value", "second_value"])

        if "something_happend":
            new_keys.append("third_value")

        incremental_dict = dlt.current.resource_state().get("incremental")
        incremental_dict.update({"second_new_key": "fourth_value"})
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Full example:
        """
    )
    return


@app.cell
def _(
    BearerTokenAuth,
    DltResource,
    HeaderLinkPaginator,
    Iterable,
    RESTClient,
    TDataItems,
    dlt,
    os,
    userdata,
):
    os.environ["SOURCES__SECRET_KEY"] = userdata.get("SECRET_KEY")

    @dlt.source
    def github_source_2(secret_key: str = dlt.secrets.value) -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=secret_key),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource
        def github_pulls(
            cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "updated_at", initial_value="2024-12-01"
            )
        ) -> TDataItems:
            new_keys = dlt.current.resource_state().setdefault(
                "new_key", ["first_value", "second_value"]
            )
            if "something_happened":
                new_keys.append("third_value")
            incremental_dict = dlt.current.resource_state().get("incremental")
            incremental_dict.update({"second_new_key": "fourth_value"})
            params = {"since": cursor_date.last_value, "status": "open"}
            for page in client.paginate("repos/dlt-hub/dlt/pulls", params=params):
                yield page

        return github_pulls

    pipeline_2 = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="duckdb",
        dataset_name="github_data",
    )
    load_info_2 = pipeline_2.run(github_source_2())
    print(load_info_2)
    return


@app.cell
def _(read_state):
    print(read_state("/var/dlt/pipelines/github_pipeline/state.json"))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ####  **(1) Source state**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        You can also access the source-scoped state with `dlt.current.source_state()` which can be shared across resources of a particular source and is also available **read-only** in the source-decorated functions. The most common use case for the source-scoped state is to store the mapping of custom fields to their displayable names.

        Let's read some custom keys from the state with:
        ```python
        source_new_keys = dlt.current.source_state().get("resources", {}).get("github_pulls", {}).get("new_key")
        ```
        Full example:
        """
    )
    return


@app.cell
def _(
    BearerTokenAuth,
    DltResource,
    HeaderLinkPaginator,
    Iterable,
    RESTClient,
    TDataItems,
    dlt,
    os,
    userdata,
):
    os.environ["SOURCES__SECRET_KEY"] = userdata.get("SECRET_KEY")

    @dlt.source
    def github_source_3(secret_key: str = dlt.secrets.value) -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=secret_key),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource
        def github_pulls(
            cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "updated_at", initial_value="2024-12-01"
            )
        ) -> TDataItems:
            params = {"since": cursor_date.last_value, "status": "open"}
            for page in client.paginate("repos/dlt-hub/dlt/pulls", params=params):
                yield page
            source_new_keys = (
                dlt.current.source_state()
                .get("resources", {})
                .get("github_pulls", {})
                .get("new_key")
            )
            print("My custom values: ", source_new_keys)

        return github_pulls

    pipeline_3 = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="duckdb",
        dataset_name="github_data",
    )
    load_info_3 = pipeline_3.run(github_source_3())
    print(load_info_3)
    return (pipeline_3,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **Sync State**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        What if you run your pipeline on, for example, Airflow, where every task gets a clean filesystem and the pipeline working directory is always deleted?

        **dlt loads** your **state** into the destination **together** with all other **data**, and when starting from a clean slate, it will try to restore the state from the destination.

        The remote state is identified by the pipeline name, the destination location (as defined by the credentials), and the destination dataset.
        To reuse **the same state**, use **the same pipeline name** and the same destination.

        The state is stored in the `_dlt_pipeline_state` table at the destination and contains information about the pipeline, the pipeline run (to which the state belongs), and the state blob.

        `dlt` provides the command:

        ```
        dlt pipeline <pipeline name> sync
        ```

        which retrieves the state from that table.

        💡 If you can keep the pipeline working directory across runs, you can disable state sync by setting `restore_from_destination = false` in your `config.toml`.
        """
    )
    return


@app.cell
def _(pipeline_3):
    import duckdb
    from google.colab import data_table
    from IPython.display import display

    data_table.enable_dataframe_formatter()
    conn = duckdb.connect(f"{pipeline_3.pipeline_name}.duckdb")
    conn.sql(f"SET search_path = '{pipeline_3.dataset_name}'")
    stats_table = conn.sql("SELECT * FROM _dlt_pipeline_state").df()
    display(stats_table)
    return (conn,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        The "state" column is a compressed json dictionary.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        |index|version|engine\_version|pipeline\_name|state|created\_at|version\_hash|\_dlt\_load\_id|\_dlt\_id|
        |---|---|---|---|---|---|---|---|---|
        |0|1|4|github\_pipeline|eNplkN....6+/m/QA7mbNc|2025-03-10 14:02:34\.340458+00:00|pnp+9AIA5jAGx5LKon6zWmPnfYVb10ROa5aIKjv9O0I=|1741615353\.5473728|FOzn5XuSZ/y/BQ|
        """
    )
    return


app._unparsable_cell(
    r"""
    !dlt --non-interactive pipeline github_pipeline sync
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ###  **Reset State**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **To fully reset the state:**

        - Drop the destination dataset to fully reset the pipeline.
        - Set the `dev_mode` flag when creating the pipeline.
        - Use the `dlt pipeline drop --drop-all` command to drop state and tables for a given schema name.

        **To partially reset the state:**

        - Use the `dlt pipeline drop <resource_name>` command to drop state and tables for a given resource.
        - Use the `dlt pipeline drop --state-paths` command to reset the state at a given path without touching the tables or data.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **Example for a partial reset:**

        >  In an ipynb environment, when the duckdb connection we opened is not yet closed -> close the connection before attempting to edit the pipeline through the CLI.
        """
    )
    return


@app.cell
def _(conn):
    conn.close()
    return


app._unparsable_cell(
    r"""
    !yes | dlt pipeline github_pipeline drop github_pulls
    """,
    name="_",
)


app._unparsable_cell(
    r"""
    !dlt pipeline -v github_pipeline info
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        🎊🎊🎊 That's it! We hope you enjoyed this course and learned more about `dlt`! 🎊🎊🎊

        Please share your feedback with us: [Feedback Google Form](https://forms.gle/1NYrGcRj5gLQ4WDt8) 🌼
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
