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
    # **Recap of [Lesson 4](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_4_using_pre_build_sources_and_destinations.ipynb) 👩‍💻🚀**

    1. Listed all available verified sources.
    2. Initialized the `github_api` verified source.
    3. Explored the built-in `rest_api` source.
    4. Explored the built-in `sql_database` source.
    5. Explored the built-in `filesystem` source.
    6. Learned how to switch between destinations.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---

    # **Write Disposition and Incremental Loading** ⚙️🧠 [![Open in molab](https://marimo.io/molab-shield.svg)](https://molab.marimo.io/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_5_write_disposition_and_incremental_loading.py) [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_5_write_disposition_and_incremental_loading.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_5_write_disposition_and_incremental_loading.ipynb)


    **Here, you will learn:**
    - `dlt` write dispositions:
      - Append
      - Replace
      - Merge
    - What incremental loading is
    - How to update and deduplicate your data
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ## **`dlt` write dispositions**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    A **write disposition** in the context of the `dlt` library defines how data should be written to the destination. There are three types:

    - **Append**: The **default** disposition. It appends new data to the existing data in the destination.

    - **Replace**: This disposition replaces all existing data at the destination with the new data from the resource. It **deletes** all previous data and **recreates** the schema before loading.

    - **Merge**: This disposition merges incoming data with existing data at the destination. For `merge`, you must specify a `primary_key` for the resource.

    The choice of write disposition depends on your dataset and how you extract it. For more details, refer to the [Incremental loading page](https://dlthub.com/docs/general-usage/incremental-loading).

    You can specify a `write_disposition` in the resource decorator:

    ```python
    @dlt.resource(write_disposition="append")
    def my_resource():
      ...
      yield data
    ```

    Or directly in the pipeline run:

    ```python
    load_info = pipeline.run(my_resource, write_disposition="replace")
    ```

    > If both are specified, the write disposition at the pipeline run level overrides the one set at the resource level.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ### **1. Append**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""As we have already said, `append` is the default loading behavior. Now we will explore how this write disposition works."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's remember our Quick Start data sample with pokemons:""")
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
    mo.md(
        r"""We create a `dlt` pipeline as usual and load this data into DuckDB."""
    )
    return


@app.cell
def _(data):
    import dlt
    from dlt.common.typing import TDataItems

    @dlt.resource(name="pokemon", write_disposition="append")
    def append_pokemon() -> TDataItems:
        yield data

    append_pipeline = dlt.pipeline(
        pipeline_name="append_poke_pipeline",
        destination="duckdb",
        dataset_name="pokemon_data",
    )
    _load_info = append_pipeline.run(append_pokemon)
    print(_load_info)
    # explore loaded data
    append_pipeline.dataset().pokemon.df()
    return TDataItems, append_pipeline, append_pokemon, dlt


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Run this example **twice**, and you'll notice that each time a copy of the data is added to your tables. We call this load mode **append**, and it is very useful.

    Example use case: when you have a new folder created daily with JSON log files, and you want to ingest them incrementally.
    """)
    return


@app.cell
def _(append_pipeline, append_pokemon):
    _load_info = append_pipeline.run(append_pokemon)
    print(_load_info)
    # explore loaded data
    append_pipeline.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ### **2. Replace**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Perhaps this duplicated data is not what you want in your work projects. For example, if your data was updated, how can we refresh it in the database? One way is to tell `dlt` to **replace** the data in the existing tables by using a **write_disposition**."""
    )
    return


@app.cell
def _(TDataItems, data, dlt):
    @dlt.resource(name="pokemon", write_disposition="replace")
    def replace_pokemon() -> TDataItems:
        yield data

    replace_pipeline = dlt.pipeline(
        pipeline_name="replace_poke_pipeline",
        destination="duckdb",
        dataset_name="pokemon_data",
    )
    _load_info = replace_pipeline.run(replace_pokemon)
    print(_load_info)
    replace_pipeline.dataset().pokemon.df()
    return replace_pipeline, replace_pokemon


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Run it again:""")
    return


@app.cell
def _(replace_pipeline, replace_pokemon):
    _load_info = replace_pipeline.run(replace_pokemon)
    print(_load_info)
    # explore loaded data
    replace_pipeline.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""TADA! No duplicates, your data was [fully refreshed](https://dlthub.com/docs/general-usage/full-loading)."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ### **3. [Merge](https://dlthub.com/docs/general-usage/incremental-loading#merge-incremental-loading)**

    Consider a scenario where the data in the source has been updated, but you want to avoid reloading the entire dataset.



    Merge write disposition is used to merge new data into the destination, using a `merge_key` and/or **deduplicating**/**upserting** new data using a `primary_key`.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_5_Write_disposition_and_incremental_loading_img1](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_5_Write_disposition_and_incremental_loading_img1.jpeg)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The **merge** write disposition can be useful in several situations:

    1.  If you have a dataset where records are frequently updated and you want to reflect these changes in your database, the `merge` write disposition can be used. It will **update the existing records** with the new data instead of creating duplicate entries.

    2. If your data source occasionally sends **duplicate records**, the merge write disposition can help handle this. It uses a `primary_key` to identify unique records, so if a duplicate record (with the same `primary_key`) is encountered, it will be merged with the existing record instead of creating a new one.

    3. If you are dealing with **Slowly Changing Dimensions** (SCD) where the attribute of a record changes over time and you want to maintain a history of these changes, you can use the `merge` write disposition with the scd2 strategy.


    When using the merge disposition, you need to specify a `primary_key` or `merge_key` for the resource.
    """)
    return


@app.cell
def _(TDataItems, data, dlt):
    @dlt.resource(name="pokemon", write_disposition="merge", primary_key="id")
    def merge_pokemon() -> TDataItems:
        yield data

    merge_pipeline = dlt.pipeline(
        pipeline_name="poke_pipeline_merge",
        destination="duckdb",
        dataset_name="pokemon_data",
    )
    _load_info = merge_pipeline.run(merge_pokemon)
    print(_load_info)
    merge_pipeline.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The merge write disposition can be used with three different strategies:

    * delete-insert (default strategy)
    * scd2
    * upsert
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ---
    ##  **Incremental Loading**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Incremental loading is the act of loading only new or changed data and not old records that we already loaded.

    Imagine you’re a Pokémon trainer trying to catch ‘em all. You don’t want to keep visiting the same old PokéStops, catching the same old Bulbasaurs—you only want to find new and exciting Pokémon that have appeared since your last trip. That’s what incremental loading is all about: collecting only the new data that’s been added or changed, without wasting your Poké Balls (or database resources) on what you already have.

    In this example, we have a dataset of Pokémon, each with a **unique ID**, their **name**, **size** (height and weight), and **when** they were "caught" (`created_at` field).

    ### **Step 1: Adding the `created_at` Field**
    """)
    return


@app.cell
def _():
    # We added `created_at` field to the data
    created_data = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "created_at": "2024-12-01",
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "created_at": "2024-09-01",
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 6, "height": 0.4},
            "created_at": "2023-06-01",
        },
    ]
    return (created_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **The goal**: Load only Pokémons caught after January 1, 2024, skipping the ones you already have.

    ### **Step 2: Defining the incremental logic**

    Using `dlt`, we set up an [incremental filter](https://www.google.com/url?q=https://dlthub.com/docs/general-usage/incremental-loading%23incremental-loading-with-a-cursor-field&sa=D&source=editors&ust=1734717286675253&usg=AOvVaw3rAF3y3p86sGt49ImCTgon) to only fetch Pokémons caught after a certain date:
    ```python
    cursor_date = dlt.sources.incremental("created_at", initial_value="2024-01-01")
    ```
    This tells `dlt`:
    - **Start date**: January 1, 2024 (`initial_value`).
    - **Field to track**: `created_at` (our timestamp).

    As you run the pipeline repeatedly, `dlt` will keep track of the latest `created_at` value processed. It will skip records older than this date in future runs.
    """)
    return


@app.cell
def _(TDataItems, created_data, dlt):
    @dlt.resource(name="pokemon", write_disposition="append")
    def incremental_pokemon(
        cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
            "created_at", initial_value="2024-01-01"
        )
    ) -> TDataItems:
        yield created_data
    return (incremental_pokemon,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""We use the `@dlt.resource` decorator to declare table **name** to which data will be loaded and **write disposition**, which is **append** by default."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Step 3: Running the pipeline**
    Finally, we run our pipeline and load the fresh Pokémon data:
    """)
    return


@app.cell
def _(dlt, incremental_pokemon):
    incremental_pipeline = dlt.pipeline(
        pipeline_name="poke_pipeline_incremental",
        destination="duckdb",
        dataset_name="pokemon_data",
    )
    _load_info = incremental_pipeline.run(incremental_pokemon)
    print(_load_info)
    # explore loaded data
    incremental_pipeline.dataset().pokemon.df()
    return (incremental_pipeline,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This:
    1. Loads **only Charmander and Bulbasaur** (caught after 2024-01-01).
    2. Skips Pikachu because it’s old news.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Only data for 2024 year was loaded.""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_5_Write_disposition_and_incremental_loading_img2](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_5_Write_disposition_and_incremental_loading_img2.png)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Run the same pipeline again. The pipeline will detect that there are **no new records** based on the `created_at` field and the incremental cursor. As a result, **no new data will be loaded** into the destination:
    >0 load package(s) were loaded
    """)
    return


@app.cell
def _(incremental_pipeline, incremental_pokemon):
    _load_info = incremental_pipeline.run(incremental_pokemon)
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Why incremental loading matters**

    * **Efficiency**. Skip redundant data, saving time and resources.
    * **Scalability**. Handle growing datasets without bottlenecks.
    * **Automation**. Let the tool track changes for you—no manual effort.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Update and deduplicate your data**
    The script above finds new pokemons and adds them to the database. It will ignore any updates to user information.
    """)
    return


@app.cell
def _():
    # We added `updated_at` field to the data
    updated_data = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "created_at": "2024-12-01",
            "updated_at": "2024-12-01",
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "created_at": "2024-09-01",
            "updated_at": "2024-09-01",
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {
                "weight": 9,
                "height": 0.4,
            },
            "created_at": "2023-06-01",
            "updated_at": "2024-12-16",
        },
    ]
    return (updated_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Get always fresh content of all the pokemons: combine an **incremental load** with **merge** write disposition, like in the script below."""
    )
    return


@app.cell
def _(TDataItems, dlt):
    @dlt.resource(name="pokemon", write_disposition="merge", primary_key="id")
    def dedup_pokemon(
        data: TDataItems,
        cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value="2024-01-01"
        ),
    ) -> TDataItems:
        yield data
    return (dedup_pokemon,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""The incremental cursor keeps an eye on the `updated_at` field. Every time the pipeline runs, it only processes records with `updated_at` values greater than the last run."""
    )
    return


@app.cell
def _(dedup_pokemon, dlt, updated_data):
    dedup_pipeline = dlt.pipeline(
        pipeline_name="poke_pipeline_dedup",
        destination="duckdb",
        dataset_name="pokemon_data",
    )
    _load_info = dedup_pipeline.run(dedup_pokemon(updated_data))
    print(_load_info)
    # explore loaded data
    dedup_pipeline.dataset().pokemon.df()
    return (dedup_pipeline,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    All Pokémons are processed because this is the pipeline’s first run.

    Now, let’s say Pikachu goes to gym and sheds some weight (down to 7.5), and the `updated_at` field is set to `2024-12-23`.
    """)
    return


@app.cell
def _():
    reupdated_data = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "created_at": "2024-12-01",
            "updated_at": "2024-12-01",
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "created_at": "2024-09-01",
            "updated_at": "2024-09-01",
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 7.5, "height": 0.4},
            "created_at": "2023-06-01",
            "updated_at": "2024-12-23",
        },
    ]
    return (reupdated_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Run the same pipeline:""")
    return


@app.cell
def _(dedup_pipeline, dedup_pokemon, reupdated_data):
    _load_info = dedup_pipeline.run(dedup_pokemon(reupdated_data))
    print(_load_info)
    # explore loaded data
    dedup_pipeline.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **What happened?**

    * The pipeline detected that `updated_at` for Bulbasaur and Charmander hasn’t changed—they’re skipped.
    * Pikachu’s record was updated to reflect the latest weight.

    You can see that the **`_dlt_load_id`** for Bulbasaur and Charmander remained the same, but for Pikachu it was changed since only the updated Pikachu data was loaded into the destination.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The **`dlt.sources.incremental`** instance above has the following attributes:

    * **`cursor_date.initial_value`** which is always equal to "2024-01-01" passed in the constructor;
    * **`cursor_date.start_value`** a maximum `updated_at` value from the previous run or the `initial_value` on the first run;
    * **`cursor_date.last_value`** a "real-time" `updated_at` value updated with each yielded item or page. Before the first yield, it equals `start_value`;
    * **`cursor_date.end_value`** (not used here) marking the end of the backfill range.

    ## **Example**
    You can use them in the resource code to make **more efficient requests**. Take look at the GitHub API example:
    """)
    return


@app.cell
def _(TDataItems, dlt, os):
    from typing import Iterable
    from dlt.extract import DltResource
    from dlt.sources.helpers import requests
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

    dlt.secrets["SOURCES__ACCESS_TOKEN"] = os.getenv("SECRET_KEY")

    @dlt.source
    def github_source(access_token: str = dlt.secrets.value) -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource(name="issues", write_disposition="merge", primary_key="id")
        def github_issues(
            cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "updated_at", initial_value="2024-12-01"
            )
        ) -> TDataItems:
            params = {"since": cursor_date.last_value, "status": "open"}
            for page in client.paginate("repos/dlt-hub/dlt/issues", params=params):
                yield page

        return github_issues

    pipeline = dlt.pipeline(pipeline_name="github_incr", destination="duckdb")
    _load_info = pipeline.run(github_source())
    print(_load_info)
    return github_source, pipeline


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Pay attention to how we use the **since** GitHub API parameter and `cursor_date.last_value` to tell GitHub which issues we are interested in. `cursor_date.last_value` holds the last `cursor_date` value from the previous run.

    Run the pipeline again and make sure that **no data is loaded**.
    """)
    return


@app.cell
def _(github_source, pipeline):
    # run the pipeline with the new resource
    _load_info = pipeline.run(github_source())
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Apply Hints**

    Alternatively, you can use `apply_hints` on a resource to define an incremental field:

    ```python
    resource = resource()
    resource.apply_hints(incremental=dlt.sources.incremental("updated_at"))
    ```

    When you apply an incremental hint using `apply_hints`, the source still performs a full extract. The incremental hint is used by `dlt` to filter the data after it has been extracted, before it is loaded into the destination.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## **Exercise 1: Make the GitHub API pipeline incremental**

    In the previous lessons, you built a pipeline to pull data from the GitHub API. Now, let’s level it up by making it incremental, so it fetches only new or updated data.


    Transform your GitHub API pipeline to use incremental loading. This means:

    * Implement a new `dlt.resource` for `pulls/comments` (List comments for Pull Requests) endpoint.
    * Fetch only pulls comments updated after the last pipeline run.
    * Use the `updated_at` field from the GitHub API as the incremental cursor.
    * [Endpoint documentation](https://docs.github.com/en/rest/pulls/comments?apiVersion=2022-11-28#list-review-comments-in-a-repository)
    * Endpoint URL: `https://api.github.com/repos/OWNER/REPO/pulls/comments`
    * Use the `since` parameter - only show results that were last updated after the given time - and `last_value`.
    * `initial_value` is `2024-12-01`.


    ### Question

    How many columns does the `comments` table have?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""✅ ▶ Proceed to the [next lesson](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_6_how_dlt_works.ipynb)!"""
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
