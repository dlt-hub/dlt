import marimo

__generated_with = "0.14.10"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Merge and replace strategies & Advanced tricks [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_6_write_disposition_strategies_and_advanced_tricks.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_6_write_disposition_strategies_and_advanced_tricks.ipynb)

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # **Recap**

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ## **`dlt` write dispositions**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Write disposition in the context of the dlt library defines how the data should be written to the destination. There are three types of write dispositions:

        * **Append**: This is the **default** disposition. It will append the data to the existing data in the destination.

        * **Replace**: This disposition replaces the data in the destination with the data from the resource. It **deletes** all the data and **recreates** the schema before loading the data.

        * **Merge**: This write disposition merges the data from the resource with the data at the destination. For the merge disposition, you need to specify a `primary_key` for the resource.

        The write disposition you choose depends on the dataset and how you can extract it. For more details, you can refer to the [Incremental loading page](https://dlthub.com/docs/general-usage/incremental-loading).



        A `write_disposition` in `dlt` can specified in the resource decorator:

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

        > In case you specify both, the write disposition specified at the pipeline run level will override the write disposition specified at the resource level.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---

        ### **Replace**

        The `replace` strategy in the dlt library is used for **full loading** of data. This strategy completely overwrites the existing data with the new dataset. It's useful when you want to refresh the entire table with the latest data. It's important to note that this strategy technically does not load only new data but instead reloads all data: old and new.

        **Example: E-commerce Product Catalog refresh**

        - A large retailer (e.g., Amazon) needs to refresh their product catalog daily.

        - Using a replace strategy ensures all product details are up-to-date and no longer available products are removed.

        - Critical for ensuring customers see only valid products and prices.

        **Importance**: Guarantees consistency and accuracy in fast-changing datasets where full refreshes are simpler than merging updates.

        **Risks**: Data downtime if the refresh fails or takes too long.

        In dlt, you can control how the data is loaded into the destination table by setting the `write_disposition` parameter in the resource configuration. When you set the `write_disposition` to `replace`, it replaces the data in the destination table with the new data.

        For more details, you can refer to the following documentation pages:

        - [Full loading](https://dlthub.com/docs/general-usage/full-loading)
        - [Write dispositions](https://dlthub.com/docs/general-usage/incremental-loading#the-3-write-dispositions)

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ### **Merge**

        Consider a scenario where the data in the source has been updated, but you want to avoid reloading the entire dataset.

        **Example: Customer data integration (e.g., Salesforce CRM)**

        - A business integrates Salesforce CRM data with its data warehouse.

        - Sales representatives continuously update customer profiles. A merge strategy ensures that only the changed records are updated without affecting the entire dataset.

        - Useful for integrating various CRM systems where incremental updates are preferred over full reloads.

        Merge write disposition is used to merge new data into the destination, using a `merge_key` and/or **deduplicating**/**upserting** new data using a `primary_key`.


        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""

        The **merge** write disposition can be useful in several situations:

        1.  If you have a dataset where records are frequently updated and you want to reflect these changes in your database, the merge write disposition can be used. It will **update the existing records** with the new data instead of creating duplicate entries.

        2. If your data source occasionally sends **duplicate records**, the merge write disposition can help handle this. It uses a `primary_key` to identify unique records, so if a duplicate record (with the same `primary_key`) is encountered, it will be merged with the existing record instead of creating a new one.

        3. If you are dealing with **Slowly Changing Dimensions** (SCD) where the attribute of a record changes over time and you want to maintain a history of these changes, you can use the merge write disposition with the scd2 strategy.


        When using the merge disposition, you need to specify a `primary_key` or `merge_key` for the resource.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---

        # **More about write dispositions and incremental loading** ⚙️🧠

        **In the dlt Fundamentals course we've already discussed:**
        - `dlt` write dispositions:
          - Append
          - Replace
          - Merge
        - What incremental loading is.

        **Now, we will cover** the different strategies for `merge` write disposition:
        - `delete-insert` strategy.
        - `upsert` strategy.
        - `SCD2` strategy.

        We also will take a look at
        * Hard deletes.
        * Falling back for incremental cursors.
        * Backfills.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---

        ## **Replace strategies**

        In this lesson, we will explore the concept of **full loading**, where we completely reload the data of our tables, removing all existing data and replacing it with new data from our source.


        We will also delve into the different replace strategies that dlt implements for doing a full load on your table:
        - `truncate-and-insert`,
        - `insert-from-staging`,
        - `staging-optimized`.



        Each of these strategies has its own unique characteristics and use cases, and we will discuss them in detail.


        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **I. Truncate-and-Insert Strategy**

        **Overview**

        The `truncate-and-insert` strategy is the **default** replace strategy in dlt and is the **fastest** of all three strategies. This strategy is particularly useful when you want to completely refresh your data and you don't need to maintain the existing data in your tables during the load process.

        When you load data with the `truncate-and-insert` strategy, the destination tables will be truncated at the beginning of the load. This means that all existing data in the tables will be removed. After truncating the tables, the new data will be inserted. The insertion of new data happens consecutively but not within the same transaction.

        **Example: Daily ETL job for financial reports (e.g., Bloomberg Terminal Data)**

        - Daily financial summaries are generated and processed overnight.

        - Using `truncate-and-insert`, the pipeline ensures that analysts work with the most recent data every morning.

        **Configuration**

        You can select the `truncate-and-insert` strategy with a setting in your `config.toml` file.

        ```yaml
        [destination]
        replace_strategy = "truncate-and-insert"
        ```

        **Limitations**

        However, it's important to note that the **downside** of this strategy is that your **tables will have no data for a while** until the load is completed. You may end up with new data in some tables and no data in other tables if the load fails during the run. Such an incomplete load may be detected by checking if the `_dlt_loads` table contains a load id from `_dlt_load_id` of the replaced tables. If you prefer to have no data downtime, please use one of the other strategies.


        Here's an example of how to use the `truncate-and-insert` strategy with the Pokemon data:

        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install "dlt[duckdb]"
    return


@app.cell
def _():
    from typing import List, Dict, Any
    import dlt
    from datetime import datetime

    data: List[Dict[str, Any]] = [
        {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
        {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
        {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
    ]
    dlt.secrets["destination.replace_strategy"] = "truncate-and-insert"
    pipeline = dlt.pipeline(
        pipeline_name="pokemon_load_1",
        destination="duckdb",
        dataset_name="pokemon_data_1",
    )
    _load_info = pipeline.run(data, table_name="pokemon", write_disposition="replace")
    print(pipeline.last_trace)
    return dlt, pipeline


@app.cell
def _(pipeline):
    with pipeline.sql_client() as _client:
        with _client.execute_query("SHOW ALL TABLES") as _table:
            _tables = _table.df()
    _tables
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""


        In this example, we're using the `replace_strategy="truncate-and-insert"` parameter in the pipeline method to indicate that we want to use the `truncate-and-insert` strategy for replacing data.


        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **II. Insert-from-staging Strategy**

        **Overview**

        The `insert-from-staging` strategy is used when you want to maintain a consistent state between nested and root tables at all times, with zero downtime. This strategy loads all new data into staging tables away from your final destination tables and then truncates and inserts the new data in one transaction.

        **Example: Airline reservation systems (e.g., Amadeus, Sabre)**

        - Ensuring that updated flight availability information doesn't interrupt user queries during ingestion.

        - Data is first written to staging tables and only swapped to production tables when the operation is complete.

        **Configuration**

        You can select the `insert-from-staging` strategy with a setting in your `config.toml` file. If you do not select a strategy, dlt will default to `truncate-and-insert`.

        ```yaml
        [destination]
        replace_strategy = "insert-from-staging"
        ```

        **Limitations**

        The `insert-from-staging` strategy, while ensuring zero downtime and maintaining a consistent state between nested and root tables, is **the slowest** of all three strategies. It loads all new data into staging tables away from your final destination tables and then truncates and inserts the new data in one transaction. This process can be time-consuming, especially for large datasets.

        Here's an example of how you can use this strategy:
        """
    )
    return


@app.cell
def _(dlt):
    data_1 = [
        {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
        {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
        {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
    ]
    dlt.secrets["destination.replace_strategy"] = "insert-from-staging"
    pipeline_1 = dlt.pipeline(
        pipeline_name="pokemon_load_2",
        destination="duckdb",
        dataset_name="pokemon_data_2",
    )
    _load_info = pipeline_1.run(
        data_1, table_name="pokemon", write_disposition="replace"
    )
    print(pipeline_1.last_trace)
    return (pipeline_1,)


@app.cell
def _(pipeline_1):
    with pipeline_1.sql_client() as _client:
        with _client.execute_query("SHOW ALL TABLES") as _table:
            _tables = _table.df()
    _tables
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        We see the introduction of the [staging](https://dlthub.com/docs/dlt-ecosystem/staging) schema called `pokemon_data_2_staging`.


        In this example, the `insert-from-staging` strategy will load the pokemon data **into a staging table** in the `pokemon_data_2_staging` schema in DuckDB (or any other destination you choose).

        Let's check the content of this table:
        """
    )
    return


@app.cell
def _(pipeline_1):
    with pipeline_1.sql_client() as _client:
        with _client.execute_query(
            "SELECT * from pokemon_data_2_staging.pokemon"
        ) as _table:
            _tables = _table.df()
    _tables
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        We see that the staging table contains all the data we loaded.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""


        dlt will then **truncate** the destination table and **insert** the new data in one transaction, ensuring that the destination dataset is always in a consistent state.

        For more details about the `insert-from-staging` strategy, you can refer to the [dlt documentation.](https://dlthub.com/docs/general-usage/full-loading#the-insert-from-staging-strategy)
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **III. Staging-optimized Strategy**


        The `staging-optimized` replace strategy is one of the three strategies implemented by dlt for doing a full load on your table.

        **Overview**

        The `staging-optimized` strategy **combines the benefits** of the `insert-from-staging` strategy with certain optimizations for **faster** loading on some destinations. However, it comes with a **trade-off**: destination tables may be dropped and recreated in some cases. This means that any views or other constraints you have placed on those tables will be dropped with the table.

        If you have a setup where you need to retain your destination tables, you should not use the `staging-optimized` strategy. On the other hand, if you do not care about tables being dropped but need the benefits of the `insert-from-staging` with some performance (and cost) saving opportunities, this strategy is a good choice.

        **Example: Data warehousing for Business Intelligence (e.g., Snowflake, BigQuery)**

        - When refreshing tables with daily marketing analytics, staging-optimized strategy uses clone operations.

        - Clone operations in platforms like Snowflake are fast and cost-effective since they avoid data copying.

        **How it works**

        The `staging-optimized` strategy behaves differently across destinations:

        - **Postgres**: After loading the new data into the staging tables, the destination tables will be dropped and replaced by the staging tables. No data needs to be moved, so this strategy is almost as fast as `truncate-and-insert`.

        - **BigQuery**: After loading the new data into the staging tables, the destination tables will be dropped and recreated with a clone command from the staging tables. This is a low-cost and fast way to create a second independent table from the data of another. You can learn more about table cloning on BigQuery [here](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery).

        - **Snowflake**: After loading the new data into the staging tables, the destination tables will be dropped and recreated with a clone command from the staging tables. This is a low-cost and fast way to create a second independent table from the data of another. You can learn more about table cloning on Snowflake [here](https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake).

        - For all **other destinations**, please look at their respective [documentation](https://dlthub.com/docs/dlt-ecosystem/destinations/) pages to see if and how the `staging-optimized` strategy is implemented. If it is not implemented, dlt will fall back to the `insert-from-staging` strategy.

        **Configuration**

        You can select the `staging-optimized` strategy with a setting in your `config.toml` file. If you do not select a strategy, dlt will default to `truncate-and-insert`.

        ```yaml
        [destination]
        # Set the optimized replace strategy
        replace_strategy = "staging-optimized"
        ```

        **Limitations**

        It's important to note that the `staging-optimized` replace strategy is **not implemented for all destinations**. For example, DuckDB doesn't support this strategy, that's why we skip the code example.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ## **Merge strategies**

        Append and replace write dispositions are quite simple to use, but with `merge` you need to be more careful.

        Let's create an example database
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
        Let's remember our Pokemon data sample from the dlt Fundamentals course:

        """
    )
    return


@app.cell
def _():
    data_2 = [
        {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
        {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
        {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
    ]
    return (data_2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Load this data into duckdb with merge write disposition.
        """
    )
    return


@app.cell
def _(data_2, dlt):
    from dlt.common.typing import TDataItems, TDataItem

    @dlt.resource(name="pokemon", write_disposition="merge", primary_key="id")
    def pokemon() -> TDataItems:
        yield data_2

    pipeline_2 = dlt.pipeline(
        pipeline_name="poke_pipeline_merge",
        destination="duckdb",
        dataset_name="pokemon_data",
    )
    _load_info = pipeline_2.run(pokemon)
    print(_load_info)
    pipeline_2.dataset().pokemon.df()
    return TDataItem, TDataItems, pipeline_2, pokemon


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        The merge write disposition can be used with three different strategies:

        * delete-insert (default strategy)
        * scd2
        * upsert


        Let's explore these strategies closer.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **I. `delete-insert` strategy**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **Overview**

        The `merge` write disposition has `delete-insert` as the default strategy. Since we haven't specified a strategy in the previous example, this is what was used by default under the hood.

        The `delete-insert` strategy loads data to a **`staging`** dataset, deduplicates the `staging` data if a `primary_key` is provided, **deletes** the data from the destination using `merge_key` and `primary_key`, and then **inserts** the new records.

        > The `merge_key` is used in the `delete-insert` strategy to determine which records to delete from the destination before inserting the new records.

        **Example: Streaming analytics (e.g., Kafka → Data Warehouse)**

        - Streaming logs are ingested with a `delete-insert` strategy to remove outdated entries and ensure only fresh data remains.

        - Used when a `merge_key` is provided, allowing old entries to be purged before new ones are inserted.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""

        Imagine that we want to load only updated data:
        """
    )
    return


@app.cell
def _():
    data_3 = [{"id": "25", "name": "pikachu", "size": {"weight": 7.5, "height": 0.4}}]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Run the pipeline again:
        """
    )
    return


@app.cell
def _(pipeline_2, pokemon):
    _load_info = pipeline_2.run(pokemon)
    print(_load_info)
    pipeline_2.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Data was updated, pikachu data has changed, now he has a different `_dlt_load_id`.

        Let's check what happened in the database in the previous run:
        """
    )
    return


@app.cell
def _(pipeline_2):
    with pipeline_2.sql_client() as _client:
        with _client.execute_query("SHOW ALL TABLES") as _table:
            _tables = _table.df()
    _tables
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        We see agian the staging schema called `pokemon_data_staging`. Let's check the content:
        """
    )
    return


@app.cell
def _(pipeline_2):
    with pipeline_2.sql_client() as _client:
        with _client.execute_query(
            "SELECT * from pokemon_data_staging.pokemon"
        ) as _table:
            _tables = _table.df()
    _tables
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        We see that only new row is in the staging table. Since we used primary key, `dlt` deleted the previous entry of Pikachu and then inserted the new one.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **II. `upsert` strategy**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **Overview**

        The upsert merge strategy does `primary_key` based upserts:

        - update record if key exists in target table
        - insert record if key does not exist in target table
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ```
        @dlt.resource(
            write_disposition={"disposition": "merge", "strategy": "upsert"},
            primary_key="my_primary_key"
        )
        def my_upsert_resource():
            ...
        ...
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **Difference between upsert and delete-insert**

        1. needs a `primary_key`
        2. expects this `primary_key` to be unique (`dlt` does not deduplicate)
        3. does not support `merge_key`
        4. uses MERGE or UPDATE operations to process updates


        **Example: Customer data management (e.g., HubSpot, Salesforce)**

        - Continuous synchronization of customer profiles across multiple systems.

        - Any update to an existing customer is reflected without deleting unrelated data.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ***Not supported in DuckDB.** List of supported destinations can be found in [docs](https://dlthub.com/docs/general-usage/incremental-loading#upsert-strategy).
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### **III. `SCD2` strategy**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        **Overview**

        `dlt` can create Slowly Changing Dimensions Type 2 (SCD2) destination tables for dimension tables that change in the source.

        The resource is expected to provide a full extract of the source table each run.

        A row hash is stored in `_dlt_id` and used as surrogate key to identify source records that have been inserted, updated, or deleted.

        **Example: Financial transaction systems (e.g., Mastercard, Visa)**

        - Keeping history of account balances over time for auditing purposes.

        - Allows analysts to trace how data evolved, which is critical for compliance and troubleshooting.


        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Before running the pipeline, let's re-use our small Pokemon dataset:
        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install dlt
    return


@app.cell
def _():
    data_4 = [
        {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
        {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
        {"id": "25", "name": "pikachu", "size": {"weight": 7.5, "height": 0.4}},
    ]
    return (data_4,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Now, run the pipeline with merge disposition and SCD2 strategy:
        """
    )
    return


@app.cell
def _(TDataItems, data_4, dlt):
    @dlt.resource(
        name="pokemon",
        write_disposition={"disposition": "merge", "strategy": "scd2"},
        primary_key="id",
    )
    def pokemon_1() -> TDataItems:
        yield data_4

    pipeline_3 = dlt.pipeline(
        pipeline_name="pokemon_pipeline",
        destination="duckdb",
        dataset_name="pokemon_scd2",
    )
    _load_info = pipeline_3.run(pokemon_1)
    print(_load_info)
    return pipeline_3, pokemon_1


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Check what happened:
        """
    )
    return


@app.cell
def _(pipeline_3):
    pipeline_3.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        New columns were created:

        - `_dlt_valid_from` – The timestamp when this record was first inserted into the table.
          - All records have the same value, which is when the pipeline first processed them.

        - `_dlt_valid_to` – The timestamp when this record was considered outdated.
          - NaT (Not a Time) means that these records are currently active and have not been superseded by newer versions.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Modify the dataset by changing Pikachu weight again. This simulates a change in source data that should be tracked by SCD2:
        """
    )
    return


@app.cell
def _():
    data_5 = [
        {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
        {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
        {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
    ]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Run the pipeline again with the modified dataset:
        """
    )
    return


@app.cell
def _(pipeline_3, pokemon_1):
    _load_info = pipeline_3.run(pokemon_1)
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Check the database:
        """
    )
    return


@app.cell
def _(pipeline_3):
    pipeline_3.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        SCD2 created a new row for Pikachu with updated `size_weight` to 6.0 while keeping the historical record.


        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ## **Hard-deletes**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        The `hard_delete` column hint can be used to delete records from the destination dataset. The behavior of the delete mechanism depends on the data type of the column marked with the hint:

        * `bool` type: only `True` leads to a delete, `None` and `False` values are disregarded.
        * Other types: each not `None` value leads to a delete.

        Each record in the destination table with the same `primary_key` or `merge_key` as a record in the source dataset that's marked as a delete will be deleted.

        Deletes are propagated to any nested table that might exist. For each record that gets deleted in the root table, all corresponding records in the nested table(s) will also be deleted.

        **Example: User account deletion (GDPR Compliance)**

        - An online social platform (e.g., Instagram, Facebook) allows users to permanently delete their accounts.

        - When a user requests account deletion, their data must be removed from the production dataset to comply with GDPR or CCPA requirements.

        - By marking records with a `deleted_flag = True`, the system ensures the user’s data is completely removed from the production tables during the next load operation.
        """
    )
    return


@app.cell
def _():
    data_6 = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "deleted_flag": True,
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "deleted_flag": None,
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 6, "height": 0.4},
            "deleted_flag": False,
        },
    ]
    return (data_6,)


@app.cell
def _(TDataItems, data_6, dlt):
    @dlt.resource(
        name="pokemon",
        write_disposition="merge",
        primary_key="id",
        columns={"deleted_flag": {"hard_delete": True}},
    )
    def pokemon_2() -> TDataItems:
        yield data_6

    pipeline_4 = dlt.pipeline(
        pipeline_name="pokemon_pipeline",
        destination="duckdb",
        dataset_name="pokemon_hd",
    )
    _load_info = pipeline_4.run(pokemon_2)
    print(_load_info)
    return pipeline_4, pokemon_2


@app.cell
def _(pipeline_4):
    pipeline_4.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Bulbasaur wasn't loaded at all.

        Let's see if can remove data from loaded data:


        """
    )
    return


@app.cell
def _():
    data_7 = [
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 6, "height": 0.4},
            "deleted_flag": True,
        }
    ]
    return


@app.cell
def _(pipeline_4, pokemon_2):
    _load_info = pipeline_4.run(pokemon_2)
    print(_load_info)
    return


@app.cell
def _(pipeline_4):
    pipeline_4.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Pikachu record was deleted from loaded data.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        #### **Deduplication**

        By default, `primary_key` deduplication is arbitrary. You can pass the `dedup_sort` column hint with a value of `desc` or `asc` to influence which record remains after deduplication.

        - Using `desc`, the records sharing the same `primary_key` are sorted in **descending** order before deduplication, making sure the record with the highest value for the column with the `dedup_sort` hint remains.

        - `asc` has the opposite behavior.


        **Example: Email marketing platforms (e.g., Mailchimp, SendGrid)**

        - Users may accidentally submit the same email address multiple times during a signup process.

        - When ingesting these signups, using deduplication ensures that only unique email addresses are retained.

        - The `dedup_sort` hint allows prioritization of the latest record.

        The example data below contains three rows of information about Pikachu.
        """
    )
    return


@app.cell
def _():
    data_8 = [
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 6, "height": 0.4},
            "deleted_flag": None,
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 7, "height": 0.4},
            "deleted_flag": True,
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 8, "height": 0.4},
            "deleted_flag": None,
        },
    ]
    return (data_8,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
         This will insert one record (the one with size__weight = 8).
        """
    )
    return


@app.cell
def _(TDataItems, data_8, dlt):
    @dlt.resource(
        name="pokemon",
        write_disposition="merge",
        primary_key="id",
        columns={
            "deleted_flag": {"hard_delete": True},
            "size__weight": {"dedup_sort": "desc"},
        },
    )
    def pokemon_3() -> TDataItems:
        yield data_8

    pipeline_5 = dlt.pipeline(
        pipeline_name="pokemon_pipeline",
        destination="duckdb",
        dataset_name="pokemon_hd",
    )
    _load_info = pipeline_5.run(pokemon_3)
    print(_load_info)
    pipeline_5.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        The row with the largest value of "size__weight" 8.0 remains.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ---
        ## **Missing incremental cursor path**

        You can customize the incremental processing of dlt by setting the parameter `on_cursor_value_missing`.

        When loading incrementally with the default settings, there are two assumptions:

        * Each row contains the cursor path.
        * Each row is expected to contain a value at the cursor path that is not `None`.

        **Example: IoT device data ingestion (e.g., Smart Homes)**

        - IoT devices (e.g., thermostats, cameras) send data continuously.

        - Due to network failures or device malfunctions, some records may lack timestamps or have None as their cursor value.

        - Using `on_cursor_value_missing="include"` ensures that such data is not discarded by default, allowing for later inspection and processing.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        To process a data set where some records **do not include the incremental cursor path** or where the values at the cursor path are **None**, there are the following four options:

        * Configure the incremental load to **raise** an exception in case there is a row where the cursor path is missing or has the value `None` using
          - `incremental(..., on_cursor_value_missing="raise")`.

          - This is the **default** behavior.
        * Configure the incremental load to **tolerate** the missing cursor path and `None` values using
          - `incremental(..., on_cursor_value_missing="include")`.
        * Configure the incremental load to **exclude** the missing cursor path and `None` values using
          - `incremental(..., on_cursor_value_missing="exclude")`.

        Here is an example of including rows where the **incremental cursor value** is **missing** or **None**:
        """
    )
    return


@app.cell
def _():
    data_9 = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "created_at": 1,
            "updated_at": 1,
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "created_at": 2,
            "updated_at": 2,
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 6, "height": 0.4},
            "created_at": 3,
            "updated_at": None,
        },
    ]
    return (data_9,)


@app.cell
def _(TDataItems, data_9, dlt):
    @dlt.resource
    def pokemon_4(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", on_cursor_value_missing="include"
        )
    ) -> TDataItems:
        yield data_9

    pipeline_6 = dlt.pipeline(
        pipeline_name="pokemon_pipeline",
        destination="duckdb",
        dataset_name="pokemon_inc",
    )
    _load_info = pipeline_6.run(pokemon_4)
    print(_load_info)
    pipeline_6.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        You can also define a [fall back column](https://dlthub.com/docs/devel/general-usage/incremental-loading#transform-records-before-incremental-processing) for an incremental cursor, as described below.

        ## **Transform records before incremental processing**

        If you want to load data that includes `None` values, you can transform the records before the incremental processing.
        You can add steps to the pipeline that [filter, transform, or pivot your data](https://dlthub.com/docs/devel/general-usage/resource#filter-transform-and-pivot-data).

        In the following example
        - the step of data yielding is at `index = 0`,
        - the custom transformation at `index = 1`,
        - and the incremental processing at `index = 2`.


        > **Caution!**
        >
        >It is important to set the `insert_at` parameter of the `add_map` function to control the order of execution and ensure that your custom steps are executed before the incremental processing starts.


        See below how you can modify rows before the incremental processing using `add_map()` and filter rows using `add_filter()`.
        """
    )
    return


@app.cell
def _():
    data_10 = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "created_at": 1,
            "updated_at": 1,
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "created_at": 2,
            "updated_at": 2,
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 6, "height": 0.4},
            "created_at": 3,
            "updated_at": None,
        },
    ]
    return (data_10,)


@app.cell
def _(TDataItem, TDataItems, data_10, dlt):
    @dlt.resource
    def some_data(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at"
        ),
    ) -> TDataItems:
        yield data_10

    def set_default_updated_at(record: TDataItem) -> TDataItems:
        if record.get("updated_at") is None:
            record["updated_at"] = record.get("created_at")
        return record

    return set_default_updated_at, some_data


@app.cell
def _(set_default_updated_at, some_data):
    # Modifies records before the incremental processing
    with_default_values = some_data().add_map(set_default_updated_at, insert_at=1)
    return (with_default_values,)


@app.cell
def _(dlt, with_default_values):
    pipeline_7 = dlt.pipeline(
        pipeline_name="pokemon_pipeline_wd",
        destination="duckdb",
        dataset_name="pokemon_inc_wd",
    )
    _load_info = pipeline_7.run(with_default_values, table_name="pokemon")
    print(_load_info)
    pipeline_7.dataset().pokemon.df()
    return


@app.cell
def _(some_data):
    # Removes records before the incremental processing
    without_none = some_data().add_filter(
        lambda r: r.get("updated_at") is not None, insert_at=1
    )
    return (without_none,)


@app.cell
def _(dlt, without_none):
    pipeline_8 = dlt.pipeline(
        pipeline_name="pokemon_pipeline_wn",
        destination="duckdb",
        dataset_name="pokemon_inc_wn",
    )
    _load_info = pipeline_8.run(without_none, table_name="pokemon")
    print(_load_info)
    pipeline_8.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## **Backfilling**

        ### Using `end_value` for backfill

        You can specify both initial and end dates when defining incremental loading. Let's go back to our Pokemon example:




        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install dlt
    return


@app.cell
def _():
    data_11 = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "created_at": 1,
            "updated_at": 1,
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "created_at": 2,
            "updated_at": 2,
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 6, "height": 0.4},
            "created_at": 3,
            "updated_at": 3,
        },
    ]
    return (data_11,)


@app.cell
def _(TDataItems, data_11, dlt):
    @dlt.resource
    def some_data_1(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "created_at", initial_value=0, end_value=2
        )
    ) -> TDataItems:
        yield data_11

    return (some_data_1,)


@app.cell
def _(dlt, some_data_1):
    pipeline_9 = dlt.pipeline(
        pipeline_name="pokemon_pipeline_wd",
        destination="duckdb",
        dataset_name="pokemon_inc_wd",
    )
    _load_info = pipeline_9.run(some_data_1, table_name="pokemon")
    print(_load_info)
    pipeline_9.dataset().pokemon.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Above, we use the `initial_value` and `end_value` arguments of the `incremental` to define the range of issues that we want to retrieve
        and pass this range to the Github API (`since` and `until`). As in the examples above, `dlt` will make sure that only the issues from
        the defined range are returned.

        Please note that when `end_date` is specified, `dlt` **will not modify the existing incremental state**. The backfill is **stateless** and:
        1. You can run backfill and incremental load in parallel (i.e., in an Airflow DAG) in a single pipeline.
        2. You can partition your backfill into several smaller chunks and run them in parallel as well.

        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Note that dlt's incremental filtering considers the ranges half-closed. `initial_value` is inclusive, `end_value` is exclusive, so chaining ranges like above works without overlaps. This behaviour can be changed with the `range_start` (default `"closed"`) and `range_end` (default `"open"`) arguments.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## **Load a large dataset using incremental loading and add_limits**
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Specifically for the `sql_database` source you can utilize another possible approach - load data in fixed chunks using `chunk_size` parameter.
        """
    )
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # !pip install dlt pymysql
    return


@app.cell
def _(dlt):
    from dlt.sources.sql_database import sql_database

    source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam", chunk_size=1000
    ).with_resources("genome")
    source.genome.apply_hints(
        incremental=dlt.sources.incremental("updated", row_order="asc")
    )
    pipeline_10 = dlt.pipeline(
        pipeline_name="sql_database_pipeline",
        destination="duckdb",
        dataset_name="sql_data",
    )
    my_table_name = "genome"
    continue_load_flag = True
    while continue_load_flag:
        _load_info = pipeline_10.run(source.genome.add_limit(10))
        continue_load_flag = (
            my_table_name
            in pipeline_10.last_trace.last_normalize_info.row_counts.keys()
        )
        print(pipeline_10.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ![Lesson_6_Write_disposition_strategies_%26_Advanced_tricks_img1](https://storage.googleapis.com/dlt-blog-images/dlt-advanced-course/Lesson_6_Write_disposition_strategies_%26_Advanced_tricks_img1.png)
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ✅ ▶ Proceed to the [next lesson](https://colab.research.google.com/drive/1mC09rjkheo92-ycjjq0AlIzgwJC8-ZMX#forceEdit=true&sandboxMode=true)!
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
