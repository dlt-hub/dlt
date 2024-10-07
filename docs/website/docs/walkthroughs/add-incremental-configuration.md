---
title: Add incremental configuration to SQL resources
description: Incremental SQL data loading strategies
keywords: [how to, load data incrementally from SQL]
slug: sql-incremental-configuration
---

# Add incremental configuration to SQL resources
Incremental loading is the act of loading only new or changed data and not old records that have already been loaded.
For example, a bank loads only the latest transactions, or a company updates its database with new or modified user
information. In this article, we’ll discuss a few incremental loading strategies.

:::important
Processing data incrementally, or in batches, enhances efficiency, reduces costs, lowers latency, improves scalability,
and optimizes resource utilization.
:::

### Incremental loading strategies

In this guide, we will discuss various incremental loading methods using `dlt`, specifically:

| S.No. | Strategy | Description |
| --- | --- | --- |
| 1. | Full load (replace) | It completely overwrites the existing data with the new/updated dataset. |
| 2. | Append new records based on Incremental ID | Appends only new records to the table based on an incremental ID.  |
| 3. | Append new records based on date ("created_at") | Appends only new records to the table based on a date field.  |
| 4. | Merge (Update/Insert) records based on timestamp ("last_modified_at") and ID | Merges records based on a composite ID key and a timestamp field. Updates existing records and inserts new ones as necessary. |

## Code examples



### 1. Full load (replace)

A full load strategy completely overwrites the existing data with the new dataset. This is useful when you want to refresh the entire table with the latest data.

:::note
This strategy technically does not load only new data but instead reloads all data: old and new.
:::

Here’s a walkthrough:

1. The initial table, named "contact," in the SQL source looks like this:

    | id | name | created_at |
    | --- | --- | --- |
    | 1 | Alice | 2024-07-01 |
    | 2 | Bob | 2024-07-02 |

2. The Python code illustrates the process of loading data from an SQL source into BigQuery using the `dlt` pipeline. Please note the `write_disposition = "replace"` used below.

    ```py
    def load_full_table_resource() -> None:
        """Load a full table, replacing existing data."""
        pipeline = dlt.pipeline(
            pipeline_name="mysql_database",
            destination='bigquery',
            dataset_name="dlt_contacts"
        )

        # Load the full table "contact"
        source = sql_database().with_resources("contact")

        # Run the pipeline
        info = pipeline.run(source, write_disposition="replace")

        # Print the info
        print(info)

    load_full_table_resource()
    ```

3. After running the `dlt` pipeline, the data loaded into the BigQuery "contact" table looks like:

    | Row | id | name | created_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 1 | Alice | 2024-07-01 | 1721878309.021546 | tgyMM73iMz0cQg |
    | 2 | 2 | Bob | 2024-07-02 | 1721878309.021546 | 88P0bD796pXo/Q |

4. Next, the "contact" table in the SQL source is updated—two new rows are added, and the row with `id = 2` is removed. The updated data source ("contact" table) now presents itself as follows:

    | id | name | created_at |
    | --- | --- | --- |
    | 1 | Alice | 2024-07-01 |
    | 3 | Charlie | 2024-07-03 |
    | 4 | Dave | 2024-07-04 |

5. The "contact" table created in BigQuery after running the pipeline again:

    | Row | id | name | created_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 1 | Alice | 2024-07-01 | 1721878309.021546 | S5ye6fMhYECZA |
    | 2 | 3 | Charlie | 2024-07-03 | 1721878309.021546 | eT0zheRx9ONWuQ |
    | 3 | 4 | Dave | 2024-07-04 | 1721878309.021546 | gtflF8BdL2NO/Q |

**What happened?**

After running the pipeline, the original data in the "contact" table (Alice and Bob) is completely replaced with the new updated table with data “Charlie” and “Dave” added and “Bob” removed. This strategy is useful for scenarios where the entire dataset needs to be refreshed or replaced with the latest information.

### 2. Append new records based on incremental ID

This strategy appends only new records to the table based on an incremental ID. It is useful for scenarios where each new record has a unique, incrementing identifier.

Here’s a walkthrough:

1. The initial table, named "contact," in the SQL source looks like this:

    | id | name | created_at |
    | --- | --- | --- |
    | 1 | Alice | 2024-07-01 |
    | 2 | Bob | 2024-07-02 |

2. The Python code demonstrates loading data from an SQL source into BigQuery using an incremental variable, `id`. This variable tracks new or updated records in the `dlt` pipeline. Please note the `write_disposition = "append"` used below.

    ```py
    def load_incremental_id_table_resource() -> None:
        """Load a table incrementally based on an ID."""
        pipeline = dlt.pipeline(
            pipeline_name="mysql_database",
            destination='bigquery',
            dataset_name="dlt_contacts",
        )

        # Load table "contact" incrementally based on ID
        source = sql_database().with_resources("contact")
        source.contact.apply_hints(incremental=dlt.sources.incremental("id"))

        # Run the pipeline with append write disposition
        info = pipeline.run(source, write_disposition="append")

        # Print the info
        print(info)
    ```

3. After running the `dlt` pipeline, the data loaded into the BigQuery "contact" table looks like:

    | Row | id | name | created_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 1 | Alice | 2024-07-01 | 1721878309.021546 | YQfmAu8xysqWmA |
    | 2 | 2 | Bob | 2024-07-02 | 1721878309.021546 | Vcb5KKah/RpmQw |

4. Next, the "contact" table in the SQL source is updated—two new rows are added, and the row with `id = 2` is removed. The updated data source now presents itself as follows:

    | id | name | created_at |
    | --- | --- | --- |
    | 1 | Alice | 2024-07-01 |
    | 3 | Charlie | 2024-07-03 |
    | 4 | Dave | 2024-07-04 |

5. The "contact" table created in BigQuery after running the pipeline again:

    | Row | id | name | created_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 1 | Alice | 2024-07-01 | 1721878309.021546 | OW9ZyAzkXg4D4w |
    | 2 | 2 | Bob | 2024-07-02 | 1721878309.021546 | skVYZ/ppQuztUg |
    | 3 | 3 | Charlie | 2024-07-03 | 1721878309.021546 | y+T4Q2JDnR33jg |
    | 4 | 4 | Dave | 2024-07-04 | 1721878309.021546 | MAXrGhNNADXAiQ |

**What happened?**

In this scenario, the pipeline appends new records (Charlie and Dave) to the existing data (Alice and Bob) without affecting the pre-existing entries. This strategy is ideal when only new data needs to be added, preserving the historical data.

### Append new records based on timestamp ("created_at")

This strategy appends only new records to the table based on a date/timestamp field. It is useful for scenarios where records are created with a timestamp, and you want to load only those records created after a certain date.

Here’s a walkthrough:

1. The initial dataset, named "contact," in the SQL source looks like this:

    | id | name | created_at |
    | --- | --- | --- |
    | 1 | Alice | 2024-07-01 00:00:00 |
    | 2 | Bob | 2024-07-02 00:00:00 |

2. The Python code illustrates the process of loading data from an SQL source into BigQuery using the `dlt` pipeline. Please note the `write_disposition = "append"`, with `created_at` being used as the incremental parameter.

    ```py
    def load_incremental_timestamp_table_resource() -> None:
        """Load a table incrementally based on created_at timestamp."""
        pipeline = dlt.pipeline(
            pipeline_name="mysql_databasecdc",
            destination='bigquery',
            dataset_name="dlt_contacts",
        )

        # Load table "contact", incrementally starting at a given timestamp
        source = sql_database().with_resources("contact")
        source.contact.apply_hints(incremental=dlt.sources.incremental(
            "created_at", initial_value=datetime(2024, 4, 1, 0, 0, 0)))

        # Run the pipeline
        info = pipeline.run(source, write_disposition="append")

        # Print the info
        print(info)

    load_incremental_timestamp_table_resource()
    ```

3. After running the `dlt` pipeline, the data loaded into the BigQuery "contact" table looks like:

    | Row | id | name | created_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 1 | Alice | 2024-07-01 00:00:00 UTC | 1721878309.021546 | 5H8ca6C89umxHA |
    | 2 | 2 | Bob | 2024-07-02 00:00:00 UTC | 1721878309.021546 | M61j4aOSqs4k2w |

4. Next, the "contact" table in the SQL source is updated—two new rows are added, and the row with `id = 2` is removed. The updated data source now presents itself as follows:

    | id | name | created_at |
    | --- | --- | --- |
    | 1 | Alice | 2024-07-01 00:00:00 |
    | 3 | Charlie | 2024-07-03 00:00:00 |
    | 4 | Dave | 2024-07-04 00:00:00 |

5. The "contact" table created in BigQuery after running the pipeline again:

    | Row | id | name | created_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 1 | Alice | 2024-07-01 00:00:00 UTC | 1721878309.021546 | Petj6R+B/63sWA |
    | 2 | 2 | Bob | 2024-07-02 00:00:00 UTC | 1721878309.021546 | 3Rr3VmY+av+Amw |
    | 3 | 3 | Charlie | 2024-07-03 00:00:00 UTC | 1721878309.021546 | L/MnhG19xeMrvQ |
    | 4 | 4 | Dave | 2024-07-04 00:00:00 UTC | 1721878309.021546 | W6ZdfvTzfRXlsA |

**What happened?**

The pipeline adds new records (Charlie and Dave) that have a `created_at` timestamp after the specified initial value while retaining the existing data (Alice and Bob). This approach is useful for loading data incrementally based on when it was created.

### 4. Merge (update/insert) records based on timestamp ("last_modified_at") and ID

This strategy merges records based on a composite key of ID and a timestamp field. It updates existing records and inserts new ones as necessary.

Here’s a walkthrough:

1. The initial dataset, named ‘contact’, in the SQL source looks like this:

    | id | name | last_modified_at |
    | --- | --- | --- |
    | 1 | Alice | 2024-07-01 00:00:00 |
    | 2 | Bob | 2024-07-02 00:00:00 |

2. The Python code illustrates the process of loading data from an SQL source into BigQuery using the `dlt` pipeline. Please note the `write_disposition = "merge"`, with `last_modified_at` being used as the incremental parameter.

    ```py
    def load_merge_table_resource() -> None:
        """Merge (update/insert) records based on last_modified_at timestamp and ID."""
        pipeline = dlt.pipeline(
            pipeline_name="mysql_database",
            destination='bigquery',
            dataset_name="dlt_contacts",
        )

        # Merge records, 'contact' table, based on ID and last_modified_at timestamp
        source = sql_database().with_resources("contact")
        source.contact.apply_hints(incremental=dlt.sources.incremental(
            "last_modified_at", initial_value=datetime(2024, 4, 1, 0, 0, 0)),
            primary_key="id")

        # Run the pipeline
        info = pipeline.run(source, write_disposition="merge")

        # Print the info
        print(info)

    load_merge_table_resource()
    ```

3. After running the `dlt` pipeline, the data loaded into BigQuery ‘contact’ table looks like:

    | Row | id | name | last_modified_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 1 | Alice | 2024-07-01 00:00:00 UTC | 1721878309.021546 | ObbVlxcly3VknQ |
    | 2 | 2 | Bob | 2024-07-02 00:00:00 UTC | 1721878309.021546 | Vrlkus/haaKlEg |

4. Next, the "contact" table in the SQL source is updated— “Alice” is updated to “Alice Updated”, and a new row “Hank” is added:

    | id | name | last_modified_at |
    | --- | --- | --- |
    | 1 | Alice Updated | 2024-07-08 00:00:00 |
    | 3 | Hank | 2024-07-08 00:00:00 |

5. The "contact" table created in BigQuery after running the pipeline again:

    | Row | id | name | last_modified_at | _dlt_load_id | _dlt_id |
    | --- | --- | --- | --- | --- | --- |
    | 1 | 2 | Bob | 2024-07-02 00:00:00 UTC | 1721878309.021546 | Cm+AcDZLqXSDHQ |
    | 2 | 1 | Alice Updated | 2024-07-08 00:00:00 UTC | 1721878309.021546 | OeMLIPw7rwFG7g |
    | 3 | 3 | Hank | 2024-07-08 00:00:00 UTC | 1721878309.021546 | Ttp6AI2JxqffpA |

**What happened?**

The pipeline updates the record for Alice with the new data, including the updated `last_modified_at` timestamp, and adds a new record for Hank. This method is beneficial when you need to ensure that records are both updated and inserted based on a specific timestamp and ID.

The examples provided explain how to use `dlt` to achieve different incremental loading scenarios, highlighting the changes before and after running each pipeline.

