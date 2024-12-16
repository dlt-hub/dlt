"""
---
title: Backfilling in chunks
description: Learn how to backfill in chunks of defined size
keywords: [incremental loading, backfilling, chunks,example]
---

In this example, you'll find a Python script that will load from a sql_database source in chunks of defined size. This is useful for backfilling in multiple pipeline runs as
opposed to backfilling in one very large pipeline run which may fail due to memory issues on ephemeral storage or just take a very long time to complete without seeing any
progress in the destination.

We'll learn how to:

- Connect to a mysql database with the sql_database source
- Select one table to load and apply incremental loading hints as well as the primary key
- Set the chunk size and limit the number of chunks to load in one pipeline run
- Create a pipeline and backfill the table in the defined chunks
- Use the datasets accessor to inspect and assert the load progress

"""

import pandas as pd

import dlt
from dlt.sources.sql_database import sql_database


if __name__ == "__main__":
    # NOTE: this is a live table in the rfam database, so the number of final rows may change
    TOTAL_TABLE_ROWS = 4178
    RFAM_CONNECTION_STRING = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"

    # create sql database source that only loads the family table in chunks of 1000 rows
    source = sql_database(RFAM_CONNECTION_STRING, table_names=["family"], chunk_size=1000)

    # we apply some hints to the table, we know the rfam_id is unique and that we can order
    # and load incrementally on the created datetime column
    source.family.apply_hints(
        primary_key="rfam_id",
        incremental=dlt.sources.incremental(
            cursor_path="created", initial_value=None, row_order="asc"
        ),
    )

    # with limit we can limit the number of chunks to load, with a chunk size of 1000 and a limit of 1
    # we will load 1000 rows per pipeline run
    source.add_limit(1)

    # create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rfam", destination="duckdb", dataset_name="rfam_data", dev_mode=True
    )

    def _assert_unique_row_count(df: pd.DataFrame, num_rows: int) -> None:
        """Assert that a dataframe has the correct number of unique rows"""
        # NOTE: this check is dependent on reading the full table back from the destination into memory,
        # so it is only useful for testing before you do a large backfill.
        assert len(df) == num_rows
        assert len(set(df.rfam_id.tolist())) == num_rows

    # after the first run, the family table in the destination should contain the first 1000 rows
    pipeline.run(source)
    _assert_unique_row_count(pipeline.dataset().family.df(), 1000)

    # after the second run, the family table in the destination should contain 1999 rows
    # there is some overlap on the incremental to prevent skipping rows
    pipeline.run(source)
    _assert_unique_row_count(pipeline.dataset().family.df(), 1999)

    # ...
    pipeline.run(source)
    _assert_unique_row_count(pipeline.dataset().family.df(), 2998)

    # ...
    pipeline.run(source)
    _assert_unique_row_count(pipeline.dataset().family.df(), 3997)

    # the final run will load all the rows until the end of the table
    pipeline.run(source)
    _assert_unique_row_count(pipeline.dataset().family.df(), TOTAL_TABLE_ROWS)

    # NOTE: in a production environment you will likely:
    # * be using much larger chunk sizes and limits
    # * run the pipeline in a loop to load all the rows
    # * and programmatically check if the table is fully loaded and abort the loop if this is the case.
