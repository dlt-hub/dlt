"""
---
title: Load mysql table with ConnectorX & Arrow
description: Load data from sql queries fast with connector x and arrow tables
keywords: [connector x, pyarrow, zero copy]
---

The example script below takes genome data from public **mysql** instance and then loads it into **duckdb**. Mind that your destination
must support loading of parquet files as this is the format that `dlt` uses to save arrow tables. [Connector X](https://github.com/sfu-db/connector-x) allows to
get data from several popular databases and creates in memory Arrow table which `dlt` then saves to load package and loads to the destination.
:::tip
You can yield several tables if your data is large and you need to partition your load.
:::

We'll learn:

- How to get arrow tables from [connector X](https://github.com/sfu-db/connector-x) and yield them.
- That merge and incremental loads work with arrow tables.
- How to enable [incremental loading](../general-usage/incremental-loading) for efficient data extraction.
- How to use build in ConnectionString credentials.

"""

import connectorx as cx

import dlt
from dlt.sources.credentials import ConnectionStringCredentials


def read_sql_x(
    conn_str: ConnectionStringCredentials = dlt.secrets.value,
    query: str = dlt.config.value,
):
    yield cx.read_sql(
        conn_str.to_native_representation(),
        query,
        return_type="arrow2",
        protocol="binary",
    )


def genome_resource():
    # create genome resource with merge on `upid` primary key
    genome = dlt.resource(
        name="genome",
        write_disposition="merge",
        primary_key="upid",
        standalone=True,
    )(read_sql_x)(
        "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",  # type: ignore[arg-type]
        "SELECT * FROM genome ORDER BY created LIMIT 1000",
    )
    # add incremental on created at
    genome.apply_hints(incremental=dlt.sources.incremental("created"))
    return genome


if __name__ == "__main__":
    pipeline = dlt.pipeline(destination="duckdb")
    genome = genome_resource()

    load_info = pipeline.run(genome)
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)
    # NOTE: run pipeline again to see that no more records got loaded thanks to incremental loading

    # check that stuff was loaded
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["genome"] == 1000
