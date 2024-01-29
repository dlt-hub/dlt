---
slug: dlt-arrow-loading
title: "Get 30x speedups when reading databases with ConnectorX + Arrow + dlt"
image: /img/arrow_30x_faster.png
authors:
  name: Marcin Rudolf
  title: dltHub CTO
  url: https://github.com/rudolfix
  image_url: https://avatars.githubusercontent.com/u/17202864
tags: [arrow, Rust, ConnectorX]
---

If rust + arrow + duckb is a new data engineering stack, now you can get a feel of it with `dlt`. We recently added native arrow tables (and panda frames) loading. What it means? You can pass an Arrow table to `dlt` **pipeline.run** or **pipeline.extract** methods, have it normalized, saved to parquet and loaded to your destination.

Here we achieved ~30x speedups when loading data from (local) postgres database using ConnectorX + Arrow compared to SqlAlchemy + json. (both use dlt as an engine, read disclaimer at the end!)

### Load postgres table with Arrow

We’ll start with [ConnectorX library](https://github.com/sfu-db/connector-x) that creates Arrow tables from SQL queries on most of the popular database engines.

```python
pip install connectorx
```

Lib has Rust inside, zero copy extraction and is amazingly fast. We’ll extract and normalize 10 000 000 [test rows](https://github.com/dlt-hub/verified-sources/blob/master/tests/sql_database/sql_source.py#L88) from local postgresql. The table **chat_message** looks like Slack messages dump.  Messages have unique autoincrement **id** which we use to load in chunks:

```python
import connectorx as cx
import dlt
from dlt.sources.credentials import ConnectionStringCredentials

def read_sql_x(
    conn_str: str
):
    # load in chunks by one million
    for _id in range(1, 10_000_001, 1_000_000):
        table = cx.read_sql(conn_str,
                          "SELECT * FROM arrow_test_2.chat_message WHERE id BETWEEN %i AND %i" % (_id, _id + 1000000 - 1),
                          return_type="arrow2",
                          protocol="binary"
                          )
        yield table

chat_messages = dlt.resource(
    read_sql_x,
    name="chat_messages"
)("postgresql://loader:loader@localhost:5432/dlt_data")
```

In this demo I just extract and normalize data and skip the loading step.

```python
pipeline = dlt.pipeline(destination="duckdb", full_refresh=True)
# extract first
pipeline.extract(chat_messages)
info = pipeline.normalize()
# print count of items normalized
print(info)
# print the execution trace
print(pipeline.last_trace)
```

Let’s run it:

```sh
$ PROGRESS=enlighten python connector_x_speed.py
Items 10000001 [00:00, 241940483.70/s]
Normalized data for the following tables:
- _dlt_pipeline_state: 1 row(s)
- chat_messages: 10000000 row(s)

Run started at 2023-10-23T19:06:55.527176+00:00 and COMPLETED in 16.17 seconds with 2 steps.
Step extract COMPLETED in 16.09 seconds.

Step normalize COMPLETED in 0.08 seconds.
```
### Load postgres table with SqlAlchemy

Here’s corresponding code working with **SqlAlchemy**. We process 10 000 000 rows, yielding in 100k rows packs and normalize to parquet in 3 parallel processes.

```python
from itertools import islice
import dlt
from sqlalchemy import create_engine

CHUNK_SIZE=100000

def read_sql_a(conn_str: str):
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        rows = conn.execution_options(yield_per=CHUNK_SIZE).exec_driver_sql("SELECT * FROM arrow_test_2.chat_message")
        while rows_slice := list(islice(map(lambda row: dict(row._mapping), rows), CHUNK_SIZE)):
            yield rows_slice

chat_messages = dlt.resource(
    read_sql_a,
    name="chat_messages",
    write_disposition="append",
)("postgresql://loader:loader@localhost:5432/dlt_data")

pipeline = dlt.pipeline(destination="duckdb", full_refresh=True)
# extract first
pipeline.extract(chat_messages)
info = pipeline.normalize(workers=3, loader_file_format="parquet")
print(info)
print(pipeline.last_trace)
```

Let’s run it:

```sh
$ PROGRESS=enlighten python sql_alchemy_speed.py
Normalized data for the following tables:
- _dlt_pipeline_state: 1 row(s)
- chat_messages: 10000000 row(s)

Run started at 2023-10-23T19:13:55.898598+00:00 and COMPLETED in 8 minutes and 12.97 seconds with 2 steps.
Step extract COMPLETED in 3 minutes and 32.75 seconds.

Step normalize COMPLETED in 3 minutes and 40.22 seconds.
Normalized data for the following tables:
- _dlt_pipeline_state: 1 row(s)
- chat_messages: 10000000 row(s)
```

### Results

So we can see **~30x overall speedup on extract and normalize steps** (~16 seconds vs ~8 minutes). The **extract step is ~13x faster**, while **normalize is few thousand times faster**. Arrow normalizer is just checking the schemas and moves parquet files around. JSON normalizer is inspecting every row to first infer the schema and then to validate the data.

As the output in both of methods is the same (parquet files) - the actual load step takes the same time in both cases and is not compared. I could easily push the load packages (parquet files) to [any of supported destinations](https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas#destinations-that-support-parquet-for-direct-loading)

### What’s next:
- [Reads our docs on Arrow](https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas)
- [Add merge and incremental loads to code above](https://dlthub.com/docs/examples/connector_x_arrow/)
- I'm on [dltHub Slack](https://dlthub.com/community) all the time.

### Disclaimers

- Playing field is not level. classical (sql alchemy) `dlt` run is processing data row by row, inferring and validating schema. that’s why it so slow. The Arrow version benefits from the fact, that data is already structured in the source.
- We load from local database. That means that network roundtrip during extraction is not included. That isolates Arrow speedups well. In case of remote database engine, the speedups will be smaller.
- You could optimize extract (both classical and arrow) by reading data from **postgres** [in parallel](https://dlthub.com/docs/examples/transformers/#using-transformers-with-the-pokemon-api) or use partitions in ConnectorX