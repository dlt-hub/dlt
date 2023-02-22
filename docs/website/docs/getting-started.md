---
sidebar_position: 3
---

# Getting started

Follow the steps below to have a working `dlt` [pipeline](./glossary#pipeline) in 5 minutes.

Please make sure you have [installed `dlt`](./installation.mdx) before getting started here.

## 1. Initialize project

Create a `dlt` project with a pipeline that loads data from the chess.com API to DuckDB by running:

```
dlt init chess duckdb
```

Install the dependencies necessary for DuckDB:
```
pip install -r requirements.txt
```

## 2. Run pipeline

Run the pipeline to load data from the chess.com API to DuckDB by running:
```
python3 chess.py
```

## 3. Query DuckDB database

Create a `query.py` file where you can write SQL queries:
```
import duckdb

conn = duckdb.connect('quack.duckdb')
conn.sql('SELECT COUNT(*) FROM games').show()
```

Run `query.py` to run the script and execute the SQL query to see the number of games:
```
python3 query.py
```

Learn more about how to query DuckDB [here](https://duckdb.org/docs/sql/introduction#querying-a-table).

Read more about the default configuration of the DuckDB destination [here](destinations.md#destination-configuration).

## 4. Next steps

Now that you have a working pipeline, you have options for what to learn next:
- Try loading data to a [different destination](./destinations) like Google BigQuery, Amazon Redshift, or Postgres
- [Deploy this pipeline](./walkthroughs/deploy-a-pipeline), so that the data is automatically
loaded on a schedule
- [Create a pipeline](./walkthroughs/create-a-pipeline) for an API that has data you want to load and use
- Transform the [loaded data](./using-loaded-data/transforming-the-data) with dbt or in Pandas DataFrames
- Set up a [pipeline in production](./running-in-production/scheduling) with scheduling,
monitoring, and alerting