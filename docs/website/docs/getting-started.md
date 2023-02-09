---
sidebar_position: 3
---

# Getting started

Follow the steps below to have a working `dlt` [pipeline](./glossary#pipeline) in 5 minutes.

Please make sure you have [installed `dlt`](./installation.mdx) before getting started here.

## 1. Initialize project

Create a `dlt` project with a pipeline that loads data from the chess.com API to Google BigQuery by running:

```
dlt init chess bigquery
```

Install the dependencies necessary for Google BigQuery:
```
pip install -r requirements.txt
```

## 2. Set up Google BigQuery

Follow [BigQuery setup](./destinations#google-bigquery) to create the service account credentials you'll need to add to `.dlt/secrets.toml`.

## 3. Run pipeline

Run the pipeline to load data from the chess.com API to Google BigQuery by running:
```
python3 chess.py
```

Go to the [Google BigQuery](https://console.cloud.google.com/bigquery) console and view the tables
that have been loaded.

## 4. Next steps

Now that you have a working pipeline, you have options for what to learn next:
- Try loading data to a [different destination](./destinations) like Amazon Redshift, Postgres or DuckDb
- [Deploy this pipeline](./walkthroughs/deploy-a-pipeline), so that the data is automatically
loaded on a schedule
- [Create a pipeline](./walkthroughs/create-a-pipeline) for an API that has data you want to load and use
- Transform the [loaded data](./using-loaded-data/transforming-the-data) with dbt or in Pandas DataFrames
- Set up a [pipeline in production](./running-in-production/scheduling) with scheduling,
monitoring, and alerting