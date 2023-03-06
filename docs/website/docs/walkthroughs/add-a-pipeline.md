---
sidebar_position: 2
---

# Add a pipeline

Follow the steps below to add a [pipeline](../glossary.md#pipeline) pipeline contributed by the other `dlt` users.

Please make sure you have [installed `dlt`](../installation.mdx) before following the steps below.

## 1. Initialize project

Create a new empty directory for your `dlt` project by running
```shell
mkdir various_pipelines
cd various_pipelines
```

List available pipelines to see their names and descriptions
```
dlt init --list-pipelines
```

Now pick one of the pipeline names, for example `pipedrive` and a destination ie. `bigquery`.
```
dlt init pipedrive bigquery
```

The command

## 2. Add Google BigQuery credentials

## 3. Add Google pipedrive credentials

## 4. Customize or write a pipeline script
Script with example pipeline usage is in `pipedrive_pipeline.py`. Use it to cut and paste code or as a starting point to your own

move to a separate walkthrough on customization

## 5. Hack a pipeline

## 6. Add more pipelines to your project
```
dlt init chess duckdb
```

## 7. Update the pipeline with the newest version
Just do
```
dlt init pipedrive bigquery
```
again to get the newest file. If pipeline was hacked -> move to a separate walkthrough