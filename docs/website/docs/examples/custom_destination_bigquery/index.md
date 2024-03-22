---
title: Custom destination with BigQuery
description: Learn how use the custom destination to load to bigquery and use credentials
keywords: [destination, credentials, example, bigquery, custom destination]
---

import Header from '../_examples-header.md';

<Header
    intro="This example demonstrates how to use the custom destination to load to BigQuery with automatic schema inference."
    slug="custom_destination_bigquery"
    run_file="custom_destination_bigquery"
    destination="biqquery"/>

## Custom destination BigQuery pipeline

In this example, you'll find a Python script that demonstrates how to load Google Sheets data using the `dlt` library.

We'll learn how to:
- use [built-in credentials](../../general-usage/credentials/config_specs#gcp-credentials)
- use the [custom destination](../../dlt-ecosystem/destinations/destination.md)
- Use pyarrow tables to create complex column types on bigquery
- Use bigquery autodetect=True for schema inference from parquet files

### Your bigquery credentials in secrets.toml
<!--@@@DLT_SNIPPET code/.dlt/example.secrets.toml::example-->

### Pipeline code

<!--@@@DLT_SNIPPET code/custom_destination_bigquery-snippets.py::example-->
