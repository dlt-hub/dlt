---
title: Google Sheets minimal example
description: Learn how work with Google services
keywords: [google sheets, credentials, example]
---

import Header from '../_examples-header.md';

<Header
    intro="This example demonstrates how to load Google Sheets data using Python and the dlt library. It covers working with Google API, using built in credentials, using union of credentials, and creating dynamically generated resources."
    slug="google_sheets"
    run_file="google_sheets"
    destination="duckdb"/>

## Google Sheets data pipeline

In this example, you'll find a Python script that demonstrates how to load Google Sheets data using the `dlt` library.

We'll learn how to:
- use [built-in credentials](../../general-usage/credentials/config_specs#gcp-credentials);
- use [union of credentials](../../general-usage/credentials/config_specs#working-with-alternatives-of-credentials-union-types);
- create [dynamically generated resources](../../general-usage/source#create-resources-dynamically).

:::tip
This example is for educational purposes. For best practices, we recommend using [Google Sheets verified source](../../dlt-ecosystem/verified-sources/google_sheets.md).
:::

### Install Google client library

```sh
 pip install google-api-python-client
```

### Loading code

<!--@@@DLT_SNIPPET code/google_sheets-snippets.py::google_sheets-->


### Run the pipeline

<!--@@@DLT_SNIPPET code/google_sheets-snippets.py::google_sheets_run-->

