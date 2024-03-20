---
title: Run chess pipeline in production
description: Learn how run chess pipeline in production
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to investigate, track, retry and test your loads."
    slug="chess_production"
    run_file="chess"
    destination="duckdb" />

## Run chess pipeline in production

In this example, you'll find a Python script that interacts with the Chess API to extract players and game data.

We'll learn how to:

- Inspecting packages after they have been loaded.
- Loading back load information, schema updates, and traces.
- Triggering notifications in case of schema evolution.
- Using context managers to independently retry pipeline stages.
- Run basic tests utilizing `sql_client` and `normalize_info`.

### Init chess source

<!--@@@DLT_SNIPPET ./code/chess-snippets.py::markdown_source-->



### Using context managers to retry pipeline stages separately

<!--@@@DLT_SNIPPET ./code/chess-snippets.py::markdown_retry_cm-->


:::warning
To run this example you need to provide Slack incoming hook in `.dlt/secrets.toml`:
```py
[runtime]
slack_incoming_hook="https://hooks.slack.com/services/***"
```
Read [Using Slack to send messages.](https://dlthub.com/docs/running-in-production/running#using-slack-to-send-messages)
:::

### Run the pipeline

<!--@@@DLT_SNIPPET ./code/chess-snippets.py::markdown_pipeline-->

