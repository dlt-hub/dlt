---
title: Telemetry
description: Anonymous usage information with dlt telemetry
keywords: [telemetry, usage information, opt out]
---

# Telemetry

`dlt` collects and reports **anonymous** usage information. This information is essential to figuring out how we should improve the library. Telemetry does not send any personal data. We create a random tracking cookie that is stored in your `~/.dlt` directory. You can disable telemetry at any moment or send it to your own servers instead.

## How to opt out

You can disable telemetry by adding `--disable-telemetry` to any dlt [command](command-line-interface.md).

This command will disable telemetry both in the current project and globally for the whole machine:

```sh
dlt --disable-telemetry
```

While this command will also permanently disable telemetry and then initialize the `chess` pipeline:

```sh
dlt --disable-telemetry init chess duckdb
```

You can check the current telemetry status with this command:

```sh
dlt telemetry
```

Another way to disable telemetry is to set the `runtime.dlthub_telemetry` option in the `config.toml` file in the `.dlt` folder.

```toml
[runtime]

dlthub_telemetry=false
```

## What we send when

Anonymous telemetry is sent when:

- Any `dlt` command is executed from the command line. The data contains the command name. In the case of the `dlt init` command, we also send the requested destination and data source names.
- When `pipeline.run` is called, we send information when the [extract, normalize, and load](explainers/how-dlt-works.md) steps are completed. The data contains the destination name (e.g., `duckdb`), hashes of the dataset name, pipeline name, default schema name, destination fingerprint (which is a hash of selected destination configuration fields), elapsed time, and whether the step succeeded or not.
- When `dbt` and `airflow` helpers are used

Here is an example `dlt init` telemetry message:

```json
{
  "anonymousId": "933dd165453d196a58adaf49444e9b4c",
  "context": {
    "ci_run": false,
    "cpu": 8,
    "exec_info": [],
    "library": {
      "name": "dlt",
      "version": "0.2.0a25"
    },
    "os": {
      "name": "Linux",
      "version": "4.19.128-microsoft-standard"
    },
    "python": "3.8.11"
  },
  "event": "command_init",
  "properties": {
    "destination_name": "bigquery",
    "elapsed": 3.1720383167266846,
    "event_category": "command",
    "event_name": "init",
    "pipeline_name": "pipedrive",
    "success": true
  }
}
```

Example for `load` pipeline run step:

```json
{
  "anonymousId": "570816b273a41d16caacc26a797204d9",
  "context": {
    "ci_run": false,
    "cpu": 3,
    "exec_info": [],
    "library": {
      "name": "dlt",
      "version": "0.2.0a26"
    },
    "os": {
      "name": "Darwin",
      "version": "21.6.0"
    },
    "python": "3.10.10"
  },
  "event": "pipeline_load",
  "properties": {
    "destination_name": "duckdb",
    "destination_fingerprint": "",
    "pipeline_name_hash": "OpVShb3cX7qQAmOZSbV8",
    "dataset_name_hash": "Hqk0a3Ov5AD55KjSg2rC",
    "default_schema_name_hash": "Hqk0a3Ov5AD55KjSg2rC",
    "elapsed": 2.234885,
    "event_category": "pipeline",
    "event_name": "load",
    "success": true,
    "transaction_id": "39c3b69c858836c36b9b7c6e046eb391"
  }
}
```

## The message `context`

The message `context` contains the following information:

- `anonymousId`: a random tracking cookie stored in `~/.dlt/.anonymous_id`.
- `ci_run`: a flag indicating if the message was sent from a CI environment (e.g., `GitHub Actions`, `Travis CI`).
- `cpu`: contains the number of cores.
- `exec_info`: contains a list of strings that identify the execution environment: (e.g., `kubernetes`, `docker`, `airflow`).
- The `library`, `os`, and `python` give us some understanding of the runtime environment of the `dlt`.

## Send telemetry data to your own tracker

You can set up your own tracker to receive telemetry events. You can create a scalable, globally distributed edge service [using `dlt` and Cloudflare](https://dlthub.com/blog/dlt-segment-migration).

Once your tracker is running, point `dlt` to it. You can use the global `config.toml` to redirect all pipelines on a given machine.

```toml
[runtime]
dlthub_telemetry_endpoint="telemetry-tracker.services4745.workers.dev"
```

### Track events with Segment

You can send anonymous telemetry to your own [Segment](https://segment.com/) account. You need to create an HTTP Server source and generate a WRITE KEY, which you then pass to the `config.toml` like this:

```toml
[runtime]
dlthub_telemetry_endpoint="https://api.segment.io/v1/track"
dlthub_telemetry_segment_write_key="<write_key>"
```

