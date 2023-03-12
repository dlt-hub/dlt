---
sidebar_position: 13
---

# Telemetry and Tracing
## Anonymous Telemetry
`dlt` collects and reports anonymous usage information. This information is essential to figure out which sources and destinations are used and how we should improve the library. Telemetry does not send any personal data. We do not create tracking cookies nor identify users, even anonymously. You can also disable telemetry at any moment or send it to your own servers.

### How to disable
You can disable the telemetry by adding `--disable-telemetry` to any dlt [command](command-line-interface.md). The telemetry will be disabled before the command is executed. That let's you opt out before any telemetry data is sent. For example
```shell
$ dlt --disable-telemetry init chess duckdb
```
will disable telemetry before the `chess` pipeline is initialized. To disable telemetry without executing any command just type
```shell
$ dlt --disable-telemetry
```
The telemetry will be disabled both in current project and globally - for the whole machine. Check the current telemetry status with
```shell
dlt telemetry
```
The other way to disable telemetry is to set the `runtime.dlthub_telemetry` option in `config.toml` file in `.dlt` folder.
```toml
[runtime]
dlthub_telemetry=false
```

### When and what we send
Anonymous telemetry is sent when:
* Any `dlt` command is executed from the command line. The data contains the command name. In case of `dlt init` command we also send the requested destination and the data source names.
* When the `pipeline.run` is called. We send information when [extract, normalize and load](architecture.md) steps are completed. The data contains the destination name (ie. `duckdb`), elapsed time and flag is step succeeded or not.

Example `dlt init` telemetry message
```json
{
  "anonymousId": "933dd165453d196a58adaf49444e9b4c",
  "context": {
    "ci_run": false,
    "cpu": 8,
    "exec_info": [],
    "library": {
      "name": "python-dlt",
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
  },
}
```

Example for `load` pipeline run step.
```json
{
  "anonymousId": "570816b273a41d16caacc26a797204d9",
  "context": {
    "ci_run": false,
    "cpu": 3,
    "exec_info": [],
    "library": {
      "name": "python-dlt",
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
    "elapsed": 2.234885,
    "event_category": "pipeline",
    "event_name": "load",
    "success": true,
    "transaction_id": "39c3b69c858836c36b9b7c6e046eb391"
  },
}
```
### The message `context`
The message context contains following information
* the `anonymousId` is a random number different for each message. It is required by Segment.
* `ci_run` a flag indicating if the message was sent from a CI environment ie. from Github Actions or Travis
* `cpu` contains number of cores
* `exec_info` contains a list of strings that identify execution environment: ie. `kubernetes`, `docker`, `airflow` etc.
* the `library`, `os` and `python` give us some understanding of the runtime environment of the `dlt`

### Sending telemetry data to your own Segment instance
You can send the anonymous telemetry information to your own [Segment](https://segment.com/) account. You need to create a HTTP Server source and generate a WRITE KEY which then you pass to the `config.toml` as follows
```toml
[runtime]
dlthub_telemetry_segment_write_key="<write_key>"
```

## Pipeline Tracing
`dlt` users can configure [Sentry](https://sentry.io) DSN to start receiving a rich information on executed pipelines and encountered errors and exceptions. **Sentry tracing is disabled by default**

### When and what we send
An exception trace is sent when:
* Any Python logger (including `dlt`) logs an error
* Any Python logger (including `dlt`) logs a warning (enabled only if the `dlt` logging level is `WARNING` or below)
* On unhandled exceptions.

A transaction trace is sent when the `pipeline.run` is called. We send information when [extract, normalize and load](architecture.md) steps are completed.

The data available in Sentry is very rich will makes finding and documenting bugs easy.
The tracing information allows to easily find bottlenecks and profile data extraction, normalization and loading.

`dlt` adds a set of additional tags ie. pipeline name, destination name etc. to the sentry data.

Please refer to the Sentry [documentation](https://docs.sentry.io/platforms/python/data-collected/)

### Enable pipeline tracing
To enable Sentry, you should configure the [DSN](https://docs.sentry.io/product/sentry-basics/dsn-explainer/) in the `config.toml`
```toml
[runtime]
sentry_dsn="https:///<...>"
```
or using the environment variables
```shell
RUNTIME__SENTRY_DSN="https:///<...>"
```
Sentry client is configured after the first pipeline is created with `dlt.pipeline()`. Feel free to use `sentry_sdk` init again to cover your special needs.

## Disable all tracing
`dlt` allows you to completely disable pipeline tracing, including the anonymous telemetry and Sentry. Using `config.toml`:
```toml
enable_runtime_trace=false
```