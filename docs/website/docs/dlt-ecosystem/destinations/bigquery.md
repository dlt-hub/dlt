---
title: Google BigQuery
description: Google BigQuery `dlt` destination
keywords: [bigquery, destination, data warehouse]
---

# Google BigQuery

## Install dlt with BigQuery

**To install the dlt library with BigQuery dependencies:**

```sh
pip install "dlt[bigquery]"
```

## Setup guide

**1. Initialize a project with a pipeline that loads to BigQuery by running:**

```sh
dlt init chess bigquery
```

**2. Install the necessary dependencies for BigQuery by running:**

```sh
pip install -r requirements.txt
```

This will install dlt with the `bigquery` extra, which contains all the dependencies required by the BigQuery client.

**3. Log in to or create a Google Cloud account**

Sign up for or log in to the [Google Cloud Platform](https://console.cloud.google.com/) in your web browser.

**4. Create a new Google Cloud project**

After arriving at the [Google Cloud console welcome page](https://console.cloud.google.com/welcome), click the project selector in the top left, then click the `New Project` button, and finally click the `Create` button after naming the project whatever you would like.

**5. Create a service account and grant BigQuery permissions**

You will then need to [create a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating). After clicking the `Go to Create service account` button on the linked docs page, select the project you created and name the service account whatever you would like.

Click the `Continue` button and grant the following roles, so that `dlt` can create schemas and load data:

- *BigQuery Data Editor*
- *BigQuery Job User*
- *BigQuery Read Session User*

You don't need to grant users access to this service account now, so click the `Done` button.

**6. Download the service account JSON**

In the service accounts table page that you're redirected to after clicking `Done` as instructed above, select the three dots under the `Actions` column for the service account you created and select `Manage keys`.

This will take you to a page where you can click the `Add key` button, then the `Create new key` button, and finally the `Create` button, keeping the preselected `JSON` option.

A `JSON` file that includes your service account private key will then be downloaded.

**7. Update your `dlt` credentials file with your service account info**

Open your `dlt` credentials file:

```sh
open .dlt/secrets.toml
```

Replace the `project_id`, `private_key`, and `client_email` with the values from the downloaded `JSON` file:

```toml
[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```

You can specify the location of the data, i.e., `EU` instead of `US`, which is the default.

### OAuth 2.0 authentication

You can use OAuth 2.0 authentication. You'll need to generate a **refresh token** with the right scopes (we suggest asking our GPT-4 assistant for details). Then you can fill the following information in `secrets.toml`:

```toml
[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id="project_id"  # please set me up!
client_id = "client_id"  # please set me up!
client_secret = "client_secret"  # please set me up!
refresh_token = "refresh_token"  # please set me up!
```

### Using default credentials

Google provides several ways to get default credentials, i.e., from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or metadata services. VMs available on GCP (cloud functions, Composer runners, Colab notebooks) have associated service accounts or authenticated users. `dlt` will try to use default credentials if nothing is explicitly specified in the secrets.

```toml
[destination.bigquery]
location = "US"
```

### Using different `project_id`

You can set the `project_id` in your configuration to be different from the one in your credentials, provided your account has access to it:
```toml
[destination.bigquery]
project_id = "project_id_destination"

[destination.bigquery.credentials]
project_id = "project_id_credentials"
```
In this scenario, `project_id_credentials` will be used for authentication, while `project_id_destination` will be used as the data destination.

## Write disposition

All write dispositions are supported.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and recreated with a [clone command](https://cloud.google.com/bigquery/docs/table-clones-create) from the staging tables.

## Data loading

`dlt` uses `BigQuery` load jobs that send files from the local filesystem or GCS buckets.
The loader follows [Google recommendations](https://cloud.google.com/bigquery/docs/error-messages) when retrying and terminating jobs.
The Google BigQuery client implements an elaborate retry mechanism and timeouts for queries and file uploads, which may be configured in destination options.

BigQuery destination also supports [streaming insert](https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery). The mode provides better performance with small (<500 records) batches, but it buffers the data, preventing any update/delete operations on it. Due to this, streaming inserts are only available with `write_disposition="append"`, and the inserted data is blocked for editing for up to 90 min (reading, however, is available immediately). [See more](https://cloud.google.com/bigquery/quotas#streaming_inserts).

To switch the resource into streaming insert mode, use hints:
```py
@dlt.resource(write_disposition="append")
def streamed_resource():
    yield {"field1": 1, "field2": 2}

streamed_resource.apply_hints(additional_table_hints={"x-insert-api": "streaming"})
```

### Use BigQuery schema autodetect for nested fields
You can let BigQuery infer schemas and create destination tables instead of `dlt`. As a consequence, nested fields (i.e., `RECORD`), which `dlt` does not support at
this moment (they are stored as JSON), may be created. You can select certain resources with the [BigQuery Adapter](#bigquery-adapter) or all of them with the following config option:
```toml
[destination.bigquery]
autodetect_schema=true
```
We recommend yielding [arrow tables](../verified-sources/arrow-pandas.md) from your resources and using the `parquet` file format to load the data. In that case, the schemas generated by `dlt` and BigQuery
will be identical. BigQuery will also preserve the column order from the generated parquet files. You can convert `json` data into arrow tables with [pyarrow or duckdb](../verified-sources/arrow-pandas.md#loading-json-documents).

```py
import pyarrow.json as paj

import dlt
from dlt.destinations.adapters import bigquery_adapter

@dlt.resource(name="cve")
def load_cve():
  with open("cve.json", 'rb') as f:
    # autodetect arrow schema and yield arrow table
    yield paj.read_json(f)

pipeline = dlt.pipeline("load_json_struct", destination="bigquery")
pipeline.run(
  bigquery_adapter(load_cve(), autodetect_schema=True)
)
```
Above, we use the `pyarrow` library to convert a JSON document into an Arrow table and use `bigquery_adapter` to enable schema autodetect for the **cve** resource.

Yielding Python dicts/lists and loading them as JSONL works as well. In many cases, the resulting nested structure is simpler than those obtained via pyarrow/duckdb and parquet. However, there are slight differences in inferred types from `dlt` (BigQuery coerces types more aggressively). BigQuery also does not try to preserve the column order in relation to the order of fields in JSON.

```py
import dlt
from dlt.destinations.adapters import bigquery_adapter

@dlt.resource(name="cve", max_table_nesting=1)
def load_cve():
  with open("cve.json", 'rb') as f:
    yield json.load(f)

pipeline = dlt.pipeline("load_json_struct", destination="bigquery")
pipeline.run(
  bigquery_adapter(load_cve(), autodetect_schema=True)
)
```
In the example below, we represent JSON data as tables up to nesting level 1. Above this nesting level, we let BigQuery create nested fields.

:::caution
If you yield data as Python objects (dicts) and load this data as `parquet`, the nested fields will be converted into strings. This is one of the consequences of
`dlt` not being able to infer nested fields.
:::

## Supported file formats

You can configure the following file formats to load data to BigQuery:

* [jsonl](../file-formats/jsonl.md) is used by default.
* [parquet](../file-formats/parquet.md) is supported.

When staging is enabled:

* [jsonl](../file-formats/jsonl.md) is used by default.
* [parquet](../file-formats/parquet.md) is supported.

:::caution
**BigQuery cannot load JSON columns from Parquet files**. `dlt` will fail such jobs permanently. Instead:
* Switch to `jsonl` to load and parse JSON properly.
* Use schema [autodetect and nested fields](#use-bigquery-schema-autodetect-for-nested-fields)
:::

## Supported column hints

BigQuery supports the following [column hints](../../general-usage/schema#tables-and-columns):

* `partition` - creates a partition with a day granularity on the decorated column (`PARTITION BY DATE`).
  It may be used with `datetime`, `date`, and `bigint` data types.
  Only one column per table is supported and only when a new table is created.
  For more information on BigQuery partitioning, read the [official docs](https://cloud.google.com/bigquery/docs/partitioned-tables).

  > ❗ `bigint` maps to BigQuery's **INT64** data type.
  > Automatic partitioning requires converting an INT64 column to a UNIX timestamp, which `GENERATE_ARRAY` doesn't natively support.
  > With a 10,000 partition limit, we can’t cover the full INT64 range.
  > Instead, we set 86,400-second boundaries to enable daily partitioning.
  > This captures typical values, but extremely large/small outliers go to an `__UNPARTITIONED__` catch-all partition.

* `cluster` - creates cluster column(s). Many columns per table are supported and only when a new table is created.

### Table and column identifiers
BigQuery uses case-sensitive identifiers by default, and this is what `dlt` assumes. If the dataset you use has case-insensitive identifiers (you have such an option
when you create it), make sure that you use a case-insensitive [naming convention](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) or you tell `dlt` about it so identifier collisions are properly detected.
```toml
[destination.bigquery]
has_case_sensitive_identifiers=false
```

You have an option to allow `dlt` to set the case sensitivity for newly created datasets. In that case, it will follow the case sensitivity of the current
naming convention (i.e., the default **snake_case** will create a dataset with case-insensitive identifiers).
```toml
[destination.bigquery]
should_set_case_sensitivity_on_new_dataset=true
```
The option above is off by default.

## Staging support

BigQuery supports GCS as a file staging destination. `dlt` will upload files in the parquet format to GCS and ask BigQuery to copy their data directly into the database.
Please refer to the [Google Storage filesystem documentation](./filesystem.md#google-storage) to learn how to set up your GCS bucket with the bucket_url and credentials.
If you use the same service account for GCS and your Redshift deployment, you do not need to provide additional authentication for BigQuery to be able to read from your bucket.

Alternatively to parquet files, you can specify jsonl as the staging file format. For this, set the `loader_file_format` argument of the `run` command of the pipeline to `jsonl`.

### BigQuery/GCS staging example

```py
# Create a dlt pipeline that will load
# chess player data to the BigQuery destination
# via a GCS bucket.
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='bigquery',
    staging='filesystem', # Add this to activate the staging location.
    dataset_name='player_data'
)
```

## Additional destination options

You can configure the data location and various timeouts as shown below. This information is not a secret so it can be placed in `config.toml` as well:

```toml
[destination.bigquery]
location="US"
http_timeout=15.0
file_upload_timeout=1800.0
retry_deadline=60.0
```

* `location` sets the [BigQuery data location](https://cloud.google.com/bigquery/docs/locations) (default: **US**)
* `http_timeout` sets the timeout when connecting and getting a response from the BigQuery API (default: **15 seconds**)
* `file_upload_timeout` is a timeout for file upload when loading local files: the total time of the upload may not exceed this value (default: **30 minutes**, set in seconds)
* `retry_deadline` is a deadline for a [DEFAULT_RETRY used by Google](https://cloud.google.com/python/docs/reference/storage/1.39.0/retry_timeout)

### dbt support

This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-bigquery](https://github.com/dbt-labs/dbt-bigquery).
Credentials, if explicitly defined, are shared with `dbt` along with other settings like **location**, retries, and timeouts.
In the case of implicit credentials (i.e., available in a cloud function), `dlt` shares the `project_id` and delegates obtaining credentials to the `dbt` adapter.

### Syncing of dlt state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## BigQuery adapter

You can use the `bigquery_adapter` to add BigQuery-specific hints to a resource.
These hints influence how data is loaded into BigQuery tables, such as specifying partitioning, clustering, and numeric column rounding modes.
Hints can be defined at both the column level and table level.

The adapter updates the DltResource with metadata about the destination column and table DDL options.

### Use an adapter to apply hints to a resource

Here is an example of how to use the `bigquery_adapter` method to apply hints to a resource on both the column level and table level:

```py
from datetime import date, timedelta

import dlt
from dlt.destinations.adapters import bigquery_adapter


@dlt.resource(
    columns=[
        {"name": "event_date", "data_type": "date"},
        {"name": "user_id", "data_type": "bigint"},
        # Other columns.
    ]
)
def event_data():
    yield from [
        {"event_date": date.today() + timedelta(days=i)} for i in range(100)
    ]


# Apply column options.
bigquery_adapter(
    event_data, partition="event_date", cluster=["event_date", "user_id"]
)

# Apply table level options.
bigquery_adapter(event_data, table_description="Dummy event data.")

# Load data in "streaming insert" mode (only available with
# write_disposition="append").
bigquery_adapter(event_data, insert_api="streaming")
```

In the example above, the adapter specifies that `event_date` should be used for partitioning and both `event_date` and `user_id` should be used for clustering (in the given order) when the table is created.

Some things to note with the adapter's behavior:

- You can only partition on one column (refer to [supported hints](#supported-column-hints)).
- You can cluster on as many columns as you would like.
- Sequential adapter calls on the same resource accumulate parameters, akin to an OR operation, for a unified execution.

> ❗ At the time of writing, table level options aren't supported for `ALTER` operations.

Note that `bigquery_adapter` updates the resource *in place*, but returns the resource for convenience, i.e., both the following are valid:

```py
bigquery_adapter(my_resource, partition="partition_column_name")
my_resource = bigquery_adapter(my_resource, partition="partition_column_name")
```

Refer to the [full API specification](../../api_reference/destinations/impl/bigquery/bigquery_adapter) for more details.

<!--@@@DLT_TUBA bigquery-->

