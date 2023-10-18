---
title: Google BigQuery
description: Google BigQuery `dlt` destination
keywords: [bigquery, destination, data warehouse]
---

# Google BigQuery

## Setup Guide

**1. Initalize a project with a pipeline that loads to BigQuery by running**
```
dlt init chess bigquery
```

**2. Install the necessary dependencies for BigQuery by running**
```
pip install -r requirements.txt
```
This will install dlt with **bigquery** extra which contains all the dependencies required by bigquery client.

**3. Log in to or create a Google Cloud account**

Sign up for or log in to the [Google Cloud Platform](https://console.cloud.google.com/) in your web browser.

**4. Create a new Google Cloud project**

After arriving at the [Google Cloud console welcome page](https://console.cloud.google.com/welcome), click the
project selector in the top left, then click the `New Project` button, and finally click the `Create` button
after naming the project whatever you would like.

**5. Create a service account and grant BigQuery permissions**

You will then need to [create a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating). After clicking the `Go to Create service account` button
on the linked docs page, select the project you just created and name the service account whatever you would like.

Click the `Continue` button and grant the following roles, so that `dlt` can create schemas and load data:
- *BigQuery Data Editor*
- *BigQuery Job User*
- *BigQuery Read Session User*

You don't need to grant users access to this service account at this time, so click the `Done` button.

**6. Download the service account JSON**

In the service accounts table page that you are redirected to after clicking `Done` as instructed above,
select the three dots under the `Actions` column for the service account you just created and
select `Manage keys`.

This will take you to page where you can click the `Add key` button, then the `Create new key` button,
and finally the `Create` button, keeping the preselected `JSON` option.

A `JSON` file that includes your service account private key will then be downloaded.

**7. Update your `dlt` credentials file with your service account info**

Open your `dlt` credentials file:
```
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

You can also specify location of the data ie. `EU` instead of `US` which is a default.

### OAuth 2.0 authentication
You can also use the OAuth 2.0 authentication. You'll need to generate an **refresh token** with right scopes (I suggest to ask our GPT-4 assistant for details). Then you can fill the following information in `secrets.toml`
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
Google provides several ways to get default credentials ie. from `GOOGLE_APPLICATION_CREDENTIALS` environment variable or metadata services. VMs available on GCP (cloud functions, Composer runners, Colab notebooks) have associated service accounts or authenticated users. `dlt` will try to use default credentials if nothing is explicitly specified in the secrets
```toml
[destination.bigquery]
location = "US"
```
## Write disposition
All write dispositions are supported

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized` the destination tables will be dropped and
  recreated with a [clone command](https://cloud.google.com/bigquery/docs/table-clones-create) from the staging tables.

## Data loading
`dlt` uses `BigQuery` load jobs that send files from local filesystem or gcs buckets. Loader follows [Google recommendations](https://cloud.google.com/bigquery/docs/error-messages) when retrying and terminating jobs. Google BigQuery client implements elaborate retry mechanism and timeouts for queries and file uploads, which may be configured in destination options.

## Supported file formats
You can configure the following file formats to load data to BigQuery
* [jsonl](../file-formats/jsonl.md) is used by default
* [parquet](../file-formats/parquet.md) is supported

When staging is enabled:
* [jsonl](../file-formats/jsonl.md) is used by default
* [parquet](../file-formats/parquet.md) is supported

> ❗ **Bigquery cannot load JSON columns from `parquet` files**. `dlt` will fail such jobs permanently. Switch to `jsonl` to load and parse JSON properly.

## Supported column hints
BigQuery supports the following [column hints](https://dlthub.com/docs/general-usage/schema#tables-and-columns):
* `partition` - creates a partition with a day granularity on decorated column (`PARTITION BY DATE`). May be used with `datetime`, `date` data types and `bigint` and `double` if they contain valid UNIX timestamps. Only one column per table is supported and only when a new table is created.
* `cluster` - creates a cluster column(s). Many column per table are supported and only when a new table is created.

## Staging Support

BigQuery supports gcs as a file staging destination. dlt will upload files in the parquet format to gcs and ask BigQuery to copy their data directly into the db. Please refer to the [Google Storage filesystem documentation](./filesystem.md#google-storage) to learn how to set up your gcs bucket with the bucket_url and credentials. If you use the same service account for gcs and your redshift deployment, you do not need to provide additional authentication for BigQuery to be able to read from your bucket.
```toml
```

Alternatively to parquet files, you can also specify jsonl as the staging file format. For this set the `loader_file_format` argument of the `run` command of the pipeline to `jsonl`.

### BigQuery/GCS staging Example Code

```python
# Create a dlt pipeline that will load
# chess player data to the BigQuery destination
# via a gcs bucket.
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='biquery',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
```

## Additional destination options
You can configure the data location and various timeouts as shown below. This information is not a secret so can be placed in `config.toml` as well.
```toml
[destination.bigquery]
location="US"
http_timeout=15.0
file_upload_timeout=1800.0
retry_deadline=60.0
```
* `location` sets the [BigQuery data location](https://cloud.google.com/bigquery/docs/locations) (default: **US**)
* `http_timeout` sets the timeout when connecting and getting a response from BigQuery API (default: **15 seconds**)
* `file_upload_timeout` a timeout for file upload when loading local files: the total time of the upload may not exceed this value (default: **30 minutes**, set in seconds)
* `retry_deadline` a deadline for a [DEFAULT_RETRY used by Google](https://cloud.google.com/python/docs/reference/storage/1.39.0/retry_timeout)

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-bigquery](https://github.com/dbt-labs/dbt-bigquery). Credentials, if explicitly defined, are shared with `dbt` along with other settings like **location** and retries and timeouts. In case of implicit credentials (ie. available in cloud function), `dlt` shares the `project_id` and delegates obtaining credentials to `dbt` adapter.

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination)
