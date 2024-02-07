---
title: 'Share a dataset: DuckDB -> BigQuery'
description: Share a local dataset by moving it to BigQuery
keywords: [how to, share a dataset]
---

# Share a dataset: DuckDB -> BigQuery

In previous how-to guides you used the local stack to create and run your pipeline. This saved you
the headache of setting up cloud account, credentials and often also money. Our choice for local
"warehouse" is `duckdb`, fast, feature rich and working everywhere. However, at some point you want
to move to production or share the results with your colleagues. The local `duckdb` file is not
sufficient for that! Let's move a [dataset for chess.com API we have already](run-a-pipeline.md) to
BigQuery:

## 1. Replace the "destination" argument with "bigquery"

```python
import dlt
from chess import chess

if __name__ == "__main__" :
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination='bigquery',
        dataset_name="games_data"
    )
    # get data for a few famous players
    data = chess(
        data=['magnuscarlsen', 'rpragchess'],
        start_month="2022/11",
        end_month="2022/12"
    )
    load_info = pipeline.run(data)
```

And that's it regarding the code modifications! If you run the script, `dlt` will create identical
dataset you had in `duckdb` but in BigQuery.

## 2. Enable access to BigQuery and obtain credentials

Please [follow those steps](../dlt-ecosystem/destinations/bigquery.md) to enable `dlt` to write data
to BigQuery.

## 3. Add credentials to secrets.toml

Please add the following section to your `secrets.toml` file, use the credentials obtained from the
previous step:

```toml
[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```

## 4. Run the pipeline again

```shell
python chess_pipeline.py
```

Head on to the next section if you see exceptions!

## 5. Troubleshoot exceptions

### Credentials Missing: ConfigFieldMissingException

You'll see this exception if `dlt` cannot find your BigQuery credentials. In the exception below all
of them ('project_id', 'private_key', 'client_email') are missing. The exception gives you also the
list of all lookups for configuration performed -
[here we explain how to read such list](run-a-pipeline.md#missing-secret-or-configuration-values).

```
dlt.common.configuration.exceptions.ConfigFieldMissingException: Following fields are missing: ['project_id', 'private_key', 'client_email'] in configuration with spec GcpServiceAccountCredentials
    for field "project_id" config providers and keys were tried in following order:
        In Environment Variables key CHESS__DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID was not found.
        In Environment Variables key CHESS__DESTINATION__CREDENTIALS__PROJECT_ID was not found.
```

The most common cases for the exception:

1. The secrets are not in `secrets.toml` at all.
1. The are placed in wrong section. For example the fragment below will not work:
  ```toml
  [destination.bigquery] # 'credentials' missed
  project_id = "project_id"
  ```
1. You run the pipeline script from the **different** folder from which it is saved. For example
   `python chess_demo/chess_pipeline.py.py` will run the script from `chess_demo` folder but the
   current working directory is folder above. This prevents `dlt` from finding
   `chess_demo/.dlt/secrets.toml` and filling-in credentials.

### Placeholders still in secrets.toml

Here BigQuery complain that the format of the `private_key` is incorrect. Practically this most
often happens if you forgot to replace the placeholders in `secrets.toml` with real values:

```
<class 'dlt.destinations.exceptions.DestinationConnectionError'>
Connection with BigQuerySqlClient to dataset name games_data failed. Please check if you configured the credentials at all and provided the right credentials values. You can be also denied access or your internet connection may be down. The actual reason given is: No key could be detected.
```

### BigQuery not enabled

[You must enable BigQuery API.](https://console.cloud.google.com/apis/dashboard)

```
<class 'google.api_core.exceptions.Forbidden'>
403 POST https://bigquery.googleapis.com/bigquery/v2/projects/bq-walkthrough/jobs?prettyPrint=false: BigQuery API has not been used in project 364286133232 before or it is disabled. Enable it by visiting https://console.developers.google.com/apis/api/bigquery.googleapis.com/overview?project=364286133232 then retry. If you enabled this API recently, wait a few minutes for the action to propagate to our systems and retry.

Location: EU
Job ID: a5f84253-3c10-428b-b2c8-1a09b22af9b2
 [{'@type': 'type.googleapis.com/google.rpc.Help', 'links': [{'description': 'Google developers console API activation', 'url': 'https://console.developers.google.com/apis/api/bigquery.googleapis.com/overview?project=364286133232'}]}, {'@type': 'type.googleapis.com/google.rpc.ErrorInfo', 'reason': 'SERVICE_DISABLED', 'domain': 'googleapis.com', 'metadata': {'service': 'bigquery.googleapis.com', 'consumer': 'projects/364286133232'}}]
```

### Lack of permissions to create jobs

Add `BigQuery Job User` as described in the
[destination page](../dlt-ecosystem/destinations/bigquery.md).

```
<class 'google.api_core.exceptions.Forbidden'>
403 POST https://bigquery.googleapis.com/bigquery/v2/projects/bq-walkthrough/jobs?prettyPrint=false: Access Denied: Project bq-walkthrough: User does not have bigquery.jobs.create permission in project bq-walkthrough.

Location: EU
Job ID: c1476d2c-883c-43f7-a5fe-73db195e7bcd
```

### Lack of permissions to query/write data

Add `BigQuery Data Editor` as described in the
[destination page](../dlt-ecosystem/destinations/bigquery.md).

```
<class 'dlt.destinations.exceptions.DatabaseTransientException'>
403 Access Denied: Table bq-walkthrough:games_data._dlt_loads: User does not have permission to query table bq-walkthrough:games_data._dlt_loads, or perhaps it does not exist in location EU.

Location: EU
Job ID: 299a92a3-7761-45dd-a433-79fdeb0c1a46
```

### Lack of billing / BigQuery in sandbox mode

`dlt` does not support BigQuery when project has no billing enabled. If you see a stack trace where
following warning appears:

```
<class 'dlt.destinations.exceptions.DatabaseTransientException'>
403 Billing has not been enabled for this project. Enable billing at https://console.cloud.google.com/billing. DML queries are not allowed in the free tier. Set up a billing account to remove this restriction.
```

or

```
2023-06-08 16:16:26,769|[WARNING]|8096|dlt|load.py|complete_jobs:198|Job for players_games_83b8ac9e98_4_jsonl retried in load 1686233775.932288 with message {"error_result":{"reason":"billingNotEnabled","message":"Billing has not been enabled for this project. Enable billing at https://console.cloud.google.com/billing. Table expiration time must be less than 60 days while in sandbox mode."},"errors":[{"reason":"billingNotEnabled","message":"Billing has not been enabled for this project. Enable billing at https://console.cloud.google.com/billing. Table expiration time must be less than 60 days while in sandbox mode."}],"job_start":"2023-06-08T14:16:26.850000Z","job_end":"2023-06-08T14:16:26.850000Z","job_id":"players_games_83b8ac9e98_4_jsonl"}
```

you must enable the billing.
