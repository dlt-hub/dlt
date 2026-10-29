---
title: Salesforce
description: dlt verified source that loads Salesforce sObjects with the Bulk API 2.0
keywords: [salesforce api, salesforce pipeline, salesforce, bulk api, soql, sobject]
---
# Salesforce

import Header from './_source-info-header.md';

<Header/>

[Salesforce](https://www.salesforce.com) is a cloud platform that streamlines business operations
and customer relationship management, encompassing sales, marketing, and customer service.

This Salesforce `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/salesforce_pipeline.py)
load Salesforce sObjects, standard and custom, to the destination of your choice:

- **Bulk loading**: dlt reads data with [Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)
  query jobs, which return CSV. For sObjects that the Bulk API does not support, dlt uses the REST API.
- **Typed columns**: field types come from the sObject description, before dlt loads any data.
- **Incremental loading**: by default, dlt loads only the records that changed since the last run and
  merges them on `Id`. `start_date` sets where the first run starts. When a record is deleted in
  Salesforce, dlt deletes it from the destination.
- **Fast with `pyarrow`**: when `pyarrow` is installed, dlt parses the data into Arrow tables.
  `pyarrow` is optional. Without it, dlt loads the data as Python dicts.

## Setup guide

### Grab credentials

We recommend the OAuth 2.0 client credentials flow. This flow needs a client id and a client secret
instead of a password:

1. In Salesforce Setup, create an **External Client App**. Then enable OAuth for this app.
1. Enter any URL as the callback URL, for example `https://login.salesforce.com/services/oauth2/success`.
   The client credentials flow does not use this URL.
1. Add the OAuth scope **Manage user data via APIs (api)**. Then select **Enable Client Credentials Flow**.
1. In the app policies, select the **Run As** user. dlt reads data with the permissions of this user.
1. Copy the **Consumer Key** and **Consumer Secret** from the OAuth settings.
1. Note your org's **My Domain**, for example `mycompany.my.salesforce.com`. The client credentials
   flow does not work with `login.salesforce.com`.

dlt also supports these authentication methods:

- user name, password and security token
- an IP allowlist with organization id
- the JWT bearer flow
- a connected app with a user name
- an existing session id

The security token and IP allowlist methods use the SOAP `login()` call. Salesforce will retire this
call in Summer '27. Salesforce disables this call by default in orgs created since Winter '26. For
this reason, the OAuth flows are a better choice.

> The Salesforce UI can change. The full guide is available at
> [this link.](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/quickstart_oauth.htm)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init salesforce duckdb
   ```

   [This command](../../reference/command-line-interface.md) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/salesforce_pipeline.py)
   with Salesforce as the [source](../../general-usage/source.md) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations/index.md).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations/index.md).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

### Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. Add the client credentials:

   ```toml
   # put your secret values and credentials here. do not share this file and do not push it to github
   [sources.salesforce.credentials]
   consumer_key = "please set me up!"
   consumer_secret = "please set me up!"
   domain = "mycompany.my" # My Domain without .salesforce.com
   ```

   For the other authentication methods, use these keys instead:

   | Method                       | Keys                                                                                    |
   | ---------------------------- | --------------------------------------------------------------------------------------- |
   | User name and security token | `user_name`, `password`, `security_token`                                               |
   | IP allowlist                 | `user_name`, `password`, `organization_id`                                              |
   | JWT bearer flow              | `user_name`, `consumer_key`, `privatekey_file` or `privatekey`, optional `instance_url` |
   | Connected app with user      | `user_name`, `password`, `consumer_key`, `consumer_secret`                              |
   | Existing session             | `session_id`, `instance` or `instance_url`                                              |

1. Optionally, configure the connection in `config.toml`:

   ```toml
   [sources.salesforce]
   domain = "test" # login domain, e.g. "test" for sandboxes
   version = "59.0" # Salesforce API version
   ```

1. Next, add the credentials for your destination. The
   [destination documentation](../destinations/index.md) shows how.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials/index.md)

### Run the pipeline

1. Install the dependencies. Install `pyarrow` as well for the fastest loading:

   ```sh
   pip install -r requirements.txt
   pip install pyarrow
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python salesforce_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `salesforce`, you may also use
   any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline.md).

## How data is loaded

The defaults below apply to every resource, from `salesforce_source` and from `salesforce_table`.

### Bulk loading

Each resource runs one SOQL query. With `api="auto"` (default), the query runs as a Bulk API 2.0 job.
Salesforce splits the job for a large sObject with PK chunking. dlt downloads the CSV results page by
page, with `chunk_size` records in each page.

If the Bulk API does not support an sObject (for example, `TaskStatus`), dlt reads the query results
with the REST API. dlt also uses the REST API when the Bulk API is not enabled in the org. With
`api="bulk"` or `api="rest"`, dlt uses only that API.

dlt selects all fields that the API can return. dlt loads compound fields, such as addresses, as their
individual fields, for example `billing_street` and `billing_city`. These fields hold the same data as
the compound field. dlt does not load base64 fields, such as attachment bodies.

### Data types

Column types come from the sObject description. As a result, dlt knows the column types before it
loads any data:

| Salesforce type                                                                                          | dlt type                                             |
| -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `currency`                                                                                               | `decimal` with the field precision and scale         |
| `double`, `percent`                                                                                      | `double`, or `decimal` with `double_as_decimal=True` |
| `int`, `long`                                                                                            | `bigint`                                             |
| `boolean`                                                                                                | `bool`                                               |
| `date`                                                                                                   | `date`                                               |
| `datetime`                                                                                               | `timestamp` in UTC                                   |
| `time`                                                                                                   | `time`                                               |
| `json`                                                                                                   | `json`                                               |
| `id`, `reference`, `string`, `picklist`, `multipicklist`, `textarea`, `email`, `phone`, `url` and others | `text`                                               |

dlt creates only the primary key as `NOT NULL`. Fields that Salesforce declares as not nillable stay
nullable. As a result, a single blank value cannot fail the load.

[`apply_hints`](../../general-usage/resource.md#set-table-name-and-adjust-schema) can change the type
of a column. The column name in the hint is the original Salesforce field name:

```py notype
account = salesforce_table(sobject="Account")
account.apply_hints(columns={"AnnualRevenue": {"data_type": "double"}})
```

### Incremental loading and write disposition

By default, dlt picks the first of these fields that the sObject has. Then dlt configures
[incremental loading](../../general-usage/incremental-loading.md) on this field:

| Cursor field       | Write disposition | Typical sObjects                               |
| ------------------ | ----------------- | ---------------------------------------------- |
| `SystemModstamp`   | `merge`           | almost all (the field changes on every update) |
| `LastModifiedDate` | `merge`           | sObjects without `SystemModstamp`              |
| `CreatedDate`      | `append`          | insert-only, for example field history         |
| `LoginTime`        | `append`          | login history                                  |
| none               | `replace`         | settings and metadata sObjects                 |

- dlt adds the cursor condition to the SOQL query. As a result, each run reads only the records that
  changed since the previous run. The cursor range of the query ends at the time that the query
  starts. The next run loads the records that change while a long query job runs.
- The primary key is `Id`. dlt updates existing records in place.
- With the `merge` write disposition, dlt re-reads the last 10 minutes on each run (`lag=600`). This
  catches records that Salesforce commits late. The `lag` argument changes this interval.
- Salesforce timestamps have a precision of seconds. As a result, many records can share the last
  cursor value. With the `merge` write disposition and a timestamp cursor, the incremental does not
  [deduplicate](../../general-usage/incremental-loading.md) these records. dlt loads them again and
  merges them on `Id`.
- On the first run, `start_date` loads only the records that changed since this date, for example
  `salesforce_source(start_date="2024-01-01")`. Without `start_date`, the first run loads all records.
- `write_disposition="replace"` reloads all records on each run, without incremental state. If
  `start_date` is set, each run reloads only the records that changed since `start_date`. Since dlt
  1.32, `write_disposition={"disposition": "replace"}` keeps the incremental state. dlt then replaces
  the table with the records that changed since the previous run.
- You can pass your own `dlt.sources.incremental` on any field to `salesforce_table`. On a resource of
  `salesforce_source`, you can set it with `apply_hints`. Your incremental replaces the default one.
  dlt uses it as is, without lag.

You can also set `start_date` in `config.toml`. The `[sources.salesforce]` section applies to
`salesforce_source` and to all `salesforce_table` resources. The section of an sObject applies only
to the `salesforce_table` of this sObject:

```toml
[sources.salesforce]
start_date = "2024-01-01"

[sources.salesforce.Account]
start_date = "2023-01-01"
```

### Deleted records

With `include_deleted=True` (default), queries also return archived activities and the deleted records
that are still in the Salesforce recycle bin. With the `merge` write disposition, `IsDeleted` is the
[hard delete](../../general-usage/merge-loading.md#delete-records) column. When a record is deleted in
Salesforce, dlt deletes it from the destination. `include_deleted=False` skips deleted and archived
records.

### Backends

`backend="auto"` (default) uses `pyarrow` when it is installed:

- `"pyarrow"` parses the CSV results with `pyarrow` and yields Arrow tables. dlt loads these tables as
  Parquet. This backend is much faster on large sObjects.
- `"python"` reads the CSV results with the Python standard library and yields dicts of strings. dlt
  converts them to the column types when it normalizes the data. This backend does not import
  `pyarrow`.

Both backends load the same values and types.

By default, Arrow tables do not get the `_dlt_load_id` and `_dlt_id` columns. The backend of a
resource can change from `"python"` to `"pyarrow"`, for example when you install `pyarrow` with
`backend="auto"`. After this change, an existing table needs these settings:

```toml
[normalize.parquet_normalizer]
add_dlt_load_id = true
add_dlt_id = true
```

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source.md) and
[resources](../../general-usage/resource.md).

### Source `salesforce_source`

Loads the most common sObjects. The queries of all resources run in parallel.

| Resource                   | sObject                |
| -------------------------- | ---------------------- |
| `sf_user`                  | User                   |
| `user_role`                | UserRole               |
| `account`                  | Account                |
| `contact`                  | Contact                |
| `lead`                     | Lead                   |
| `opportunity`              | Opportunity            |
| `opportunity_line_item`    | OpportunityLineItem    |
| `opportunity_contact_role` | OpportunityContactRole |
| `campaign`                 | Campaign               |
| `campaign_member`          | CampaignMember         |
| `product_2`                | Product2               |
| `pricebook_2`              | Pricebook2             |
| `pricebook_entry`          | PricebookEntry         |
| `task`                     | Task                   |
| `event`                    | Event                  |

```py notype
@dlt.source(name="salesforce")
def salesforce_source(
    credentials: SalesforceAuth = dlt.secrets.value,
    session: Optional[Session] = None,
    start_date: Optional[TAnyDateTime] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    soql_filters: Optional[Dict[str, str]] = None,
    include_deleted: bool = True,
    double_as_decimal: bool = False,
    backend: Literal["auto", "python", "pyarrow"] = "auto",
    api: Literal["auto", "bulk", "rest"] = "auto",
    chunk_size: int = 50000,
    defer_table_reflect: bool = False,
) -> Iterable[DltResource]:
    ...
```

- `credentials`: Salesforce credentials, see [Add credentials](#add-credentials).
- `session`: requests session used to call Salesforce. By default, dlt retries HTTP 429 and 5xx
  responses and the requests that Salesforce refuses over the limit of concurrent long requests.
- `start_date`: loads the records changed since this date on the first run, all records if not set.
- `write_disposition`: write disposition of all resources, for example `"replace"`. Derived per
  sObject if not set.
- `soql_filters`: SOQL conditions by resource name, for example `{"opportunity": "IsClosed = false"}`.
- `include_deleted`, `double_as_decimal`, `backend`, `api`, `chunk_size`: see [How data is loaded](#how-data-is-loaded).

### Resource `salesforce_table`

Loads any single sObject, including custom objects, like `sql_table` loads a database table:

```py notype
invoices = salesforce_table(
    sobject="Invoice__c",
    included_columns=["Id", "Name", "Amount__c", "IsDeleted", "SystemModstamp"],
    soql_filter="Status__c = 'Posted'",
)
pipeline.run(invoices)
```

It takes the arguments of `salesforce_source`, except `soql_filters`, for one sObject. It also takes
these arguments:

- `sobject`: API name of the sObject, for example `Account` or `Invoice__c`.
- `incremental`: your own [`dlt.sources.incremental`](../../general-usage/incremental-loading.md) on
  any field instead of the default one. Its range becomes the `WHERE` clause of the query. Its
  `row_order` becomes the `ORDER BY` clause. Pass `start_date` or `incremental`, not both.
- `lag`: seconds re-read on each run by the default incremental.
- `soql_filter`: SOQL condition added to the `WHERE` clause of the query.
- `included_columns`, `excluded_columns`: fields to load or to skip. dlt logs a warning for names that
  are not queryable fields of the sObject.
- `primary_key`, `merge_key`: by default, the primary key is `Id`.
- `defer_table_reflect`: logs in and describes the sObject only in the extract step, see
  [Deploy on Airflow](#deploy-on-airflow). Also available on `salesforce_source`.

dlt adds [`add_limit`](../../general-usage/resource.md#sample-from-large-data) to the query as
`LIMIT`, in multiples of `chunk_size`.

In config files, the write disposition of `salesforce_table` is a TOML table in the section of its
sObject:

```toml
[sources.salesforce.Account]
write_disposition = { disposition = "replace" }
```

## Customization

### Create your own pipeline

1. Configure the pipeline with a pipeline name, a destination, and a dataset:

   ```py notype
   pipeline = dlt.pipeline(
       pipeline_name="salesforce_pipeline",
       destination="duckdb",
       dataset_name="salesforce_data",
   )
   ```

1. Load all resources. dlt merges the records that changed since the last run:

   ```py notype
   load_info = pipeline.run(salesforce_source())
   print(load_info)
   ```

1. On the first run, skip the records that did not change since 2024:

   ```py notype
   load_info = pipeline.run(salesforce_source(start_date="2024-01-01"))
   ```

1. On each run, reload all accounts and the open opportunities in full:

   ```py notype
   source = salesforce_source(
       write_disposition="replace",
       soql_filters={"opportunity": "IsClosed = false"},
   ).with_resources("account", "opportunity")
   load_info = pipeline.run(source)
   ```

1. Change the incremental of a single resource. For example, load the tasks created since 2024:

   ```py notype
   source = salesforce_source()
   source.resources["task"].apply_hints(
       incremental=dlt.sources.incremental("CreatedDate", initial_value="2024-01-01T00:00:00Z")
   )
   load_info = pipeline.run(source)
   ```

1. Backfill a large sObject in date windows. With an `end_value`, the incremental does not change the
   pipeline state. Each window runs as a separate query:

   ```py notype
   for start, end in [("2023-01-01", "2023-07-01"), ("2023-07-01", "2024-01-01")]:
       tasks = salesforce_table(
           sobject="Task",
           incremental=dlt.sources.incremental(
               "SystemModstamp", initial_value=f"{start}T00:00:00Z", end_value=f"{end}T00:00:00Z"
           ),
       )
       pipeline.run(tasks)
   ```

### Load in chunks

To load a large sObject over several runs, limit the records of each run with `add_limit`. Then
order the records by the cursor with `row_order="asc"`. As a result, each run continues where the
previous run stopped. If the cursor field has unique values, such as an auto number or a sequence,
use an open range. With an open range, dlt does not load a record twice. When a run loads no
records, stop the loop:

```py notype
invoices = salesforce_table(
    sobject="Invoice__c",
    chunk_size=10000,
    incremental=dlt.sources.incremental("InvoiceNumber__c", row_order="asc", range_start="open"),
)
# each run loads one page of max 10000 records
while not pipeline.run(invoices.add_limit(1)).is_empty:
    pass
```

If the cursor is a timestamp such as `SystemModstamp`, keep the default closed range. The next run
loads the records that share the last timestamp again, and dlt merges them on `Id`. When a run loads
less than a full page, stop the loop:

```py notype
accounts = salesforce_table(
    sobject="Account",
    chunk_size=10000,
    incremental=dlt.sources.incremental("SystemModstamp", row_order="asc"),
)
while True:
    pipeline.run(accounts.add_limit(1))
    if pipeline.last_trace.last_normalize_info.row_counts.get("account", 0) < 10000:
        break
```

The limit must be larger than the number of records that share a timestamp, for example the records
of one mass update. Otherwise the cursor cannot move and dlt logs a warning.

### Deploy on Airflow

[Airflow](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md) parses DAG files
every 30 seconds. When you create a resource, dlt logs in and describes its sObject. To do this only
in the extract step, pass `defer_table_reflect=True`. To load the
[data interval](../../general-usage/incremental/cursor.md#using-airflow-schedule-for-backfill-and-incremental-loading)
of each Airflow run, pass an incremental with `allow_external_schedulers=True`. If no interval is
available, this incremental raises an error:

```py notype
tasks = salesforce_table(
    sobject="Task",
    defer_table_reflect=True,
    incremental=dlt.sources.incremental("SystemModstamp", allow_external_schedulers=True),
)
```

With `defer_table_reflect`, the default incremental expects `SystemModstamp`. Also, `add_limit` does
not work yet.

## Performance and troubleshooting

### Throughput

Salesforce runs each Bulk API query as a job. A job usually needs 5-30 seconds before it returns
data, even for small sObjects. `salesforce_source` runs the jobs of all resources in parallel, five at
a time by default. To run more at once, set the number of extract workers:

```toml
[extract]
workers = 15
```

On large sObjects, parsing takes most of the time. These are the results of our tests on a laptop,
with the local filesystem as the destination:

| Backend   | 50,000 rows, 36 fields | 200,000 rows, 6 fields |
| --------- | ---------------------- | ---------------------- |
| `pyarrow` | 0.15 s                 | 0.12 s                 |
| `python`  | 2.1 s                  | 3.1 s                  |

If you load large sObjects, install `pyarrow`. With the `python` backend, the
[file format](../file-formats.md) of the destination also has an effect. For example, `insert_values`
into DuckDB took several times longer than the parsing.

### Large sObjects

- `row_order` adds `ORDER BY` to the query, and `add_limit` adds `LIMIT`. These clauses disable PK
  chunking of Bulk API jobs. Use them with a cursor on an indexed field, such as `SystemModstamp` or
  `Id`.
- If a result page needs too much memory, decrease `chunk_size`.
- A Bulk API query job that does not complete in 24 hours is aborted and fails the load.
- To read less data, use `soql_filter` or `included_columns`. If the first load is too large for a
  single run, [backfill in windows](#create-your-own-pipeline) or [load in chunks](#load-in-chunks).

### Warnings about LIMIT

- `LIMIT ... is applied ... without ORDER BY`: the query returns records in no specific order, up to
  the limit. The next run starts after the largest cursor value among them. As a result, the pipeline
  can skip records. Set `row_order="asc"` on the incremental.
- `Could not move the cursor`: all records that the limit returns have the cursor value that the run
  started from. As a result, the next run reads them again. Increase `add_limit` or `chunk_size`. When the incremental
  has a `lag`, the limit must also cover the records within the lag.

### API limits and sessions

- dlt retries HTTP 429 and 5xx responses. Salesforce reports most limits as `REQUEST_LIMIT_EXCEEDED`
  with HTTP 403. dlt also retries the requests that Salesforce refuses because the org runs too many
  long requests at once. Other limits, for example the daily API requests, fail the load. If a load
  fails on a limit, examine the API usage of your org. Setup and the `/limits` REST endpoint show the
  daily API requests and the Bulk API 2.0 query jobs.
- The REST API returns a maximum of 2,000 records for each request. Each request counts against the
  daily API limit. For large sObjects, use the Bulk API.
- A long job can run longer than the session. When the session expires, dlt logs in again and
  continues with the next result page. This new login does not work for credentials that have only a
  `session_id`.

### sObjects the Bulk API does not support

Some sObjects need a different query:

- The Bulk API cannot read some sObjects, for example the status sObjects `TaskStatus` and
  `CaseStatus`. With `api="auto"`, dlt reads them with the REST API.
- With `api="auto"`, dlt also uses the REST API when the Bulk API rejects a compound field of the
  sObject.
- The REST API sends the query in the URL. If a query selects very many fields, the URL can be too
  long. In this case, select fewer fields with `included_columns`.
- Some sObjects, for example `UserRecordAccess`, can only be queried with specific filters. Pass these
  filters with `soql_filter`.
- Other sObjects, for example `OpenActivity`, can only be queried as part of their parent record. dlt
  cannot load these sObjects.

### How Salesforce stores values

Loaded values can be different from the values that were entered in Salesforce:

- Salesforce stores numbers, including currency, as doubles. Salesforce rounds values with more than
  15 significant digits. This rounding also occurs with `double_as_decimal=True`.
- `datetime` fields have a precision of seconds. `time` fields keep milliseconds.
- Salesforce trims the leading and trailing whitespace of text fields. Salesforce returns rich text
  fields HTML-escaped.
- dlt loads strings such as `NA`, `N/A` or `NULL` as strings. dlt loads blank fields as nulls.
- Salesforce computes formula fields when a query reads the record. Salesforce rounds the result to
  the scale of the field. If only the formula definition, a related record or the date of `TODAY()`
  changes, the `SystemModstamp` of the record does not change. As a result, incremental loading does
  not get the new value. Reload these sObjects or reproduce the calculation from the base fields.
- Sharing changes, for example changes to account and opportunity team members, do not change
  `SystemModstamp` either.

### Deleted records stay in the destination

Salesforce keeps deleted records in the recycle bin for 15 days. If records are deleted and purged
between two runs, dlt does not delete them from the destination. Run the pipeline more often, or
reload in full.

### Upgrade from an earlier version

Earlier versions of this source used the Bulk API 1.0 and loaded most fields as text, without primary
keys. This version changes column types (for example, dates and currencies), adds `Id` as the primary
key, and merges all sObjects. After the upgrade, reload the data:

```py notype
pipeline.run(salesforce_source(), refresh="drop_sources")
```

<!--@@@DLT_TUBA salesforce-->
