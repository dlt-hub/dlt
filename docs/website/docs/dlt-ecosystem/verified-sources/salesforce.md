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

- **Bulk loading**: data is read with [Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)
  query jobs, which return CSV. sObjects that the Bulk API does not support are read with the REST API.
- **Typed columns**: field types come from the sObject description, before any data is loaded.
- **Incremental loading**: by default, only records changed since the last run are loaded and merged
  on `Id`. Records deleted in Salesforce are deleted from the destination.
- **Fast with `pyarrow`**: when `pyarrow` is installed, data is parsed and loaded as Arrow tables. It
  is optional: without it, data is loaded as Python dicts.

## Setup guide

### Grab credentials

We recommend the OAuth 2.0 client credentials flow, which needs a client id and secret instead of a
password:

1. In Salesforce Setup, create an **External Client App** and enable OAuth.
1. Enter any URL as the callback URL, e.g., `https://login.salesforce.com/services/oauth2/success`.
   The client credentials flow does not use it.
1. Add the OAuth scope **Manage user data via APIs (api)** and select **Enable Client Credentials Flow**.
1. In the app policies, select the **Run As** user. The source reads data with the permissions of this user.
1. Copy the **Consumer Key** and **Consumer Secret** from the OAuth settings.
1. Note your org's **My Domain**, e.g., `mycompany.my.salesforce.com`. The client credentials flow
   does not work with `login.salesforce.com`.

The source also supports user name, password and security token, an IP allowlist with organization
id, the JWT bearer flow, a connected app with a user name, and an existing session id. Salesforce
retires the SOAP `login()` call used for user name and password in Summer '27, and it is disabled by
default in orgs created since Winter '26, so prefer one of the OAuth flows.

> The Salesforce UI might change. The full guide is available at
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

1. Next, follow the [destination documentation](../destinations/index.md) instructions to
   add credentials for your chosen destination.

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

Each resource runs one SOQL query. With `api="auto"` (default), the query runs as a Bulk API 2.0 job:
Salesforce splits large sObjects into chunks on its side, and the source downloads the CSV results page
by page, `chunk_size` records at a time. When the Bulk API does not support an sObject (e.g.,
`TaskStatus`) or is not enabled in the org, the source reads the query results with the REST API
instead. Set `api="bulk"` or `api="rest"` to force one API.

All fields that the API can return are selected. Compound fields such as addresses are loaded as their
individual fields, e.g., `billing_street` and `billing_city`, which hold the same data. Base64 fields
such as attachment bodies are not loaded.

### Data types

Column types come from the sObject description, so they are known before any data is loaded:

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

Only the primary key is created as `NOT NULL`. Fields that Salesforce declares as not nillable stay
nullable, so a single blank value cannot fail the load.

Change the type of a column with [`apply_hints`](../../general-usage/resource.md#set-table-name-and-adjust-schema),
using the original Salesforce field name:

```py notype
account = salesforce_table(sobject="Account")
account.apply_hints(columns={"AnnualRevenue": {"data_type": "double"}})
```

### Incremental loading and write disposition

With `incremental="auto"` (default), the source picks the first of these fields that the sObject has
and sets up the [incremental loading](../../general-usage/incremental-loading.md) for you:

| Cursor field       | Write disposition | Typical sObjects                       |
| ------------------ | ----------------- | -------------------------------------- |
| `SystemModstamp`   | `merge`           | almost all, it changes on every update |
| `LastModifiedDate` | `merge`           | sObjects without `SystemModstamp`      |
| `CreatedDate`      | `append`          | insert-only, e.g., field history       |
| `LoginTime`        | `append`          | login history                          |
| none               | `replace`         | settings and metadata sObjects         |

- The cursor condition is added to the SOQL query, so each run reads only the records changed since
  the previous run.
- The primary key is `Id`, so merged records are updated in place.
- Merging re-reads the last 10 minutes on each run (`lag=600`) to catch records committed late. Set
  `lag` to change it.
- Salesforce timestamps have a precision of seconds, so many records may share the last cursor value.
  When merging on a timestamp cursor, the incremental does not
  [deduplicate](../../general-usage/incremental-loading.md) these records: they are loaded again and
  merged on `Id`.
- `write_disposition="replace"` reloads all records on each run, without incremental loading.
  Since dlt 1.32, `write_disposition={"disposition": "replace"}` keeps the incremental state, so the
  table is replaced with the records changed since the previous run.
- `incremental=None` loads all records on each run, and any `dlt.sources.incremental` on any field
  can be passed to `salesforce_table` instead.

### Deleted records

With `include_deleted=True` (default), queries also return deleted records that are still in the
Salesforce recycle bin and archived activities. When merging, `IsDeleted` is the
[hard delete](../../general-usage/merge-loading.md#delete-records) column, so records deleted in
Salesforce are deleted from the destination. Set `include_deleted=False` to skip deleted and archived
records.

### Backends

`backend="auto"` (default) uses `pyarrow` when it is installed:

- `"pyarrow"` parses the CSV results with `pyarrow` and yields Arrow tables, which are loaded as
  Parquet. It is much faster on large sObjects.
- `"python"` reads the CSV results with the Python standard library and yields dicts of strings. dlt
  converts them to the column types when it normalizes the data. It does not import `pyarrow`.

Both backends load the same values and types.

Switching an existing table from `"python"` to `"pyarrow"`, e.g., by installing `pyarrow` with
`backend="auto"`, needs these settings, because Arrow tables do not get the `_dlt_load_id` and
`_dlt_id` columns by default:

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
    incremental: Optional[Literal["auto"]] = "auto",
    write_disposition: Optional[TWriteDispositionConfig] = None,
    soql_filters: Optional[Dict[str, str]] = None,
    include_deleted: bool = True,
    double_as_decimal: bool = False,
    backend: Literal["auto", "python", "pyarrow"] = "auto",
    api: Literal["auto", "bulk", "rest"] = "auto",
    chunk_size: int = 50000,
) -> Iterable[DltResource]:
    ...
```

- `credentials`: Salesforce credentials, see [Add credentials](#add-credentials).
- `session`: requests session used to call Salesforce. By default, HTTP 429 and 5xx responses are retried.
- `incremental`: `"auto"` loads records changed since the last run, `None` loads all records.
- `write_disposition`: write disposition of all resources. Derived per sObject if not set.
- `soql_filters`: SOQL conditions by resource name, e.g., `{"opportunity": "IsClosed = false"}`.
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

It takes the same arguments as `salesforce_source`, applied to one sObject, plus:

- `sobject`: API name of the sObject, e.g., `Account` or `Invoice__c`.
- `incremental`: `"auto"`, `None` or a [`dlt.sources.incremental`](../../general-usage/incremental-loading.md)
  on any field. Its range becomes the `WHERE` clause of the query, and `row_order` its `ORDER BY`.
- `lag`: seconds re-read on each run by the `"auto"` incremental.
- `soql_filter`: SOQL condition added to the `WHERE` clause of the query.
- `included_columns`, `excluded_columns`: fields to load or to skip. Names that are not queryable
  fields of the sObject are logged as a warning.
- `primary_key`, `merge_key`: by default, the primary key is `Id`.
- `defer_table_reflect`: logs in and describes the sObject only when extracting, see
  [Deploy on Airflow](#deploy-on-airflow). Also available on `salesforce_source`.

[`add_limit`](../../general-usage/resource.md#sample-from-large-data) is added to the query as
`LIMIT`, in multiples of `chunk_size`.

In config files, set the write disposition of `salesforce_table` as a table, in the section of its
sObject:

```toml
[sources.salesforce.Account]
write_disposition = { disposition = "replace" }
```

## Customization

### Create your own pipeline

1. Configure the pipeline by specifying the pipeline name, destination, and dataset:

   ```py notype
   pipeline = dlt.pipeline(
       pipeline_name="salesforce_pipeline",
       destination="duckdb",
       dataset_name="salesforce_data",
   )
   ```

1. Load all resources, merging the records changed since the last run:

   ```py notype
   load_info = pipeline.run(salesforce_source())
   print(load_info)
   ```

1. Reload open opportunities and their accounts in full on each run:

   ```py notype
   source = salesforce_source(
       write_disposition="replace",
       soql_filters={"opportunity": "IsClosed = false"},
   ).with_resources("account", "opportunity")
   load_info = pipeline.run(source)
   ```

1. Change the incremental of a single resource, e.g., load tasks created since 2024:

   ```py notype
   source = salesforce_source()
   source.resources["task"].apply_hints(
       incremental=dlt.sources.incremental("CreatedDate", initial_value="2024-01-01T00:00:00Z")
   )
   load_info = pipeline.run(source)
   ```

1. Backfill a large sObject in date windows. With an `end_value`, the incremental does not change the
   pipeline state, and each window runs as a separate query:

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

To load a large sObject over several runs, limit the records of each run with `add_limit` and order
them by the cursor with `row_order="asc"`, so each run continues where the previous one stopped. On a
field with unique values, such as an auto number or a sequence, use an open range so no record is
loaded twice, and stop when a run loads nothing:

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

On a timestamp such as `SystemModstamp`, keep the default closed range: records that share the last
timestamp are loaded again by the next run and merged on `Id`. Stop when a run loads less than a full
page:

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

The limit must be larger than the number of records that share a timestamp, e.g., records updated by
one mass update. Otherwise the cursor cannot move and the source logs a warning.

### Deploy on Airflow

[Airflow](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md) parses DAG files
every 30 seconds, and creating a resource logs in and describes its sObject. Pass
`defer_table_reflect=True` to do that only when extracting. To load the
[data interval](../../general-usage/incremental/cursor.md#using-airflow-schedule-for-backfill-and-incremental-loading)
of each Airflow run, pass an incremental with `allow_external_schedulers=True`, which raises an
error when no interval is available:

```py notype
tasks = salesforce_table(
    sobject="Task",
    defer_table_reflect=True,
    incremental=dlt.sources.incremental("SystemModstamp", allow_external_schedulers=True),
)
```

With `defer_table_reflect`, the `"auto"` incremental expects `SystemModstamp`, and `add_limit`
does not work yet.

## Performance and troubleshooting

### Throughput

Salesforce runs each Bulk API query as a job, which usually takes 5-30 seconds to start returning
data, even for small sObjects. `salesforce_source` runs the jobs of all resources in parallel, five at
a time by default. To run more at once, set the number of extract workers:

```toml
[extract]
workers = 15
```

On large sObjects, parsing dominates. In our tests on a laptop, loading to the local filesystem:

| Backend   | 50,000 rows, 36 fields | 200,000 rows, 6 fields |
| --------- | ---------------------- | ---------------------- |
| `pyarrow` | 0.15 s                 | 0.12 s                 |
| `python`  | 2.1 s                  | 3.1 s                  |

Install `pyarrow` when you load large sObjects. With the `python` backend, the
[file format](../file-formats.md) of the destination matters as well: e.g., `insert_values` into
DuckDB took several times longer than the parsing itself.

### Large sObjects

- Ordering the results with `row_order` and `add_limit` add `ORDER BY` and `LIMIT` to the query, which
  disable chunking of Bulk API jobs. Use them on indexed fields, such as `SystemModstamp` or `Id`.
- Lower `chunk_size` if a result page needs too much memory.
- Use `soql_filter` or `included_columns` to read less data, and
  [backfill in windows](#create-your-own-pipeline) or [load in chunks](#load-in-chunks) when the first
  load is too large for a single run.

### Warnings about LIMIT

- `LIMIT ... is applied ... without ORDER BY`: the query returns any records up to the limit, and the
  next run starts after the largest cursor value among them, so records may be skipped. Set
  `row_order="asc"` on the incremental.
- `Could not move the cursor`: all records returned by the limit have the cursor value the run started
  from, so the next run reads them again. Increase `add_limit` or `chunk_size`. When the incremental
  has a `lag`, the limit must also cover the records within the lag.

### API limits and sessions

- HTTP 429 and 5xx responses are retried. Salesforce reports most limits as `REQUEST_LIMIT_EXCEEDED`
  with HTTP 403, which is not retried: check the daily API requests and Bulk API 2.0 query jobs of your
  org in Setup or with the `/limits` REST endpoint.
- The REST API returns at most 2,000 records per request, and each request counts against the daily API
  limit. Prefer the Bulk API for large sObjects.
- Long jobs may outlive the session. When the session expires, the source logs in again and continues
  with the next result page. This does not work for credentials with a `session_id` only.

### sObjects the Bulk API does not support

Some sObjects, e.g., status sObjects like `TaskStatus` and `CaseStatus`, cannot be read with the Bulk
API, and `api="auto"` reads them with the REST API. The REST API sends the query in the URL, so a query
selecting very many fields may be too long: select fewer fields with `included_columns`. Some sObjects
can only be queried with specific filters, e.g., `UserRecordAccess`, which you can pass with
`soql_filter`. Others, e.g., `OpenActivity`, can only be queried as part of their parent record and
cannot be loaded.

### How Salesforce stores values

Loaded values may differ from what was entered in Salesforce:

- Salesforce stores numbers, including currency, as doubles. Values with more than 15 significant
  digits are rounded by Salesforce, also with `double_as_decimal=True`.
- `datetime` fields have a precision of seconds. `time` fields keep milliseconds.
- Leading and trailing whitespace of text fields is trimmed, and rich text fields are HTML-escaped.
- Strings like `NA`, `N/A` or `NULL` are loaded as strings. Blank fields are loaded as nulls.
- Formula fields are computed when the record is read. When only the formula definition or a related
  record changes, the record's `SystemModstamp` does not change and incremental loading does not pick up
  the new value. Reload these sObjects or reproduce the calculation from the base fields.

### Deleted records are not removed

- Salesforce keeps deleted records in the recycle bin for 15 days. Records deleted and purged between
  two runs are not removed from the destination: run the pipeline more often or reload in full.
- Deleted `OpportunityLineItem` records and some other line item sObjects do not go to the recycle
  bin, so their deletes are never returned. Load these sObjects with `write_disposition="replace"`.

### Upgrading from an earlier version

Earlier versions of this source used the Bulk API 1.0 and loaded most fields as text, without primary
keys. This version changes column types (e.g., dates and currencies), adds `Id` as the primary key, and
merges all sObjects. Reload the data after upgrading:

```py notype
pipeline.run(salesforce_source(), refresh="drop_sources")
```

<!--@@@DLT_TUBA salesforce-->
