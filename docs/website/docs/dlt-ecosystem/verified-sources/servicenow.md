---
title: ServiceNow
description: dlt verified source that loads ServiceNow tables with the Table API
keywords: [servicenow api, servicenow pipeline, servicenow, table api, itsm, cmdb]
---
# ServiceNow

import Header from './_source-info-header.md';

<Header/>

[ServiceNow](https://www.servicenow.com) is a platform for IT service management, IT operations
and other enterprise workflows. ServiceNow keeps its data in tables: incidents, changes, catalog
requests, users, configuration items (CMDB) and custom tables.

This ServiceNow `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/servicenow_pipeline.py)
load ServiceNow tables to the destination of your choice:

- **Table API**: dlt reads tables with the
  [Table API](https://www.servicenow.com/docs/r/api-reference/rest-apis/c_TableAPI.html) or with
  the CSV export of a list.
- **Typed columns**: column types come from `sys_dictionary`, before dlt loads any data. A table
  also gets the fields that it inherits, for example `incident` from `task`.
- **Incremental loading**: by default, dlt loads only the records that changed since the last run
  and merges them on `sys_id`. `start_date` sets where the first run starts. When a record is
  deleted in ServiceNow, dlt deletes it from the destination.
- **Parallel reads**: dlt reads tables in parallel. dlt also splits a large table into `sys_id`
  ranges and reads the ranges in parallel.
- **Fast with `pyarrow`**: when `pyarrow` is installed, dlt parses each page into an Arrow table.
  `pyarrow` is optional. Without it, dlt loads the data as Python dicts.

`servicenow_source` loads these tables by default:

| Table                                             | Description                                                               |
| ------------------------------------------------- | ------------------------------------------------------------------------- |
| `incident`, `problem`                             | Incidents and problems                                                    |
| `change_request`, `change_task`                   | Changes and their tasks                                                   |
| `sc_request`, `sc_req_item`, `sc_task`            | Service catalog requests, requested items and their tasks                 |
| `task_sla`                                        | SLAs of tasks                                                             |
| `kb_knowledge`                                    | Knowledge articles                                                        |
| `sys_user`, `sys_user_group`, `sys_user_grmember` | Users, groups and group members                                           |
| `core_company`, `cmn_location`, `cmn_department`  | Companies, locations and departments                                      |
| `cmdb_ci`                                         | Configuration items of all classes, with the fields of the base class     |
| `sys_choice`                                      | Labels of choice values, for example the label of `state` 2 of `incident` |

To load other tables, including custom tables, pass `tables` to `servicenow_source` or use
`servicenow_table`.

## Setup guide

### Grab credentials

dlt accesses the API as a ServiceNow user. Create an integration user for dlt:

1. Grant the user read access to the tables that you load.
1. Grant the user read access to `sys_db_object` and `sys_dictionary`. dlt reads the table
   structure from these tables.
1. Grant the user read access to `sys_audit_delete`. dlt reads deleted records from this table.
1. Grant the user access to all records of the loaded tables. dlt skips records that row ACLs hide,
   and each skipped page costs a request.

If the ServiceNow instance uses query ACLs, grant `query_match` and `query_range` access to the
fields that dlt filters on:

- `sys_id`, `sys_updated_on` and `sys_created_on` of the loaded tables, and the cursor field of your
  own incremental
- `sys_id` of `sys_db_object`
- `sys_id`, `name` and `element` of `sys_dictionary`
- `sys_id`, `sys_created_on` and `tablename` of `sys_audit_delete`
- the fields of your encoded queries

The time zone of the user does not change the loaded data. dlt compares date-times in UTC.

dlt supports these authentication methods:

- **OAuth password grant**: in **System OAuth** > **Application Registry**, create an OAuth API
  endpoint for external clients. dlt uses its client id and secret, the user name and the password.
- **OAuth client credentials**: available from the Washington DC release. Set the system property
  `glide.oauth.inbound.client.credential.grant_type.enabled` to `true`. Then set an OAuth
  application user on the entry in the application registry.
- **API key**: available from the Washington DC release with the "API Key and HMAC Authentication"
  plugin (`com.glide.tokenbased_auth`). Create a REST API key for the user. Then create an API
  access policy that allows API keys for the Table API. dlt sends the key in the `x-sn-apikey`
  header.
- **Basic authentication**: the user name and the password. A ServiceNow instance can allow basic
  authentication only for users with the `snc_basic_auth_api_access` role.

### Initialize the verified source

1. Run the following command:

   ```cli
   dlt init servicenow duckdb
   ```

   [This command](../../reference/command-line-interface.md) creates
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/servicenow_pipeline.py)
   with ServiceNow as the [source](../../general-usage/source.md) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations/index.md).

1. To use a different destination, replace `duckdb` with the name of your
   [destination](../destinations/index.md).

The command creates a directory with the files and the config that the pipeline needs.

### Add credentials

In `.dlt/secrets.toml`, set `instance` and the credentials of one authentication method. `instance`
is the name of the ServiceNow instance, for example `dev12345` for `dev12345.service-now.com`. You
can also set the host or the URL.

```toml
[sources.servicenow.credentials]
instance = "dev12345"
# OAuth password grant
client_id = "client_id"
client_secret = "client_secret"
username = "username"
password = "password"
```

```toml
[sources.servicenow.credentials]
instance = "dev12345"
# OAuth client credentials
client_id = "client_id"
client_secret = "client_secret"
```

```toml
[sources.servicenow.credentials]
instance = "dev12345"
api_key = "api_key"
```

```toml
[sources.servicenow.credentials]
instance = "dev12345"
# basic authentication
username = "username"
password = "password"
```

### Run the pipeline

1. Install the dependencies. For the fastest loading, also install `pyarrow`:

   ```cli
   pip install -r requirements.txt
   pip install pyarrow
   ```

1. Run the pipeline:

   ```sh
   python servicenow_pipeline.py
   ```

1. Show the loaded data:

   ```cli
   dlt pipeline servicenow show
   ```

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline.md).

## How data is loaded

The defaults below apply to every resource, from `servicenow_source` and from `servicenow_table`.

### Pages

dlt reads a table in pages of 10000 records, ordered by `sys_id`. Each page starts after the last
`sys_id` of the previous page. A record that changes while dlt reads the table stays on its page, so
dlt does not miss it or read it twice.

When the first page of a table is full, dlt counts the remaining records. If more than one page
remains, dlt splits the remaining `sys_id`s into ranges. The dlt extract workers read the ranges in
parallel. Both methods, page by page and in `sys_id` ranges, load the same records.

### Data types

| ServiceNow type                                               | dlt type                           |
| ------------------------------------------------------------- | ---------------------------------- |
| `boolean`                                                     | `bool`                             |
| `integer`, `longint`                                          | `bigint`                           |
| `decimal`, `currency`, `price`                                | `decimal`                          |
| `float`                                                       | `double`                           |
| `glide_date_time`, `due_date`                                 | `timestamp` with time zone, in UTC |
| `glide_date`                                                  | `date`                             |
| `glide_time`                                                  | `time`                             |
| `glide_duration`, `timer`                                     | `bigint`, the number of seconds    |
| `journal`, `journal_input`, `password`, `password2`           | not loaded                         |
| other types: `string`, `reference`, `choice`, `GUID` and more | `text`                             |

- dlt loads a reference as the `sys_id` of the referenced record.
- dlt loads a choice as its value. The labels of choice values are in `sys_choice`.
- Comments and work notes are records of `sys_journal_field`.
- To change a type, use `apply_hints`.

### Incremental loading and write disposition

dlt picks the cursor and the write disposition of each table from its fields:

| The table has                                                          | Cursor           | Write disposition                                             |
| ---------------------------------------------------------------------- | ---------------- | ------------------------------------------------------------- |
| `sys_updated_on`                                                       | `sys_updated_on` | `merge` on `sys_id`, with [deleted records](#deleted-records) |
| only `sys_created_on`, for example `sys_journal_field` and `sys_audit` | `sys_created_on` | `append`                                                      |
| neither                                                                | none             | `replace`                                                     |

The first run loads all records, or the records changed since `start_date`. Later runs load the
records changed since the last run. With `merge`, each run also loads again the last 10 minutes
before the last cursor value (`lag`). As a result, dlt also loads a record that the ServiceNow
instance saved with an earlier timestamp.

Each run reads the records changed before the time of the ServiceNow instance when the run started.
The next run reads the records that changed during the run. As a result, all pages and `sys_id`
ranges of a run select the same records, whatever the ServiceNow instance writes during the run.
Scripts and imports that write system fields can set `sys_updated_on` to a time in the future. dlt
loads such a record only after the instance clock passes this time, or after the record changes
again. A run with `write_disposition="replace"` and no `start_date` also loads it.

With `write_disposition="replace"`, each run loads all records again, or the records changed since
`start_date`.

### Deleted records

When dlt merges a table, dlt reads the records deleted since the last run from
`sys_audit_delete`. Then dlt deletes them from the destination. These deleted records include the
records of tables that extend the table, for example the servers of `cmdb_ci`.

ServiceNow does not write every deleted record to `sys_audit_delete`:

- ServiceNow writes deleted records only for audited tables. By default, tables with names that
  start with `sys` are not audited, for example `sys_choice`.
- Scripts, cleanup jobs and import sets can delete records without an entry in `sys_audit_delete`.
  The same applies to tables with the `no_audit_delete` dictionary attribute.
- When you restore a deleted record, ServiceNow deletes its entry in `sys_audit_delete`. The
  restored record keeps its old `sys_updated_on`, so only a full reload loads it again.

To delete these records from the destination, reload such tables from time to time with
`write_disposition="replace"`.

### Backends

- `backend="pyarrow"` parses each page into an Arrow table. This backend uses the least CPU.
- `backend="python"` loads lists of dicts and does not need `pyarrow`.
- `backend="auto"` uses `pyarrow` when it is installed.

Both backends load the same values and types.

### CSV export

With `api="csv"`, dlt reads the CSV export of the table (`incident_list.do?CSV`) instead of the
Table API. dlt reads the pages in the same order and with the same encoded queries. As a result,
incremental loading works the same way. dlt still reads the fields, the deleted records and the time
of the ServiceNow instance with the Table API.

- The CSV export needs `pyarrow`. dlt parses CSV pages with less CPU than JSON pages. The ServiceNow
  instance takes about the same time to send both.
- The CSV export accepts basic authentication and OAuth tokens, not API keys. If the ServiceNow
  instance does not accept the credentials, it returns the login page and dlt raises an error.
- The CSV export uses the Windows-1252 encoding by default and does not name its encoding. The
  ServiceNow instance loses characters that Windows-1252 does not have, and dlt logs a warning. To
  export all characters, set the system property `glide.export.csv.charset` to `UTF-8`.
- The CSV export puts an apostrophe before text that starts with `=`, `+`, `-` or `@`
  (`glide.export.escape_formulas`). dlt removes this apostrophe. As a result, text that starts with
  an apostrophe and one of these characters loses its apostrophe.
- An export returns at most 10000 records, the default of `glide.csv.export.limit`. dlt lowers a
  larger `page_size` to 10000 and logs a warning.

## Sources and resources

### Source `servicenow_source`

```py notype
@dlt.source(name="servicenow")
def servicenow_source(
    credentials: ServiceNowAuth = dlt.secrets.value,
    tables: Sequence[str] = DEFAULT_TABLES,
    session: Optional[Session] = None,
    start_date: Optional[TAnyDateTime] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    encoded_queries: Optional[Dict[str, str]] = None,
    include_deleted: bool = True,
    api: TReadApi = "table",
    backend: TTableBackend = "auto",
    page_size: int = 10000,
    allow_partitioned_load: bool = True,
    max_partitions: int = 10,
    defer_table_reflect: bool = False,
) -> Iterable[DltResource]:
    ...
```

`servicenow_source` returns a `servicenow_table` resource for each table in `tables`, with the name
of the table. When you create the source, dlt reads the fields of all tables at once, with one query
of `sys_db_object` and one of `sys_dictionary`. dlt reads the tables in parallel.

- `session`: by default, a session that retries HTTP 429 and 5xx and does not keep cookies. The
  ServiceNow instance runs the requests that share a session cookie one at a time. For this reason,
  a session that you pass must not keep cookies.
- `encoded_queries`: the encoded queries by table name, for example `{"incident": "active=true"}`.
- dlt passes the other arguments to every `servicenow_table`.
- To change the incremental of one table, use `apply_hints` on its resource. The new incremental
  replaces the default one, as with the `incremental` argument of `servicenow_table`.
- `defer_table_reflect`: dlt reads the fields of all tables when it extracts the data, with one
  query of `sys_dictionary`. As a result, dlt makes no API calls when you create the source. If you
  run on Airflow, use this option.

### Resource `servicenow_table`

```py notype
def servicenow_table(
    credentials: ServiceNowAuth = dlt.secrets.value,
    table: str = dlt.config.value,
    session: Optional[Session] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    lag: Optional[float] = None,
    encoded_query: Optional[str] = None,
    include_deleted: bool = True,
    api: TReadApi = "table",
    backend: TTableBackend = "auto",
    page_size: int = 10000,
    allow_partitioned_load: bool = True,
    max_partitions: int = 10,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = None,
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    defer_table_reflect: bool = False,
) -> DltResource:
    ...
```

dlt reads the config of a table from `sources.servicenow.<table>`, for example
`sources.servicenow.incident.start_date`.

- `session`: the requests session, as in `servicenow_source`.
- `start_date`: the first run loads the records changed since this date. Without `start_date`, the
  first run loads all records. A date without a time zone is in the time zone of the dlt context
  (`DLT_INTERVAL_TIMEZONE`), UTC by default.
- `incremental`: your own `dlt.sources.incremental`, for example with another cursor field.
  - The incremental replaces the default one. dlt uses its `lag`, none by default, and deduplicates
    the records that have the last cursor value. An incremental that you set with `apply_hints`
    works the same way.
  - `last_value_func` can be `max` or `min`. With `min`, the incremental goes back in time and dlt
    does not read deleted records.
  - The cursor field can be a date-time, a date, a number or text. dlt sends the cursor range to
    the ServiceNow instance in the encoded query. The ServiceNow instance ignores case when it
    compares text. Python does not. For other field types, dlt reads all records and the incremental
    filters them.
  - For a date-time cursor, a range without an upper end ends at the time of the ServiceNow
    instance when the run starts. The next run reads the records that changed during the run.
  - The type of `initial_value` and `end_value` must match the cursor field, for example a
    `datetime` for a date-time field. dlt does not support `lag` on decimal fields.
  - With `on_cursor_value_missing="include"`, the first run also reads the records without a
    cursor value. Later runs do not read them again. With `"exclude"`, dlt does not read them.
  - Pass `start_date` or `incremental`, not both.
- `lag`: the seconds before the last cursor value that the default incremental loads again on each
  run, 600 for `merge`. With `lag`, dlt also loads records that the ServiceNow instance saved with
  an earlier timestamp.
- `encoded_query`: an encoded query that filters the records, for example `active=true^priority=1`.
  An encoded query cannot contain `ORDERBY`.

  :::caution
  Before you load a table, run its encoded query in a list view. ServiceNow ignores the parts of an
  encoded query that are not valid. An example is a condition on a field that does not exist. As a
  result, a typo can load all records of the table. If the system property
  `glide.invalid_query.returns_no_rows` is `true`, the ServiceNow instance returns no records
  instead. See the
  [GlideRecord API](https://www.servicenow.com/docs/r/zurich/api-reference/server-api-reference/c_GlideRecordAPI.html).
  :::

- `include_deleted`: when dlt merges on `sys_updated_on` with a `max` incremental, dlt deletes the
  records deleted in ServiceNow.
- `api`: `"table"` reads the Table API. `"csv"` reads the [CSV export](#csv-export).
- `backend`: see [Backends](#backends).
- `page_size`: the maximum number of records in a request.
  - The CSV export returns at most 10000 records. With `api="csv"`, dlt lowers a larger page size to
    10000 and logs a warning.
  - Requests that take more time than a transaction quota of the ServiceNow instance get a part of
    the page and an error. The Table API quota is 60 seconds by default. dlt then reads the page
    again with half the records, down to 100.
  - dlt retries HTTP 429, with the time in `Retry-After`, and HTTP 5xx.
- `allow_partitioned_load`: reads the records after a full first page in `sys_id` ranges, in
  parallel. dlt does not use ranges with `row_order` or `add_limit`. With `False`, dlt reads the
  pages one after another.
- `max_partitions`: the maximum number of `sys_id` ranges of a table.
- `included_columns`, `excluded_columns`: the fields to load or to skip.
  - With `included_columns`, dlt also loads `sys_id`, the cursor field, the primary key and the
    merge key. As a result, the cursor and the write disposition stay the same.
  - `excluded_columns` can skip these fields too. Without the cursor field, dlt raises an error.
  - See [Performance](#performance-and-troubleshooting).
- `defer_table_reflect`: dlt reads the fields of the table when it extracts the data. As a result,
  dlt makes no API calls when you create the resource. dlt then sets the column hints when it
  extracts the data. The default incremental expects `sys_updated_on`, which nearly all tables have.

## Customization

### Create your own pipeline

1. Configure the pipeline. Give the pipeline name, the destination and the dataset name:

   ```py notype
   pipeline = dlt.pipeline(
       pipeline_name="servicenow",
       destination="duckdb",
       dataset_name="servicenow_data",
   )
   ```

1. To load selected tables with encoded queries, pass `tables` and `encoded_queries`:

   ```py notype
   source = servicenow_source(
       tables=["incident", "change_request", "sys_user"],
       encoded_queries={"incident": "active=true", "change_request": "active=true"},
       start_date="2024-01-01",
   )
   print(pipeline.run(source))
   ```

1. To load a table with selected fields, use `servicenow_table`:

   ```py notype
   incident = servicenow_table(
       table="incident",
       included_columns=["number", "short_description", "state", "priority"],
   )
   print(pipeline.run(incident))
   ```

1. To load all records of a table again, use `write_disposition="replace"`:

   ```py notype
   print(pipeline.run(servicenow_table(table="cmdb_ci", write_disposition="replace")))
   ```

### Load in chunks

To load a large table over several runs, limit the pages of each run with `add_limit`. Then order
the records by the cursor with `row_order="asc"`. As a result, each run continues where the
previous run stopped. When a run loads no records, stop the loop:

```py notype
incident = servicenow_table(
    table="incident",
    page_size=10000,
    incremental=dlt.sources.incremental("sys_updated_on", row_order="asc"),
)
while True:
    pipeline.run(incident.add_limit(1))
    if pipeline.last_trace.last_normalize_info.row_counts.get("incident", 0) == 0:
        break
```

- Use `add_limit` only with `row_order`. Without `row_order`, dlt reads the pages in `sys_id`
  order. Then the next run skips the unread records that have a lower cursor value.
- With `last_value_func=min`, the chunks go back in time, with the newest records first.
- The next run starts at the last cursor value. dlt does not load the records with this value
  again.
- The page size must be larger than the number of records that share a timestamp. Otherwise the
  cursor cannot move. A bulk import or a mass update can give thousands of records the same
  timestamp.
- In a large table, the order by the cursor needs an index on the cursor field.
- dlt reads the chunks one page after another, not in `sys_id` ranges.
- dlt reads deleted records after all pages. A run that `add_limit` stops does not read them. After
  the last chunk, run without `add_limit`.
- If all records of a chunk have the cursor value that the run started from, dlt logs a warning.
- The ServiceNow instance ignores case when it orders text. For this reason, dlt cannot read chunks
  ordered by a text cursor.
- Your incremental does not have `lag`. After the last chunk, add `lag` to it.

### Load rotated tables

ServiceNow splits rotated tables, for example `syslog` and `sys_email`, into shards by
`sys_created_on`. To read only the latest shards, use an incremental on `sys_created_on` and the
`append` write disposition:

```py notype
syslog = servicenow_table(
    table="syslog",
    incremental=dlt.sources.incremental("sys_created_on"),
    write_disposition="append",
)
```

## Performance and troubleshooting

### Throughput

The ServiceNow instance, not dlt, sets the speed. ServiceNow checks the field ACLs of every value
that it returns. As a result, wide tables load slowly.

On a ServiceNow developer instance, a page of 10000 records with 59 fields took about 38 seconds.
With 12 fields, a page took about 6.5 seconds. To load wide tables faster, load only the fields that
you need with `included_columns`.

dlt reads 5 resources or `sys_id` ranges at a time by default. To read more at a time, set more
workers in `config.toml`:

```toml
[extract]
workers = 10
```

On a ServiceNow developer instance, 12 tables with 97000 records loaded in 35 seconds with 5
workers. With 10 workers, they loaded in 22 seconds. One table with 152000 records loaded in 162
seconds page by page, in 58 seconds in `sys_id` ranges, and in 34 seconds with 10 workers.

A node of the ServiceNow instance runs about 4 API requests at a time and queues the others. When
the queue is full, the ServiceNow instance returns HTTP 429 and dlt retries the request.

dlt logs each page at the `INFO` level with the time that the ServiceNow instance took, for example
``Read 10000 records of `incident` in 6.5s``.

### Fields and records that the user cannot read

- If the user cannot read a field, dlt loads the field as null. These nulls replace the values that
  dlt loaded before. dlt logs a warning with the names of these fields. To load these fields, grant
  the user read access to them. To skip them, set `excluded_columns` of
  [`servicenow_table`](#resource-servicenow_table).
- If the user cannot read the cursor field, dlt raises an error. Without the cursor values,
  incremental loading cannot continue.
- ServiceNow applies row ACLs after it limits a page. As a result, a page of hidden records is
  empty. dlt counts the remaining records and skips the page, which costs a request.

### Query ACLs

When a query filters on a field without `query_match` or `query_range` access, the Table API
returns HTTP 403. The CSV export returns no records for the same query. dlt raises an error in both
cases. To correct the error, grant `query_match` and `query_range` access to the fields that the
error names.

### Limitations

- dlt does not load display values, the labels of references and choices. To get the labels, join
  `sys_choice` and the referenced tables.
- dlt does not load database views. Load the tables that a view joins.
- `task` contains the records of all tables that extend it, for example incidents and changes. Load
  `task` or these tables, not both.
