---
title: SharePoint
description: dlt verified source that loads SharePoint lists and document library files with Microsoft Graph
keywords: [sharepoint, microsoft graph, sharepoint lists, document library, microsoft 365, excel, onedrive]
---
# SharePoint

import Header from './_source-info-header.md';

<Header/>

[SharePoint](https://www.microsoft.com/microsoft-365/sharepoint/collaboration) is the document and
collaboration platform of Microsoft 365. A SharePoint site keeps structured data in lists and files
in document libraries.

This SharePoint `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sharepoint_pipeline.py)
load lists and files of one site with [Microsoft Graph](https://learn.microsoft.com/graph/api/resources/sharepoint)
to the destination of your choice:

- **Lists**: dlt loads the items of a list into a table named after the list, one column per list
  column.
- **Incremental lists**: the first run reads the whole list with the Graph
  [delta query](https://learn.microsoft.com/graph/api/listitem-delta). Later runs read only the
  items that were added, changed or deleted. dlt merges them on `id`.
- **Files**: dlt reads the rows of csv, jsonl, parquet and Excel files of a document library folder
  into one table. Each row has the URL and the name of its file. Each run reads only new or
  modified files.
- **Your own readers**: the folder listing is dlt's
  [filesystem resource](filesystem/index.md), so dlt's readers and your own transformers work on it.
  The `read_files` transformer also reads files from any other filesystem, for example a bucket.

The sources and resources that this verified source supports are:

| Name                   | Description                                                           |
| ---------------------- | --------------------------------------------------------------------- |
| `sharepoint_lists`     | A source with one `sharepoint_list` resource per list of the site     |
| `sharepoint_list`      | A resource that loads one list, named after the list                  |
| `sharepoint_files`     | A source that loads the rows of the files of a folder into one table  |
| `sharepoint_folder`    | A resource that lists the new or modified files of a folder           |
| `read_files`           | A transformer that reads csv, jsonl, parquet or Excel files into rows |
| `to_decimal`           | A converter for `reader_kwargs` that loads amounts as decimals        |
| `SharepointFileSystem` | A read-only fsspec filesystem over a document library                 |

## Setup guide

### Grab credentials

dlt accesses Graph as a Microsoft Entra ID application, without a user. Register the application.
Then grant it read access to the site:

1. Sign in to the [Microsoft Entra admin center](https://entra.microsoft.com). Go to
   **Identity** > **Applications** > **App registrations** > **New registration**. Give the
   application a name, for example `dlt`. Leave the redirect URI empty.
1. On the **Overview** page of the application, copy the **Application (client) ID** and the
   **Directory (tenant) ID**.
1. Go to **Certificates & secrets** > **New client secret**. Copy the **Value** of the secret
   immediately. Entra ID does not show it again.
1. Go to **API permissions** > **Add a permission** > **Microsoft Graph** >
   **Application permissions**. Add `Sites.Read.All` or `Sites.Selected`. `Sites.Read.All` gives
   read access to every site of the tenant. `Sites.Selected` gives read access only to the sites
   that an administrator grants to the application.
1. Click **Grant admin consent** for the tenant. Without the consent, Graph answers every request
   with HTTP 403.
1. If you added `Sites.Selected`, grant the application access to the site with the
   [permissions API](https://learn.microsoft.com/graph/permissions-selected-overview).

Then find the id of the site. Sign in to the
[Graph Explorer](https://developer.microsoft.com/graph/graph-explorer). Run this request with the
host of your tenant and the path of the site:

```text
GET https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/finance?$select=id
```

The id has three parts: `contoso.sharepoint.com,<site-collection-id>,<web-id>`. For the root site
of the tenant, request `/sites/contoso.sharepoint.com?$select=id`.

### Initialize the verified source

1. Run the following command:

   ```cli
   dlt init sharepoint duckdb
   ```

   [This command](../../reference/command-line-interface.md) creates
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sharepoint_pipeline.py)
   with SharePoint as the [source](../../general-usage/source.md) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations/index.md).

1. To use a different destination, replace `duckdb` with the name of your
   [destination](../destinations/index.md).

The command creates a directory with the files and the config that the pipeline needs.

### Add credentials

In `.dlt/secrets.toml`, set the ids and the secret of the application and the id of the site:

```toml
[sources.sharepoint.credentials]
tenant_id = "tenant_id"
client_id = "client_id"
client_secret = "client_secret"
site_id = "contoso.sharepoint.com,site-collection-id,web-id"
```

### Run the pipeline

1. Install the dependencies:

   ```cli
   pip install -r requirements.txt
   ```

1. Run the pipeline:

   ```sh
   python sharepoint_pipeline.py
   ```

   The pipeline example loads the lists of the generic template, see
   [`sharepoint_lists`](#source-sharepoint_lists). `load_list_columns` and `load_files` load one
   list and the files of one folder. To run them, set a list and a folder of your site. Then
   uncomment the calls at the end of the script.

1. Show the loaded data:

   ```cli
   dlt pipeline sharepoint show
   ```

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline.md).

## How data is loaded

### Lists

dlt reads the items of a list with the Graph delta query. The first run reads all items. dlt keeps
the delta link in the pipeline state. Later runs continue from this link. They read only the items
that were added, changed or deleted after the previous run. dlt merges the items on `id`, the id of
the item in the list.

- A row has a column for each column that Graph returns in the `fields` of an item. These are the
  columns of the list and the system columns, for example `Title`, `Created`, `Modified`,
  `AuthorLookupId` and `ContentType`. dlt removes the `@odata` annotations. `included_columns`
  selects the columns.
- Column names are the internal names of the list columns. SharePoint removes spaces from the name
  that you type, so a column named "Story Points" is `StoryPoints`. To see the internal names,
  request `/sites/{site-id}/lists/{list-title}/columns` in the Graph Explorer.
- A lookup or person column loads as `<column>LookupId`, the id of the referenced item.
- Number columns load as `double`. A date-only column loads as midnight in the time zone of the
  site, converted to UTC, for example `2024-04-01T07:00:00Z` for a site in Pacific time.
- A multi-value column, for example a multi-choice column, becomes a
  [nested table](../../general-usage/destination-tables.md#nested-tables). To keep it as JSON, set
  `max_table_nesting = 0` on the source.
- Graph can send the same item more than once in one run. dlt keeps the last occurrence. To find
  it, dlt adds `_dlt_list_idx`, the position of the record in the run, with the `dedup_sort` hint.

When an item is deleted in SharePoint, Graph sends only its id. By default, dlt keeps the record
with `_deleted` set to `true` and the other columns empty. With `hard_delete=True` on
`sharepoint_lists`, dlt deletes the record from the destination. For a single `sharepoint_list`,
set the `hard_delete` hint on the `_deleted` column:

```py notype
tasks = sharepoint_list("Tasks").apply_hints(columns={"_deleted": {"hard_delete": True}})
```

A delta link is valid for one list and one set of columns. When you change `included_columns`, the
next run reads the whole list again and merges it into the same table. The records of items that
were deleted since the previous run stay in the table. To drop the table and read the list again,
run the pipeline with `refresh="drop_resources"`. If Graph cannot continue from the stored delta
link (HTTP 410), dlt raises an error that asks for this refresh. This occurs, for example, after a
long time without a run.

### Files

`sharepoint_folder` lists the files of a folder of a document library with dlt's
[filesystem resource](filesystem/index.md). It yields the files in the folder that match
`file_glob`. With `**` in the glob, it also yields the files in the subfolders. dlt lists a OneNote
notebook like a folder, so `**` also finds its section files.

The listing is incremental on `modification_date`, the time of the last change in SharePoint. Each
run yields only the files modified since the last run. With `start_date`, the first run loads only
the files modified since this date. An `incremental` that you pass replaces the default one, for
example to load a window of time.

`read_files` reads the rows of each file and adds three columns:

| Column                  | Description                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------- |
| `_dlt_file_url`         | The `sharepoint://<library>/<path>` URL of the file. The library is empty for the default library. |
| `_dlt_file_name`        | The name of the file                                                                               |
| `_dlt_file_modified_at` | The time of the last change, in UTC                                                                |

`_dlt_file_url` is the `merge_key` of the table. As a result, the records of a modified file
replace all of its old records. A renamed file counts as a new file. The records of a file that was
deleted in SharePoint stay in the table. dlt skips blank files, that is, empty files or files with
only whitespace. As a result, the records of a file that becomes blank also stay.

`sharepoint_files` pipes `sharepoint_folder` into `read_files` and loads all matching files into
`table_name`. A file with other columns adds these columns to the table. The file listing is the
`<table_name>_files` resource of the source. dlt does not select it by default. To also load the
name, path, size and modification time of each file, select it:

```py notype
reports = sharepoint_files("Reports", "csv", table_name="reports")
reports.resources["reports_files"].selected = True
```

#### File types

- dlt reads `csv` and `excel` files with pandas. `reader_kwargs` are the arguments of `pandas.read_csv`
  and `pandas.read_excel`, for example `sep`, `encoding`, `header`, `dtype`, `converters` or
  `sheet_name`. pandas infers the types, so an amount like `10.50` loads as `double`. To load it
  as `decimal`, pass the `to_decimal` converter:

  ```py notype
  reports = sharepoint_files(
      "Reports", "csv", table_name="reports", reader_kwargs={"converters": {"amount": to_decimal}}
  )
  ```

  `to_decimal` removes `$` and `,` from text. It keeps the 15 significant digits that Excel shows.
  It loads empty cells and text that is not a number as `null`.
- An Excel workbook loads one table per sheet, named `<table_name>_<sheet>`. `sheet_name` in
  `reader_kwargs` selects sheets by name or position. Excel dates load as `timestamp`. Excel
  durations load as a number of seconds. `.xlsx` files need `openpyxl`, which `requirements.txt`
  installs. `.xls` files need `xlrd`.
- `jsonl` keeps nested objects as JSON. dlt uses only `encoding` from `reader_kwargs`.
- dlt reads `parquet` files with pyarrow. `columns` in `reader_kwargs` selects the columns.

Graph does not send the encoding of a file. dlt selects the encoding of a csv or jsonl file in this
order:

1. The byte order mark of the file (UTF-8, UTF-16 or UTF-32).
1. `encoding` in `reader_kwargs`.
1. UTF-8.

If Excel saved a csv file in a Windows code page, set `encoding`, for example to `cp1252`.

#### Downloads

dlt downloads each file of up to 16 MiB in one piece on its extract workers, 5 at a time by
default. The main thread parses the downloaded files. dlt reads larger files in ranges while it
parses them. To download more files at a time, set more workers in `config.toml`:

```toml
[extract]
workers = 10
```

## Sources and resources

### Source `sharepoint_lists`

```py notype
@dlt.source(name="sharepoint_lists")
def sharepoint_lists(
    list_titles: Optional[List[str]] = None,
    hard_delete: bool = False,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> Iterable[DltResource]:
    ...
```

`sharepoint_lists` returns a `sharepoint_list` resource for each list, named after the list.

- `list_titles`: the display names of the lists to load. Without it, dlt reads the lists of the
  site when you create the source. It loads every visible list of the generic template, that is,
  the lists made with **Blank list** or **From CSV**. Lists of other templates, for example tasks or
  contacts, load only when you name them. A list that you add in SharePoint becomes a new table on
  the next run. To keep a fixed set of tables, pass `list_titles`.
- `hard_delete`: deletes the records of deleted items from the destination. Without it, dlt keeps
  them with `_deleted` set to `true`.

If two list titles normalize to the same table name, for example "Sales Report" and "Sales-Report",
both lists load into one table. Item ids start at 1 in every list, so the records of the two lists
overwrite each other. To keep both, load one list with `sharepoint_list` and rename it with
`with_name`.

### Resource `sharepoint_list`

```py notype
@dlt.resource(name=lambda args: args["list_title"], write_disposition="merge", primary_key="id")
def sharepoint_list(
    list_title: str = dlt.config.value,
    included_columns: Optional[List[str]] = None,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> Iterator[TDataItems]:
    ...
```

The resource and its table are named after the list. To load the list into another table, rename
the resource with `with_name`. dlt reads the config of a list from `sources.sharepoint.<list title>`.

- `list_title`: the display name of the list, as shown in SharePoint.
- `included_columns`: the internal names of the columns to load. Without it, dlt loads the columns
  that Graph returns by default. Graph returns up to 12 lookup columns per request.

### Source `sharepoint_files`

```py notype
@dlt.source(name="sharepoint_files", max_table_nesting=0)
def sharepoint_files(
    folder_path: str = dlt.config.value,
    file_type: TFileType = dlt.config.value,
    table_name: str = dlt.config.value,
    library: Optional[str] = None,
    file_glob: str = "*",
    reader_kwargs: Optional[Dict[str, Any]] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> Iterable[DltResource]:
    ...
```

`sharepoint_files` returns two resources: `<table_name>`, which pipes `sharepoint_folder` into
`read_files`, and the unselected `<table_name>_files` listing. dlt keeps the incremental state per
`table_name`. When you change the folder or the glob, run the pipeline with
`refresh="drop_resources"`. Several `sharepoint_files` sources in one pipeline share one schema, so
`refresh="drop_sources"` on one drops the tables of the others. Give each its own name, for example
`sharepoint_files.with_args(name="reports")(...)`.

- `folder_path`: the folder relative to the root of the library, for example `Reports/2024`. An
  empty string loads the root of the library. The path cannot contain glob characters.
- `file_type`: `"csv"`, `"jsonl"`, `"parquet"` or `"excel"`.
- `table_name`: the destination table. For Excel files, the prefix of the sheet tables,
  `<table_name>_<sheet>`.
- `library`: the display name of the document library, for example `Documents`. Without it, dlt
  reads the default library of the site. System libraries, for example `Site Assets`, cannot be
  named.
- `file_glob`: the glob that the paths under the folder must match, `*` by default. `**/*.csv`
  also searches subfolders.
- `reader_kwargs`: the arguments of the reader, see [File types](#file-types). Converters are
  functions, so pass them in code, not in config.
- `start_date`: the first run loads the files modified since this date. Without `start_date`, the
  first run loads all files. A date without a time zone is in UTC.
- `incremental`: your own `dlt.sources.incremental` on `modification_date` instead of the default
  one, for example with an `end_value`. An `incremental` with an `end_value` stores no state, so
  you can backfill with it. Pass `start_date` or `incremental`, not both.

### Resource `sharepoint_folder`

```py notype
def sharepoint_folder(
    folder_path: str = dlt.config.value,
    library: Optional[str] = None,
    file_glob: str = "*",
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> DltResource:
    ...
```

`sharepoint_folder` returns dlt's [filesystem resource](filesystem/index.md) over the folder, with
the arguments of `sharepoint_files`. It yields `FileItem`s, so dlt's readers `read_csv`,
`read_jsonl` and `read_parquet` and your own transformers can follow it. dlt reads its config from
`sources.sharepoint.sharepoint_folder`.

### Transformer `read_files`

```py notype
@dlt.transformer(write_disposition="merge", merge_key="_dlt_file_url")
def read_files(
    items: Iterable[FileItemDict],
    file_type: TFileType = dlt.config.value,
    reader_kwargs: Optional[Dict[str, Any]] = None,
) -> Iterator[TDataItems]:
    ...
```

`read_files` reads the files that `sharepoint_folder` or any other listing of `FileItem`s yields,
for example dlt's `filesystem` resource over a bucket or a local folder. Rename the table with
`with_name` or a `table_name` hint.

## Customization

### Create your own pipeline

1. Configure the pipeline. Give the pipeline name, the destination and the dataset name:

   ```py notype
   pipeline = dlt.pipeline(
       pipeline_name="sharepoint",
       destination="duckdb",
       dataset_name="sharepoint_data",
   )
   ```

1. To load selected lists and delete the rows of deleted items, pass `list_titles` and
   `hard_delete`:

   ```py notype
   source = sharepoint_lists(list_titles=["Tasks", "Projects"], hard_delete=True)
   print(pipeline.run(source))
   ```

1. To load selected columns of one list into a table of your own name, use `sharepoint_list`:

   ```py notype
   tasks = sharepoint_list("Tasks", included_columns=["Title", "Status", "DueDate"])
   print(pipeline.run(tasks.with_name("task_status")))
   ```

1. To load the csv files of a folder, including its subfolders, from a date on:

   ```py notype
   reports = sharepoint_files(
       "Reports",
       "csv",
       table_name="reports",
       library="Finance",
       file_glob="**/*.csv",
       start_date="2024-01-01",
   )
   print(pipeline.run(reports))
   ```

1. To load one sheet of the Excel workbooks of a folder into `budgets_data`:

   ```py notype
   budgets = sharepoint_files(
       "Budgets", "excel", table_name="budgets", reader_kwargs={"sheet_name": "Data"}
   )
   print(pipeline.run(budgets))
   ```

### Backfill a window of files

An `incremental` with an `end_value` loads the files modified in a window and stores no state. Run
one window after another to backfill a large folder:

```py notype
from datetime import datetime, timezone

window = dlt.sources.incremental(
    "modification_date",
    initial_value=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_value=datetime(2024, 2, 1, tzinfo=timezone.utc),
    range_start="closed",
    range_end="open",
)
print(pipeline.run(sharepoint_files("Reports", "csv", table_name="reports", incremental=window)))
```

The listing is in folder order, not in the order of `modification_date`. For this reason, chunked
loads with `add_limit` and `row_order` do not work.

### Use dlt's readers or your own

`sharepoint_folder` yields `FileItem`s like dlt's filesystem resource, so the
[readers of the filesystem source](filesystem/index.md) work on it. They do not add the file
columns, so the records of a modified file do not replace its old records:

```py notype
from dlt.sources.filesystem import read_csv

rows = sharepoint_folder("Reports", file_glob="*.csv") | read_csv()
print(pipeline.run(rows.with_name("reports")))
```

`read_files` works on any listing of `FileItem`s, for example dlt's filesystem resource over a
bucket:

```py notype
from dlt.sources.filesystem import filesystem

files = filesystem(bucket_url="s3://bucket/reports", file_glob="*.xlsx")
print(pipeline.run(files | read_files("excel").with_name("reports")))
```

### Read files with pandas

`SharepointFileSystem` is registered as the `sharepoint` protocol of fsspec, so pandas and other
fsspec clients can read files of a library directly:

```py notype
import pandas as pd
import sharepoint  # registers the protocol

df = pd.read_csv(
    "sharepoint://Documents/Reports/report_2024_01.csv",
    storage_options={
        "tenant_id": "...",
        "client_id": "...",
        "client_secret": "...",
        "site_id": "...",
    },
)
```

The URL is `sharepoint://<library>/<path>`. For the default library of the site, leave the library
empty: `sharepoint:///Reports/report_2024_01.csv`.

## Performance and troubleshooting

### Throttling

SharePoint limits the resource units per minute that an application can use in a tenant. The limit
depends on the number of licenses of the tenant. If an application uses more units, Graph answers
with HTTP 429 or 503 and a `Retry-After` header. dlt then retries the request after this time.

dlt sends its name in the `User-Agent` header. SharePoint gives named traffic priority over unnamed
traffic. See
[avoid getting throttled](https://learn.microsoft.com/sharepoint/dev/general-development/how-to-avoid-getting-throttled-or-blocked-in-sharepoint-online).

A file costs two units: one request for its metadata and one for its content. A page of a folder
listing or a delta query costs one or two units. For a folder with thousands of small files, the
unit limit sets the speed, not the size of the files.

### Tokens

dlt requests an access token for the application on the first call. The token expires after about
one hour, and dlt renews it before that time. All resources of a pipeline share one token.

### Limitations

- Files deleted in SharePoint keep their records in the destination. To delete these records, run
  the pipeline with `refresh="drop_resources"`. dlt then loads all files of the folder again.
- The time of the last change of a file is the time that the client reported. The OneDrive sync
  client and migration tools keep the local times of a file. Such a file can be older than the last
  `modification_date` that dlt stored. dlt loads it only in a run with `refresh="drop_resources"`.
- dlt finds a list by its display name. If you rename the list in SharePoint, pass the new name.
  The next run then reads the whole list again.
- dlt does not load attachments, list versions or site pages.
