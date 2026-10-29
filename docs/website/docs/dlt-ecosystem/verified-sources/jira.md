---
title: Jira
description: dlt verified source for Atlassian Jira
keywords: [jira api, jira verified source, jira, jql, jira issues, incremental loading]
---
# Jira

import Header from './_source-info-header.md';

<Header/>

[Jira](https://www.atlassian.com/software/jira) by Atlassian helps teams manage projects and tasks
efficiently, prioritize work, and collaborate.

This Jira `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira_pipeline.py)
load data from the Jira Cloud REST API to the destination of your choice. `dlt` loads issues, their
comments and changelogs, and worklogs incrementally and merges them by default. `dlt` replaces the
tables of the other resources on each run.

The resources that this verified source supports are:

| Name                | Write disposition | Description                                                                    |
| ------------------- | ----------------- | ------------------------------------------------------------------------------ |
| `issues`            | merge on `id`     | Issues that match your JQL query, one column for each field                    |
| `issue_comments`    | merge on `id`     | All comments of the loaded issues                                              |
| `issue_changelogs`  | merge on `id`     | The whole change history of the loaded issues                                  |
| `worklogs`          | merge on `id`     | Worklogs of all issues of the site. `dlt` deletes the worklogs deleted in Jira |
| `issue_fields`      | replace           | Names and schemas of system and custom fields                                  |
| `users`             | replace           | Active, inactive and deleted users of the site                                 |
| `workflows`         | replace           | Workflows and the statuses they use                                            |
| `workflow_statuses` | replace           | Names and categories of the statuses used by workflows                         |
| `projects`          | replace           | Projects with their issue types, leads and issue counts                        |

[jira/settings.py](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira/settings.py)
lists the endpoints of the replaced resources. `dlt` authenticates as the owner of the API token and
loads what this user can see.

## Setup guide

### Grab credentials

1. Log in to your Jira account.

1. Click on your profile picture in the top right corner, and choose "Manage account."

1. Go to the "Security" tab and select "Create and manage API tokens."

1. Click "Create API token," provide a descriptive name, and click the "Create" button.

1. Safely copy the newly generated API token.

> Note: The Jira UI, which is described here, might change.
The full guide is available at [this link.](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```cli
   dlt init jira duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira_pipeline.py)
   with Jira as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as
   the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

### Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

   Here's what the file looks like:

   ```toml
   # put your secret values and credentials here. Please do not share this file, and do not push it to GitHub
   [sources.jira]
   subdomain = "set me up!" # please set me up!
   email = "set me up!" # please set me up!
   api_token = "set me up!" # please set me up!
   ```

1. A subdomain in a URL identifies your Jira account. For example, in "[https://example.atlassian.net](https://example.atlassian.net)", "example" is the subdomain.

1. Use the email address associated with your Jira account.

1. Replace the "api_token" value with the [previously copied one](jira.md#grab-credentials) to ensure secure access to your Jira account.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to add credentials for your chosen destination, ensuring proper routing of your data to the final destination.

For more information, read [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:

   ```cli
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python jira_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:

   ```cli
   dlt pipeline <pipeline_name> show
   ```

   The pipeline example runs two pipelines: `jira_pipeline` loads users, projects and workflows,
   and `jira_issues_pipeline` loads issues and their comments. You can also use any custom name
   instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Source `jira`

This source function returns all resources of the table above.

```py
from typing import Any, Iterable, Optional
from dlt.common.schema.typing import TWriteDispositionConfig
from dlt.common.typing import TAnyDateTime
from dlt.extract import DltResource

@dlt.source(max_table_nesting=2)
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: Optional[int] = None,
    jql: Optional[str] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = "merge",
) -> Iterable[DltResource]:
    ...
```

- `subdomain`: The subdomain of the Jira account. Configured in ".dlt/secrets.toml".
- `email`: The email associated with the Jira account. Configured in ".dlt/secrets.toml".
- `api_token`: The API token for accessing the Jira account. Configured in ".dlt/secrets.toml".
- `page_size`: Rows per request of users, workflows and projects. Defaults to the largest page each endpoint serves.
- `jql`: A JQL query that selects the issues to load, for example `project = KAN`. It must not contain `ORDER BY`.
- `start_date`: Loads issues and worklogs updated since this date on the first run. If not set, the first run loads all of them.
- `incremental`: Your own incremental for issues instead of the default one, see [`jira_issues`](#resource-issues).
- `write_disposition`: The write disposition of issues, comments, changelogs and worklogs, `merge` by default. See [Replace all issues on each run](#replace-all-issues-on-each-run).

`issues` loads [incrementally](../../general-usage/incremental-loading.md) on the `updated` field:

- The first run loads all issues that match `jql`, or the issues updated since `start_date`.
- The next runs load the issues updated since the previous run. Jira search is eventually
  consistent. For this reason, each run reads the last 10 minutes of the previous run again with a
  [lag](../../general-usage/incremental/lag.md).
- `dlt` [merges](../../general-usage/merge-loading.md) issues on `id`, so each issue keeps its
  latest version.

`dlt` loads the comments and changelogs of the issues in the same run as the issues. `worklogs`
reads the worklog feeds of the whole site. `jql` does not restrict them.

### Resource `issues`

`jira_issues` returns the `issues` resource. With it, you can load issues without the other
resources. You can also load several tables of issues with different JQL queries.

```py
from typing import Any, List, Optional
from dlt.common.schema.typing import TWriteDispositionConfig
from dlt.common.typing import TAnyDateTime, TColumnNames
from dlt.extract import DltResource

def jira_issues(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    jql: Optional[str] = None,
    start_date: Optional[TAnyDateTime] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    lag: Optional[float] = None,
    chunk_size: Optional[int] = None,
    bulk_fetch_size: Optional[int] = None,
    search_page_size: int = 5000,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
    write_disposition: Optional[TWriteDispositionConfig] = "merge",
    primary_key: TColumnNames = None,
    merge_key: TColumnNames = None,
    name: str = "issues",
) -> DltResource:
    ...
```

- `jql`, `start_date`: As in the [`jira` source](#source-jira).
- `incremental`: Your own `dlt.sources.incremental` instead of the default one. Its cursor must be a
  date or datetime field, for example `created` or the id of a custom date field. `dlt` adds the
  `initial_value` and `end_value` of this incremental to the JQL query. You can use them to
  [backfill](../../general-usage/incremental/cursor.md#using-end_value-for-backfill) a date range.
- `lag`: Seconds that each run reads again, 600 by default.
- `chunk_size`: Rows in a page of issues, comments and changelogs, 1000 by default.
- `bulk_fetch_size`: Issues fetched in a single request, at most 1000.
- `search_page_size`: Issue ids listed in a single search request, at most 5000.
- `included_columns`: Field ids to load, all fields if not set.
- `excluded_columns`: Field ids to skip.
- `write_disposition`, `primary_key`, `merge_key`: `merge` on `id` by default.
- `name`: The name of the resource and of its table.

The [extract workers](../../reference/performance.md#extract) of the pipeline fetch the issues in
parallel.

The `issues` table has a column for each field, named by the field id:

- System fields keep their names, for example `summary`, `duedate` or `status__name`. Custom fields
  use their ids as names, for example `customfield_10016`. The `issue_fields` table holds the names
  of the fields.
- `dlt` converts each value to the type of its field. If `dlt` cannot convert a value, the value
  loads as `NULL` and `dlt` logs a warning.
- Objects and arrays load as Jira returns them, for example `status__name` or the
  [nested table](../../general-usage/destination-tables.md#nested-tables) `issues__labels`. The
  sources load at most two levels of nested tables. `jira_issues` used without a source has no
  nesting limit. To keep two levels, you can set `issues.max_table_nesting = 2`.
- Fields of unknown structure, for example fields added by apps, load as JSON text. Empty values do
  not create columns.
- Rich text fields, for example `description`, are in wiki markup.
- Comments and worklogs are not columns of `issues`. The `issue_comments` and `worklogs` resources
  load them.

A field gets a column when `dlt` loads the first issue with a value for this field. Fields in `included_columns` get
typed columns on the first run, also before they have values.

### Resources `issue_comments` and `issue_changelogs`

These [transformers](../../general-usage/resource.md#process-resources-with-dlttransformer) load
the comments and changelogs of the issues piped into them:

```py notype
issues = jira_issues(jql="project = KAN")
pipeline.run([issues, issues | issue_comments, issues | issue_changelogs])
```

- `issue_comments` loads all comments of each loaded issue. These comments replace the comments of
  the issue that `dlt` loaded before. `dlt` updates edited comments and deletes the comments deleted
  in Jira. The merge key is `issue_id`.
- `issue_changelogs` loads the whole changelog of each loaded issue, because Jira cannot filter
  changelogs by date. The changed fields are in the nested table `issue_changelogs__items`.

A transformer runs its parent: `jira().with_resources("issue_comments")` loads only comments, but it
still searches and fetches the issues.

### Resource `worklogs`

```py
from datetime import datetime
from typing import Iterator
from dlt.common.typing import TDataItem

@dlt.resource(name="worklogs", write_disposition="merge", primary_key="id")
def worklogs(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    updated: dlt.sources.incremental[datetime] = dlt.sources.incremental(
        "updated", lag=600
    ),
) -> Iterator[TDataItem]:
    ...
```

`worklogs` loads the worklogs changed since the previous run. `dlt` deletes the worklogs deleted in
Jira. `worklogs` reads the worklogs of all issues of the site, not only of the issues loaded by
`issues`. The first run reads all worklogs. To start later, you can set the `initial_value` of
`updated`, or the `start_date` of the `jira` source.

### Resource `issue_fields`

`issue_fields` loads the id, name, schema and JQL clause names of each field. To find the name of a
custom field, you can join `issue_fields.id` to the column names of `issues`.

### Source `jira_search`

This source returns an `issues` resource that loads the issues found by several JQL queries
([Jira Query Language](https://support.atlassian.com/jira-service-management-cloud/docs/use-advanced-search-with-jira-query-language-jql/)).
It replaces its table on each run.

```py
from typing import Iterable, List, Optional
from dlt.extract import DltResource

@dlt.source(max_table_nesting=2)
def jira_search(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: Optional[int] = None,
    jql_queries: List[str] = dlt.config.value,
    page_limit: Optional[int] = None,
) -> Iterable[DltResource]:
    ...
```

- `page_size`: Issue ids listed in a single search request, at most 5000 (the default).
- `jql_queries`: The JQL queries to run. Jira rejects a query without a filtering clause, for example
  a query that only sorts.
- `page_limit`: The maximum number of search pages to read per query.

Issues load as in the [`issues` resource](#resource-issues), with the `query_index` of the query
that found them. An issue that two queries find loads twice. The primary key is `id` and
`query_index`.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods as discussed above.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our documentation [here](../../general-usage/pipeline):

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="jira_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="jira",  # Use a custom name if desired
   )
   ```

1. To load selected resources, for example users and projects:

   ```py notype
   load_info = pipeline.run(jira().with_resources("users", "projects"))
   print(load_info)
   ```

1. To load the issues of a project updated since a date, with their comments, changelogs and
   worklogs:

   ```py notype
   source = jira(jql="project = KAN", start_date="2024-01-01")
   load_info = pipeline.run(
       source.with_resources("issues", "issue_comments", "issue_changelogs", "worklogs")
   )
   print(load_info)
   ```

   `worklogs` loads the worklogs of the whole site since the same date. Each next run loads the
   issues updated since the previous run.

1. To load issues and their comments without the other resources:

   ```py notype
   issues = jira_issues(jql="project = KAN AND issuetype = Bug")
   load_info = pipeline.run([issues, issues | issue_comments])
   print(load_info)
   ```

1. To load only some fields:

   ```py notype
   issues = jira_issues(
       jql="project = KAN",
       included_columns=["summary", "status", "assignee", "updated", "customfield_10016"],
   )
   load_info = pipeline.run(issues)
   ```

   If you request at most 100 fields with a single value, Jira returns 1000 issues per request
   instead of 100.

1. To load issues created in a date range, with your own incremental on `created`:

   ```py notype
   from datetime import datetime, timezone

   issues = jira_issues(
       jql="project = KAN",
       incremental=dlt.sources.incremental(
           "created",
           initial_value=datetime(2024, 1, 1, tzinfo=timezone.utc),
           end_value=datetime(2024, 7, 1, tzinfo=timezone.utc),
       ),
   )
   load_info = pipeline.run(issues)
   ```

1. To load two tables of issues in one pipeline, give each resource its own name. Each resource keeps
   its own table and incremental state:

   ```py notype
   bugs = jira_issues(jql="issuetype = Bug", name="bugs")
   epics = jira_issues(jql="issuetype = Epic", name="epics")
   load_info = pipeline.run([bugs, epics])
   ```

1. Jira objects carry `self` links to the REST API and `avatarUrls` and `iconUrl` image urls. To skip
   these columns, add `drop_keys` to the resources. To skip other keys, pass their names to
   `drop_keys`:

   ```py notype
   source = jira()
   for resource in source.resources.values():
       resource.add_map(drop_keys())
   load_info = pipeline.run(source)
   ```

1. To load the issues that JQL queries find into a table that `dlt` replaces on each run:

   ```py notype
   queries = [
       "created >= -30d order by created DESC",
       'created >= -30d AND project = DEV AND issuetype = Epic AND status = "In Progress" order by created DESC',
   ]
   load_info = pipeline.run(jira_search(jql_queries=queries))
   print(load_info)
   ```

### Replace all issues on each run

A replace load skips the merge at the destination. A replace load is often cheaper than a merge. It
also works on destinations that do not support merge, for example
[filesystem](../destinations/filesystem.md) without a table format. With
`write_disposition="replace"`, each run loads all issues that match `jql`, or the issues updated
since `start_date`. The example below loads the issues of a project to Parquet
files:

```py notype
pipeline = dlt.pipeline(
    pipeline_name="jira_issues_files",
    destination="filesystem",
    dataset_name="jira",
)
issues = jira_issues(jql="project = KAN", write_disposition="replace")
load_info = pipeline.run(issues, loader_file_format="parquet")
print(load_info)
```

Set the location of the files in `.dlt/config.toml`:

```toml
[destination.filesystem]
bucket_url = "s3://my-bucket/jira"
```

The `jira` source passes its `write_disposition` to issues, comments, changelogs and worklogs.
With `replace`, `dlt` does not load deleted worklogs:

```py notype
load_info = pipeline.run(
    jira(jql="project = KAN", write_disposition="replace"), loader_file_format="parquet"
)
```

You can also set it in `.dlt/config.toml`:

```toml
[sources.jira]
write_disposition = "replace"
```

:::caution
When you pipe the transformers yourself, give them the write disposition of the issues:

```py notype
issues = jira_issues(jql="project = KAN", write_disposition="replace")
comments = (issues | issue_comments).apply_hints(write_disposition="replace")
load_info = pipeline.run([issues, comments], loader_file_format="parquet")
```

Do not set `replace` on `issues` with `apply_hints`. Pass the write disposition to `jira_issues` or
set it in the config. `apply_hints` does not reset the incremental state of `issues`. Then each run
replaces the table with only the updated issues.
:::

The dict form `{"disposition": "replace"}` keeps the incremental state: each run replaces the tables
with the issues updated since the previous run.

### Configure issues in `config.toml`

You can set the arguments of `jira` and `jira_issues` in `.dlt/config.toml`. The arguments of
`jira_issues` go in the `sources.jira.issues` section:

```toml
[sources.jira]
jql = "project = KAN"
start_date = "2024-01-01"

[sources.jira.issues]
included_columns = ["summary", "status", "assignee", "updated"]
lag = 60
```

### Reset the incremental state

`issues_ids`, the resource that lists the issue ids, keeps the incremental state of `issues`.
To load all issues again, run the pipeline once with
[`refresh="drop_resources"`](../../general-usage/pipeline.md#refresh-pipeline-data-and-state), or
drop both resources with the CLI:

```sh
dlt pipeline <pipeline_name> drop issues issues_ids
```

For a resource named with `jira_issues(name=...)`, `<name>_ids` keeps the state.

`issue_changelogs` loads only the changelogs of issues updated after you added `issue_changelogs` to
the pipeline. To load the changelogs of older issues, reset the state of `issues`.

### Large loads

- To normalize and load in parallel, set `file_max_items` in the `[sources.data_writer]` section,
  for example to `100000`. See
  [file rotation](../../reference/performance.md#controlling-intermediary-file-size-and-rotation).
- `add_limit(n)` on the source or on `issues` loads at most `n` pages of `chunk_size` issues, 1000
  by default, with their comments and changelogs. In `dlt` versions before 1.32.0, `add_limit` does
  not limit `issues`. With these versions, narrow `jql` or set `start_date` instead. `page_limit` of `jira_search` limits each
  query.

## Upgrade from previous versions

Version 1.0.0 has breaking changes. The
[changelog](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira/README.md#changelog)
in the README of the source lists them with the upgrade steps.

<!--@@@DLT_TUBA jira-->
