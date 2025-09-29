---
title: Cursor-based incremental loading
description: Track changes using cursor fields with dlt
keywords: [incremental loading, cursor, timestamp, last_value]
---

# Cursor-based incremental loading

In most REST APIs (and other data sources, i.e., database tables), you can request new or updated data by passing a timestamp or ID of the "last" record to a query. The API/database returns just the new/updated records from which you take the maximum/minimum timestamp/ID for the next load.

To do incremental loading this way, we need to:

- Figure out which field is used to track changes (the so-called **cursor field**) (e.g., "inserted_at", "updated_at", etc.);
- Determine how to pass the "last" (maximum/minimum) value of the cursor field to an API to get just new or modified data (how we do this depends on the source API).

Once you've figured that out, `dlt` takes care of finding maximum/minimum cursor field values, removing duplicates, and managing the state with the last values of the cursor. Take a look at the GitHub example below, where we request recently created issues.

```py
@dlt.resource(primary_key="id")
def repo_issues(
    access_token=dlt.secrets.value,
    repository=dlt.config.value,
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    # Get issues since "updated_at" stored in state on previous run (or initial_value on first run)
    for page in _get_issues_page(access_token, repository, since=updated_at.start_value):
        yield page
        # Last_value is updated after every page
        print(updated_at.last_value)
```

Here we add an `updated_at` argument that will receive incremental state, initialized to `1970-01-01T00:00:00Z`. It is configured to track the `updated_at` field in issues yielded by the `repo_issues` resource. It will store the newest `updated_at` value in `dlt` [state](../state.md) and make it available in `updated_at.start_value` on the next pipeline run. This value is inserted in the `_get_issues_page` function into the request query param **since** to the [GitHub API](https://docs.github.com/en/rest/issues/issues?#list-repository-issues).

In essence, the `dlt.sources.incremental` instance above:
* **updated_at.initial_value** which is always equal to "1970-01-01T00:00:00Z" passed in the constructor
* **updated_at.start_value** a maximum `updated_at` value from the previous run or the **initial_value** on the first run
* **updated_at.last_value** a "real-time" `updated_at` value updated with each yielded item or page. Before the first yield, it equals **start_value**
* **updated_at.end_value** (here not used) [marking the end of the backfill range](#using-end_value-for-backfill)

When paginating, you probably need the **start_value** which does not change during the execution of the resource, however, most paginators will return a **next page** link which you should use.

Behind the scenes, dlt will deduplicate the results, i.e., in case the last issue is returned again (`updated_at` filter is inclusive) and skip already loaded ones.

In the example below, we incrementally load the GitHub events, where the API does not let us filter for the newest events - it always returns all of them. Nevertheless, `dlt` will load only the new items, filtering out all the duplicates and past issues.
```py
# Use naming function in table name to generate separate tables for each event
@dlt.resource(primary_key="id", table_name=lambda i: i['type'])  # type: ignore
def repo_events(
    last_created_at = dlt.sources.incremental("created_at", initial_value="1970-01-01T00:00:00Z", last_value_func=max), row_order="desc"
) -> Iterator[TDataItems]:
    repos_path = "/repos/%s/%s/events" % (urllib.parse.quote(owner), urllib.parse.quote(name))
    for page in _get_rest_pages(access_token, repos_path + "?per_page=100"):
        yield page
```

We just yield all the events and `dlt` does the filtering (using the `id` column declared as `primary_key`).

GitHub returns events ordered from newest to oldest. So we declare the `rows_order` as **descending** to [stop requesting more pages once the incremental value is out of range](#declare-row-order-to-not-request-unnecessary-data). We stop requesting more data from the API after finding the first event with `created_at` earlier than `initial_value`.

:::note
`dlt.sources.incremental` is implemented as a [filter function](../resource.md#filter-transform-and-pivot-data) that is executed **after** all other transforms you add with `add_map` or  `add_filter`. This means that you can manipulate the data item before the incremental filter sees it. For example:
* You can create a surrogate primary key from other columns
* You can modify the cursor value or create a new field composed of other fields
* Dump Pydantic models to Python dicts to allow incremental to find custom values

[Data validation with Pydantic](../schema-contracts.md#use-pydantic-models-for-data-validation) happens **before** incremental filtering.
:::

## Max, min, or custom `last_value_func`

`dlt.sources.incremental` allows you to choose a function that orders (compares) cursor values to the current `last_value`.
* The default function is the built-in `max`, which returns the larger value of the two.
* Another built-in, `min`, returns the smaller value.

You can also pass your custom function. This lets you define
`last_value` on nested types, i.e., dictionaries, and store indexes of last values, not just simple
types. The `last_value` argument is a [JSON Path](https://github.com/json-path/JsonPath#operators)
and lets you select nested data (including the whole data item when `$` is used).
The example below creates a last value which is a dictionary holding a max `created_at` value for each
created table name:

```py
def by_event_type(event):
    last_value = None
    if len(event) == 1:
        item, = event
    else:
        item, last_value = event

    if last_value is None:
        last_value = {}
    else:
        last_value = dict(last_value)
    item_type = item["type"]
    last_value[item_type] = max(item["created_at"], last_value.get(item_type, "1970-01-01T00:00:00Z"))
    return last_value

@dlt.resource(primary_key="id", table_name=lambda i: i['type'])
def get_events(last_created_at = dlt.sources.incremental("$", last_value_func=by_event_type)):
    with open("tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8") as f:
        yield json.load(f)
```

## Using `end_value` for backfill

You can specify both initial and end dates when defining incremental loading. Let's go back to our Github example:
```py
@dlt.resource(primary_key="id")
def repo_issues(
    access_token=dlt.secrets.value,
    repository=dlt.config.value,
    updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z", end_value="2022-07-01T00:00:00Z")
):
    # get issues updated in range defined by incremental
    for page in _get_issues_page(access_token, repository, since=updated_at.start_value, until=updated_at.end_value):
        yield page
```
Above, we use the `initial_value` and `end_value` arguments of the `incremental` to define the range of issues that we want to retrieve
and pass this range to the Github API (`since` and `until`). As in the examples above, `dlt` will make sure that only the issues from
the defined range are returned.

Please note that when `end_date` is specified, `dlt` **will not modify the existing incremental state**. The backfill is **stateless** and:
1. You can run backfill and incremental load in parallel (i.e., in an Airflow DAG) in a single pipeline.
2. You can partition your backfill into several smaller chunks and run them in parallel as well.

To define specific ranges to load, you can simply override the incremental argument in the resource, for example:

```py
july_issues = repo_issues(
    updated_at=dlt.sources.incremental(
        initial_value='2022-07-01T00:00:00Z', end_value='2022-08-01T00:00:00Z'
    )
)
august_issues = repo_issues(
    updated_at=dlt.sources.incremental(
        initial_value='2022-08-01T00:00:00Z', end_value='2022-09-01T00:00:00Z'
    )
)
...
```

Note that dlt's incremental filtering considers the ranges half-closed. `initial_value` is inclusive, `end_value` is exclusive, so chaining ranges like above works without overlaps. This behaviour can be changed with the `range_start` (default `"closed"`) and `range_end` (default `"open"`) arguments.

### Partition large backfills
You can execute a backfill on large amount of data by partitioning it into ranges. In best case you are able to
create partitions ie. by day or week without additionally querying your data source [as we do in sql database example](../../dlt-ecosystem/verified-sources/sql_database/advanced.md#split-or-partition-long-incremental-loads). Ranges will be loaded using `initial_value` and `end_value`, each in separate pipeline run.
1. As mentioned above, each run of this kind is stateless and may be run in parallel.
2. However, we recommend that you load a single partition first (pick a small one) so `dlt` creates dataset and schema without risk of races and a need to retry
ie. when two runs created new dataset at the same time.
3. Such first load is also a good opportunity to begin a [full refresh](../../general-usage/pipeline.md#refresh-pipeline-data-and-state)
4. After a backfill you can resume regular incremental loading with a state. You'll need to use `end_value` of final range as `initial_value` of incremental loading.

For a robust backfill of this kind you probably want to use an orchestrator to make sure that each partition is loaded and loaded only once.

Also check [filesystem](../../dlt-ecosystem/verified-sources/filesystem/basic.md#6-split-large-incremental-loads) example.

## Declare row order to not request unnecessary data

With the `row_order` argument set, dlt will stop retrieving data from the data source (e.g., GitHub API) if it detects that the values of the cursor field are out of the range of **start** and **end** values.

In particular:
* dlt stops processing when the resource yields any item with a cursor value _equal to or greater than_ the `end_value` and `row_order` is set to **asc**. (`end_value` is not included)
* dlt stops processing when the resource yields any item with a cursor value _lower_ than the `last_value` and `row_order` is set to **desc**. (`last_value` is included)

:::note
"higher" and "lower" here refer to when the default `last_value_func` is used (`max()`),
when using `min()` "higher" and "lower" are inverted.
:::

:::warning
If you use `row_order`, **make sure that the data source returns ordered records** (ascending / descending) on the cursor field,
e.g., if an API returns results both higher and lower
than the given `end_value` in no particular order, data reading stops and you'll miss the data items that were out of order.
The following commonly used sources apply row order to returned rows:
1. [sql_database](../../dlt-ecosystem/verified-sources/sql_database/)
2. [filesystem](../../dlt-ecosystem/verified-sources/filesystem/)
3. [mongodb](../../dlt-ecosystem/verified-sources/mongodb.md)
:::

Row order is most useful when:

1. The data source does **not** offer start/end filtering of results (e.g., there is no `start_time/end_time` query parameter or similar).
2. The source returns results **ordered by the cursor field**.

The GitHub events example is exactly such a case. The results are ordered on cursor value descending, but there's no way to tell the API to limit returned items to those created before a certain date. Without the `row_order` setting, we'd be getting all events, each time we extract the `github_events` resource.

In the same fashion, the `row_order` can be used to **optimize backfill** so we don't continue
making unnecessary API requests after the end of the range is reached. For example:

```py
@dlt.resource(primary_key="id")
def tickets(
    zendesk_client,
    updated_at=dlt.sources.incremental(
        "updated_at",
        initial_value="2023-01-01T00:00:00Z",
        end_value="2023-02-01T00:00:00Z",
        row_order="asc"
    ),
):
    for page in zendesk_client.get_pages(
        "/api/v2/incremental/tickets", "tickets", start_time=updated_at.start_value
    ):
        yield page
```

In this example, we're loading tickets from Zendesk. The Zendesk API yields items paginated and ordered from oldest to newest,
but only offers a `start_time` parameter for filtering, so we cannot tell it to
stop retrieving data at `end_value`. Instead, we set `row_order` to `asc` and `dlt` will stop
getting more pages from the API after the first page with a cursor value `updated_at` is found older
than `end_value`.

:::warning
In rare cases when you use Incremental with a transformer, `dlt` will not be able to automatically close
the generator associated with a row that is out of range. You can still call the `can_close()` method on
incremental and exit the yield loop when true.
:::

:::tip
The `dlt.sources.incremental` instance provides `start_out_of_range` and `end_out_of_range`
attributes which are set when the resource yields an element with a higher/lower cursor value than the
initial or end values. If you do not want `dlt` to stop processing automatically and instead want to handle such events yourself, do not specify `row_order`:
```py
@dlt.transformer(primary_key="id")
def tickets(
    zendesk_client,
    updated_at=dlt.sources.incremental(
        "updated_at",
        initial_value="2023-01-01T00:00:00Z",
        end_value="2023-02-01T00:00:00Z",
    ),
):
    for page in zendesk_client.get_pages(
        "/api/v2/incremental/tickets", "tickets", start_time=updated_at.start_value
    ):
        yield page
        # Stop loading when we reach the end value
        if updated_at.end_out_of_range:
            return

```
:::


## Split large loads into chunks
You can split large incremental resources into smaller chunks and load them sequentially. This way you'll see the data quicker and
in case of loading error you are able to retry a single chunk. **This method works only if your source returns data in deterministic order**, for example:
* you can request your REST API endpoint to return data ordered by `updated_at`.
* you use `row_order` on one of supported sources like `sql_database` or `filesystem`.

Below we go for the second option and load data from messages table that we order on `created_at` column.
```py
import dlt
from dlt.sources.sql_database import sql_table

pipeline = dlt.pipeline("test_load_sql_table_split_loading", destination="duckdb")

messages = sql_table(
    table="chat_message",
    incremental=dlt.sources.incremental(
        "created_at",
        row_order="asc",  # critical to set row_order when doing split loading
        range_start="open",  # use open range to disable deduplication
    ),
)

# produce chunk each minute, stop when empty
while not pipeline.run(messages.add_limit(max_time=60)).is_empty:
    pass
```
Note how we combine `incremental` and `add_limit` to generate chunk each minute. If you create and index on `created_at`, the database
engine will be able to stream data using the index without the need to scan the whole table.

:::warning
If your source returns unordered data, you will most probably miss some data items or load them twice.
:::

Check two other examples: [filesystem](../../dlt-ecosystem/verified-sources/filesystem/basic.md#6-split-large-incremental-loads) and
[sql_database](../../dlt-ecosystem/verified-sources/sql_database/advanced.md#split-or-partition-long-incremental-loads).


## Deduplicate overlapping ranges

`Incremental` **does not** deduplicate datasets like the **merge** write disposition does. However, it ensures that when another portion of data is extracted, records that were previously loaded **at the end of range** won't be included again. `dlt` assumes that you load a range of data, where the lower bound is inclusive by default (i.e., greater than or equal). This ensures that you never lose any data but will also re-acquire some rows. For example, if you have a database table with a cursor field on `updated_at` which has a day resolution, then there's a high chance that after you extract data on a given day, more records will still be added. When you extract on the next day, you should reacquire data from the last day to ensure all records are present; however, this will create overlap with data from the previous extract.

By default, a content hash (a hash of the JSON representation of a row) will be used to deduplicate. This may be slow, so `dlt.sources.incremental` will inherit the primary key that is set on the resource. You can optionally set a `primary_key` that is used exclusively to deduplicate and which does not become a table hint. The same setting lets you disable the deduplication altogether when an empty tuple is passed. Below, we pass `primary_key` directly to `incremental` to disable deduplication. That overrides the `delta` primary_key set in the resource:

```py
@dlt.resource(primary_key="delta")
# disable the unique value check by passing () as primary key to incremental
def some_data(last_timestamp=dlt.sources.incremental("item.ts", primary_key=())):
    for i in range(-10, 10):
        yield {"delta": i, "item": {"ts": pendulum.now().timestamp()}}
```

This deduplication process is enabled when `range_start` is set to `"closed"` (default).
When you pass `range_start="open"` no deduplication is done as it is not needed as rows with the previous cursor value are excluded. This can be a useful optimization to avoid the performance overhead of deduplication if the cursor field is guaranteed to be unique.
Deduplication is also disabled when [lag](lag.md) is used or when `end_value` is specified as in this case, state is disabled and no hashes from previous runs will be present.

## Use `dlt.sources.incremental` with dynamically created resources

When resources are [created dynamically](../source.md#create-resources-dynamically), it is possible to use the `dlt.sources.incremental` definition as well.

```py
@dlt.source
def stripe():
    # declare a generator function
    def get_resource(
        endpoints: List[str] = ENDPOINTS,
        created: dlt.sources.incremental=dlt.sources.incremental("created")
    ):
        ...

    # create resources for several endpoints on a single decorator function
    for endpoint in endpoints:
        yield dlt.resource(
            get_resource,
            name=endpoint.value,
            write_disposition="merge",
            primary_key="id"
        )(endpoint)
```

Please note that in the example above, `get_resource` is passed as a function to `dlt.resource` to which we bind the endpoint: **dlt.resource(...)(endpoint)**.

:::warning
The typical mistake is to pass a generator (not a function) as below:

`yield dlt.resource(get_resource(endpoint), name=endpoint.value, write_disposition="merge", primary_key="id")`.

Here we call **get_resource(endpoint)** and that creates an un-evaluated generator on which the resource is created. That prevents `dlt` from controlling the **created** argument during runtime and will result in an `IncrementalUnboundError` exception.
:::

## Using Airflow schedule for backfill and incremental loading

When [running an Airflow task](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#2-modify-dag-file), you can opt-in your resource to get the `initial_value`/`start_value` and `end_value` from the Airflow schedule associated with your DAG. Let's assume that the **Zendesk tickets** resource contains a year of data with thousands of tickets. We want to backfill the last year of data week by week and then continue with incremental loading daily.

```py
@dlt.resource(primary_key="id")
def tickets(
    zendesk_client,
    updated_at=dlt.sources.incremental[int](
        "updated_at",
        allow_external_schedulers=True
    ),
):
    for page in zendesk_client.get_pages(
        "/api/v2/incremental/tickets", "tickets", start_time=updated_at.start_value
    ):
        yield page
```

We opt-in to the Airflow scheduler by setting `allow_external_schedulers` to `True`:
1. When running on Airflow, the start and end values are controlled by Airflow and the dlt [state](../state.md) is not used.
2. In all other environments, the `incremental` behaves as usual, maintaining the dlt state.

Let's generate a deployment with `dlt deploy zendesk_pipeline.py airflow-composer` and customize the DAG:

```py
from dlt.helpers.airflow_helper import PipelineTasksGroup

@dag(
    schedule_interval='@weekly',
    start_date=pendulum.DateTime(2023, 2, 1),
    end_date=pendulum.DateTime(2023, 8, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_task_args
)
def zendesk_backfill_bigquery():
    tasks = PipelineTasksGroup("zendesk_support_backfill", use_data_folder=False, wipe_local_data=True)

    # import zendesk like in the demo script
    from zendesk import zendesk_support

    pipeline = dlt.pipeline(
        pipeline_name="zendesk_support_backfill",
        dataset_name="zendesk_support_data",
        destination='bigquery',
    )
    # select only incremental endpoints in support api
    data = zendesk_support().with_resources("tickets", "ticket_events", "ticket_metric_events")
    # create the source, the "serialize" decompose option will convert dlt resources into Airflow tasks. use "none" to disable it
    tasks.add_run(pipeline, data, decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)


zendesk_backfill_bigquery()
```

What got customized:
1. We use a weekly schedule and want to get the data from February 2023 (`start_date`) until the end of July (`end_date`).
2. We make Airflow generate all weekly runs (`catchup` is True).
3. We create `zendesk_support` resources where we select only the incremental resources we want to backfill.

When you enable the DAG in Airflow, it will generate several runs and start executing them, starting in February and ending in August. Your resource will receive subsequent weekly intervals starting with `2023-02-12, 00:00:00 UTC` to `2023-02-19, 00:00:00 UTC`.

You can repurpose the DAG above to start loading new data incrementally after (or during) the backfill:

```py
@dag(
    schedule_interval='@daily',
    start_date=pendulum.DateTime(2023, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args
)
def zendesk_new_bigquery():
    tasks = PipelineTasksGroup("zendesk_support_new", use_data_folder=False, wipe_local_data=True)

    # import your source from pipeline script
    from zendesk import zendesk_support

    pipeline = dlt.pipeline(
        pipeline_name="zendesk_support_new",
        dataset_name="zendesk_support_data",
        destination='bigquery',
    )
    tasks.add_run(pipeline, zendesk_support(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
```

Above, we switch to a daily schedule and disable catchup and end date. We also load all the support resources to the same dataset as backfill (`zendesk_support_data`).
If you want to run this DAG parallel with the backfill DAG, change the pipeline name, for example, to `zendesk_support_new` as above.

**Under the hood**

Before `dlt` starts executing incremental resources, it looks for `data_interval_start` and `data_interval_end` Airflow task context variables. These are mapped to `initial_value` and `end_value` of the `Incremental` class:
1. `dlt` is smart enough to convert Airflow datetime to ISO strings or Unix timestamps if your resource is using them. In our example, we instantiate `updated_at=dlt.sources.incremental[int]`, where we declare the last value type to be **int**. `dlt` can also infer the type if you provide the `initial_value` argument.
2. If `data_interval_end` is in the future or is None, `dlt` sets the `end_value` to **now**.
3. If `data_interval_start` == `data_interval_end`, we have a manually triggered DAG run. In that case, `data_interval_end` will also be set to **now**.

**Manual runs**

You can run DAGs manually, but you must remember to specify the Airflow logical date of the run in the past (use the Run with config option). For such a run, `dlt` will load all data from that past date until now.
If you do not specify the past date, a run with a range (now, now) will happen, yielding no data.

## Reading incremental loading parameters from configuration

Consider the example below for reading incremental loading parameters from "config.toml". We create a `generate_incremental_records` resource that yields "id", "idAfter", and "name". This resource retrieves `cursor_path` and `initial_value` from "config.toml".

1. In "config.toml", define the `cursor_path` and `initial_value` as:
   ```toml
   # Configuration snippet for an incremental resource
   [pipeline_with_incremental.sources.id_after]
   cursor_path = "idAfter"
   initial_value = 10
   ```

   `cursor_path` is assigned the value "idAfter" with an initial value of 10.

1. Here's how the `generate_incremental_records` resource uses the `cursor_path` defined in "config.toml":
   ```py
   @dlt.resource(table_name="incremental_records")
   def generate_incremental_records(id_after: dlt.sources.incremental = dlt.config.value):
       for i in range(150):
           yield {"id": i, "idAfter": i, "name": "name-" + str(i)}

   pipeline = dlt.pipeline(
       pipeline_name="pipeline_with_incremental",
       destination="duckdb",
   )

   pipeline.run(generate_incremental_records)
   ```
   `id_after` incrementally stores the latest `cursor_path` value for future pipeline runs.

## Loading when incremental cursor path is missing or value is None/NULL

You can customize the incremental processing of dlt by setting the parameter `on_cursor_value_missing`.

When loading incrementally with the default settings, there are two assumptions:
1. Each row contains the cursor path.
2. Each row is expected to contain a value at the cursor path that is not `None`.

For example, the two following source data will raise an error:
```py
@dlt.resource
def some_data_without_cursor_path(updated_at=dlt.sources.incremental("updated_at")):
    yield [
        {"id": 1, "created_at": 1, "updated_at": 1},
        {"id": 2, "created_at": 2},  # cursor field is missing
    ]

list(some_data_without_cursor_path())

@dlt.resource
def some_data_without_cursor_value(updated_at=dlt.sources.incremental("updated_at")):
    yield [
        {"id": 1, "created_at": 1, "updated_at": 1},
        {"id": 3, "created_at": 4, "updated_at": None},  # value at cursor field is None
    ]

list(some_data_without_cursor_value())
```


To process a data set where some records do not include the incremental cursor path or where the values at the cursor path are `None`, there are the following four options:

1. Configure the incremental load to raise an exception in case there is a row where the cursor path is missing or has the value `None` using `incremental(..., on_cursor_value_missing="raise")`. This is the default behavior.
2. Configure the incremental load to tolerate the missing cursor path and `None` values using `incremental(..., on_cursor_value_missing="include")`.
3. Configure the incremental load to exclude the missing cursor path and `None` values using `incremental(..., on_cursor_value_missing="exclude")`.
4. Before the incremental processing begins: Ensure that the incremental field is present and transform the values at the incremental cursor to a value different from `None`. [See docs below](#transform-records-before-incremental-processing)

Here is an example of including rows where the incremental cursor value is missing or `None`:
```py
@dlt.resource
def some_data(updated_at=dlt.sources.incremental("updated_at", on_cursor_value_missing="include")):
    yield [
        {"id": 1, "created_at": 1, "updated_at": 1},
        {"id": 2, "created_at": 2},
        {"id": 3, "created_at": 4, "updated_at": None},
    ]

result = list(some_data())
assert len(result) == 3
assert result[1] == {"id": 2, "created_at": 2}
assert result[2] == {"id": 3, "created_at": 4, "updated_at": None}
```

If you do not want to import records without the cursor path or where the value at the cursor path is `None`, use the following incremental configuration:

```py
@dlt.resource
def some_data(updated_at=dlt.sources.incremental("updated_at", on_cursor_value_missing="exclude")):
    yield [
        {"id": 1, "created_at": 1, "updated_at": 1},
        {"id": 2, "created_at": 2},
        {"id": 3, "created_at": 4, "updated_at": None},
    ]

result = list(some_data())
assert len(result) == 1
```

## Transform records before incremental processing
If you want to load data that includes `None` values, you can transform the records before the incremental processing.
You can add steps to the pipeline that [filter, transform, or pivot your data](../resource.md#filter-transform-and-pivot-data).

:::warning
It is important to set the `insert_at` parameter of the `add_map` function to control the order of execution and ensure that your custom steps are executed before the incremental processing starts.
In the following example, the step of data yielding is at `index = 0`, the custom transformation at `index = 1`, and the incremental processing at `index = 2`.
:::

See below how you can modify rows before the incremental processing using `add_map()` and filter rows using `add_filter()`.

```py
@dlt.resource
def some_data(updated_at=dlt.sources.incremental("updated_at")):
    yield [
        {"id": 1, "created_at": 1, "updated_at": 1},
        {"id": 2, "created_at": 2, "updated_at": 2},
        {"id": 3, "created_at": 4, "updated_at": None},
    ]

def set_default_updated_at(record):
    if record.get("updated_at") is None:
        record["updated_at"] = record.get("created_at")
    return record

# Modifies records before the incremental processing
with_default_values = some_data().add_map(set_default_updated_at, insert_at=1)
result = list(with_default_values)
assert len(result) == 3
assert result[2]["updated_at"] == 4

# Removes records before the incremental processing
without_none = some_data().add_filter(lambda r: r.get("updated_at") is not None, insert_at=1)
result_filtered = list(without_none)
assert len(result_filtered) == 2
```