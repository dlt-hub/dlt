---
title: Incremental loading
description: Incremental loading with dlt
keywords: [incremental loading, loading methods, append]
---

# Incremental loading

Incremental loading is the act of loading only new or changed data and not old records that we already loaded. It enables low-latency and low cost data transfer.

The challenge of incremental pipelines is that if we do not keep track of the state of the load (i.e. which increments were loaded and which are to be loaded). Read more about state [here](../general-usage/state.md).

## Choosing a write disposition

### The 3 write dispositions:
**Full load**: replaces the destination dataset with whatever the source produced on this run. To achieve this, use `write_disposition='replace'`in your resources.

**Append**: appends the new data to the destination. Use `write_disposition='append'`.

**Merge**: Merges new data to the destination using `merge_key` and/or deduplicates/upserts new data using `private_key`. Use `write_disposition='merge'`.

### Two simple questions determine the write disposition you use

<div style={{textAlign: 'center'}}>

![write disposition flowchart](/img/write-dispo-choice.png)

</div>

The write disposition you choose depends on the data set and how you can extract it.

To find the write disposition you should use, the first question you should ask yourself is "Is my data stateful or stateless"?
Stateful data has a state that is subject to change - for example a user's profile
Stateless data cannot change - for example, a recorded event, such as a page view.

Because stateless data does not need to be updated, we can just append it.

For stateful data, comes a second question - Can I extract it incrementally from the source?
If not, then we need to replace the entire data set. If however we can request the data incrementally such as "all users added or modified since yesterday" then we can simply apply changes to our existing dataset with the merge write disposition.

## Merge incremental loading
The `merge` write disposition is used in two scenarios:

1. You want to keep only one instance of certain record ie. you receive updates of the `user` state from an API and want to keep just one record per `user_id`.
2. You receive data in daily batches and you want to make sure that you always keep just a single instance of a record for each batch even in case you load an old batch or load the current batch several times a day (ie. to receive "live" updates).

The `merge` write disposition loads data to a `staging` dataset, deduplicates the staging data is `primary_key` is provided, deletes the data from the destination by `merge_key` and `primary_key` and then inserts the new records. This all happens in single atomic transaction for a parent and all child tables.

Example below loads all the github events and updates them in the destination using "id" as primary key, making sure that only a single copy of event is present in `github_repo_events` table:

```python
@dlt.resource(primary_key="id", write_disposition="merge")
def github_repo_events():
    yield from _get_event_pages()
```

You can use compound primary keys:

```python
@dlt.resource(primary_key=("id", "url"), write_disposition="merge")
...
```

Example below merges on a column `batch_day` that holds the day for which given record is valid. Merge keys also can be compound:
```python
@dlt.resource(merge_key="batch_day", write_disposition="merge")
def get_daily_batch(day):
    yield _get_batch_from_bucket(day)
```

As with any other write disposition you can use it to load data ad hoc. Below we load issues with top reactions for `duckdb` repo. The lists have, obviously, many overlapping issues but we want to keep just one instance of each.

```python
p = dlt.pipeline(destination="bigquery", dataset_name="github")
issues = []
reactions = ["%2B1", "-1", "smile", "tada", "thinking_face", "heart", "rocket", "eyes"]
for reaction in reactions:
    for page_no in range(1, 3):
      page = requests.get(f"https://api.github.com/repos/{repo}/issues?state=all&sort=reactions-{reaction}&per_page=100&page={page_no}", headers=headers)
      print(f"got page for {reaction} page {page_no}, requests left", page.headers["x-ratelimit-remaining"])
      issues.extend(page.json())
p.run(issues, write_disposition="merge", primary_key="id", table_name="issues")
```

Example below dispatches github events to several tables by event type, keeps one copy of each event by "id" and skips loading of past records using "last value" incremental. As you can see, all of this we can just declare in our resource.

```python
@dlt.resource(primary_key="id", write_disposition="merge", table_name=lambda i: i['type'])
def github_repo_events(last_created_at = dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z")):
    """A resource taking a stream of github events and dispatching them to tables named by event type. Deduplicates be 'id'. Loads incrementally by 'created_at' """
    yield from _get_rest_pages("events")
```

### Forcing root key propagation
Merge write disposition requires that the `_dlt_id` of top level table is propagated to child tables. This concept is similar to foreign key which references a parent table and we call it a `root key`. Root key is automatically propagated for all tables that have `merge` write disposition set. We do not enable it everywhere because it takes storage space. Nevertheless is some cases you may want to permanently enable root key propagation.
```python
pipeline = dlt.pipeline(pipeline_name='facebook_insights', destination='duckdb', dataset_name='facebook_insights_data', full_refresh=True)
fb_ads = facebook_ads_source()
# enable root key propagation on a source that is not a merge one by default. this is not required if you always use merge but below we start
# with replace
fb_ads.root_key = True
# load only disapproved ads
fb_ads.ads.bind(states=("DISAPPROVED", ))
info = pipeline.run(fb_ads.with_resources("ads"), write_disposition="replace")
# merge the paused ads. the disapproved ads stay there!
fb_ads = facebook_ads_source()
fb_ads.ads.bind(states=("PAUSED", ))
info = pipeline.run(fb_ads.with_resources("ads"), write_disposition="merge")
```
In example above we enforce the root key propagation with `fb_ads.root_key = True`. This ensures that correct data is propagated on initial `replace` load so the future `merge` load can be executed. You can achieve the same in the decorator `@dlt.source(root_key=True)`.

## Incremental loading with last value

In most of the APIs (and other data sources ie. database tables) you can request only new or updated data by passing a timestamp or id of the last record to a query. The API/database returns just the new/updated records from which you take "last value" timestamp/id for the next load.

To do incremental loading this way, we need to
- figure which data element is used to get new/updated records (e.g. “last value”, “last updated at”, etc)
- request the new part only (how we do this depends on the source API)

Once you've figured that out, `dlt` takes care of the loading of the incremental, removing duplicates and managing the state with last values. Take a look at github example below, where we request recently created issues.

```python
@dlt.resource(primary_key="id")
  def repo_issues(access_token, repository, created_at = dlt.sources.incremental("created_at", initial_value="1970-01-01T00:00:00Z")):
      # get issues from created from last "created_at" value
      for page in _get_issues_page(access_token, repository, since=created_at.last_value):
          yield page
```

Here we add `created_at` argument that will receive incremental state, initialized to `1970-01-01T00:00:00Z`. It is configured to track `created_at` field in issues returned by `_get_issues_page` and then yielded. It will store the newest `created_at` value in `dlt` [state](../general-usage/state.md) and make it available in `created_at.last_value` on next pipeline run. This value is used to request only issues newer (or equal) via github API.

On the first run of this resource, all the issues (we use "1970-01-01T00:00:00Z" as initial to get all of them) will be loaded and the `created_at.last_value` will get the `created_at` of most recent issue. On the second run we'll pass this value to `_get_issues_page` to get only the newer issues.

Behind the scenes, `dlt` will deduplicate the results ie. in case the last issue is returned again (`created_at` filter is inclusive) and skip already loaded ones. In the example below we incrementally load the github events, where API does not let us to filter for the newest events - it always returns all of them. Nevertheless `dlt` will load only the incremental part, skipping all the duplicates and past issues.

```python
  # use naming function in table name to generate separate tables for each event

  @dlt.resource(primary_key="id", table_name=lambda i: i['type'])  # type: ignore
  def repo_events(last_created_at = dlt.sources.incremental("created_at", initial_value="1970-01-01T00:00:00Z", last_value_func=max)) -> Iterator[TDataItems]:
      repos_path = "/repos/%s/%s/events" % (urllib.parse.quote(owner), urllib.parse.quote(name))
      for page in _get_rest_pages(access_token, repos_path + "?per_page=100"):
          yield page

          # ---> part below is an optional optimization

          # stop requesting pages if the last element was already older than incremental value at the beginning of the run
          if page and page[-1]["created_at"] < last_created_at.start_value:
              break

  return repo_events
```
We just yield all the events and `dlt` does the filtering (using `id` column declared as `primary_key`). A small optimization will stop requesting more result pages, when `created_at` of the last element in the page is smaller than `last_created_at.start_value`. `last_created_at.start_value` keeps the `last_value` from the beginning of a pipeline run. `last_created_at.initial_value` keeps the initial value passed to the incremental ie. "1970-01-01T00:00:00Z" in the example above.

`dlt.sources.incremental` allows to define custom `last_value` function. This lets you define `last_value` on complex types ie. dictionaries and store indexes of last values, not just simple types. The `last_value` argument is a [JSON Path](https://github.com/json-path/JsonPath#operators) and let's you select nested and complex data (including the whole data item when `$` is used). Example below creates last value which is a dictionary holding a max `created_at` value for each created table name:

```python
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

`dlt.sources.incremental` let's you optionally set a `primary_key` that is used exclusively to deduplicate and which does not becomes a table hint. The same setting let's you disable the deduplication altogether when empty tuple is passed. Below we pass `primary_key` directly to `incremental` to disable deduplication. That overrides `delta` primary_key set in the resource:

```python
@dlt.resource(primary_key="delta")
  # disable the unique value check by passing () as primary key to incremental
  def some_data(last_timestamp=dlt.sources.incremental("item.ts", primary_key=())):
      for i in range(-10, 10):
          yield {"delta": i, "item": {"ts": pendulum.now().timestamp()}}
```

### Using `dlt.sources.incremental` with dynamically created resources
When resources are [created dynamically](source.md#create-resources-dynamically) it is possible to use `dlt.sources.incremental` definition as well.
```python

@dlt.source
def stripe():
  # declare a generator function
  def get_resource(endpoint: Endpoints, created: dlt.sources.incremental =dlt.sources.incremental("created")):
    ...
    yield data

  # create resources for several endpoints on a single decorator function
  for endpoint in Endpoints:
    yield dlt.resource(get_resource, name=endpoint.value, write_disposition="merge", primary_key="id")(endpoint)
```
Please note that in the example above, `get_resource` is passed as a function to `dlt.resource` to which we bind the endpoint: **dlt.resource(...)(endpoint)**

> 🛑 The typical mistake is to pass a generator (not a function) as below:
>
> `yield dlt.resource(get_resource(endpoint), name=endpoint.value, write_disposition="merge", primary_key="id")`.
>
> Here we call **get_resource(endpoint)** and that creates un-evaluated generator on which resource is created. That prevents `dlt` from controlling the **created** argument during runtime and will result in `IncrementalUnboundError` exception.

## Doing a full refresh

You may force a full refresh of a `merge` and `append` pipelines.
1. In case of a `merge` the data in the destination is deleted and loaded fresh. Currently we do not deduplicate data during the full refresh.
2. In case of `dlt.sources.incremental` the data is deleted and loaded from scratch. The state of the incremental is reset to the initial value.

Example:

```python
p = dlt.pipeline(destination="bigquery", dataset_name="github_3")
# do a full refresh
p.run(merge_source(), write_disposition="replace")
# do a full refresh of just one table
p.run(merge_source().with_resources("merge_table"), write_disposition="replace")
# run a normal merge
p.run(merge_source())
```

Passing write disposition to `replace` will change write disposition on all the resources in `repo_events` during the run of the pipeline.

## Custom incremental loading with pipeline state

The pipeline state is a Python dictionary which gets committed atomically with the data; you can set values in it in your resources and on next pipeline run, request them back.

The pipeline state is in principle scoped to the resource - all values of the state set by resource are private and isolated from any other resource. You can also access the source-scoped state which can be shared across resources. [You can find more information on pipeline state here](state.md#pipeline-state).

### Preserving the last value in resource state.

For the purpose of preserving the “last value” or similar loading checkpoints, we can open a dlt state dictionary with a key and a default value as below. When the resource is executed and the data is loaded, the yielded resource data will be loaded at the same time with the update to the state.

In the two examples below you see how the `dlt.sources.incremental` is working under the hood.

```python
@resource()
def tweets():
	# Get a last value from loaded metadata. If not exist, get None
	last_val = dlt.current.resource_state().setdefault("last_updated", None)
  # get data and yield it
  data = get_data(start_from=last_val)
  yield data
  # change the state to the new value
  dlt.current.state()["last_updated"]  = data["last_timestamp"]
```
if we keep a list or a dictionary in the state, we can modify the underlying values in the objects and thus we do not need to set the state back explicitly.
```python
@resource()
def tweets():
  # Get a last value from loaded metadata. If not exist, get None
  loaded_dates = dlt.current.resource_state().setdefault("days_loaded", [])
  # do stuff: get data and add new values to the list
  # `loaded_date` is a reference to the `dlt.current.resource_state()["days_loaded"]` list
  # and thus modifying it modifies the state
  yield data
  loaded_dates.append('2023-01-01')
```

Step by step explanation of how to get or set the state:
1. We can use the function `var = dlt.current.resource_state().setdefault("key", [])`. This allows us to retrieve the values of `key`. If `key` was not set yet, we will get the default value `[]` instead
2. We now can treat `var` as a python list - We can append new values to it, or if applicable we can read the values from previous loads.
3. On pipeline run, the data will load, and the new `var`'s value will get saved in the state. The state is stored at the destination, so it will be available on subsequent runs.


### Advanced state usage: storing a list of processed entities

Let’s look at the `player_games` resource from the chess pipeline. The chess API has a method to request games archives for a given month. The task is to prevent the user to load the same month data twice - even if the user makes a mistake and requests the same months range again:
- Our data is requested in 2 steps
    - get all available archives URLs
    - get the data from each URL
- We will add the “chess archives” URLs to this list we created
- This will allow us to track what data we have loaded
- When the data is loaded, the list of archives is loaded with it
- Later we can read this list and know what data has already been loaded

In the following example, we initialize a variable with an empty list as a default:

```python
@dlt.resource(write_disposition="append")
def players_games(chess_url, players, start_month=None, end_month=None):

    loaded_archives_cache = dlt.current.resource_state().setdefault("archives", [])

		# as far as python is concerned, this variable behaves like
    # loaded_archives_cache = state['archives'] or []
    # afterwards we can modify list, and finally
    # when the data is loaded, the cache is updated with our loaded_archives_cache

		# get archives for a given player
    archives = get_players_archives(chess_url, players)
    for url in archives:
        # if not in cache, yield the data and cache the URL
        if url not in loaded_archives_cache:
						# add URL to cache and yield the associated data
						loaded_archives_cache.append(url)
		        r = requests.get(url)
		        r.raise_for_status()
		        yield r.json().get("games", [])
        else:
            print(f"skipping archive {url}")
```

### Advanced state usage: tracking the last value for all search terms in twitter API

```python
@dlt.resource(write_disposition="append")
def search_tweets(twitter_bearer_token=dlt.secrets.value, search_terms=None, start_time=None, end_time=None, last_value=None):
    headers = _headers(twitter_bearer_token)
    for search_term in search_terms:
				# make cache for each term
        last_value_cache = dlt.current.state().setdefault(f"last_value_{search_term}", None)
        print(f'last_value_cache: {last_value_cache}')
        params = {...
                  }
        url = "https://api.twitter.com/2/tweets/search/recent"
        response = _paginated_get(url, headers=headers, params=params)
        for page in response:
            page['search_term'] = search_term
            last_id = page.get('meta', {}).get('newest_id', 0)
            #set it back - not needed if we
            dlt.current.state()[f"last_value_{search_term}"] = max(last_value_cache or 0, int(last_id))
						# print the value for each search term
            print(f'new_last_value_cache for term {search_term}: {last_value_cache}')

            yield page
```