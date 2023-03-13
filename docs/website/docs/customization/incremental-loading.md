---
sidebar_position: 1
---

# Incremental loading

Incremental loading is the act of loading only new or changed data and not old records that we already loaded. It enables low-latency and low cost data transfer.

The challenge of incremental pipelines is that if we do not keep track of the state of the load (i.e. which increments were loaded and which are to be loaded). Read more about state [here](../concepts/state.md).

### There are 3 types of common loading methods:

**Full load**: replaces the destination dataset with whatever the source produced on this run. To achieve this, use `write_disposition=’replace’`in your resources.

**Append**: appends the new data to the destination. Use `write_disposition=’append’`.

**Upsert or merge**: not currently supported, but it is on our immediate roadmap. Until we offer support for it, you can use full or append load with a `dbt` merge materialisation to generate a merge or similar.

### How to do incremental loading

To do incremental loading, we need to
- figure out where we left off with the last load (e.g. “last value”, “last updated at”, etc)
- request the new part only (how we do this depends on the source API)
- add the increment into the destination by appending stateless data (e.g. events)

Preserving the last value in state.

The state is a python dictionary which gets committed atomically with the data; it is in essence metadata that you can keep in sync with your data.

For the purpose of preserving the “last value” or similar loading checkpoints, we can open a dlt state dictionary with a key and a default value as below. When the resource is executed and the data is loaded, the yielded resource data will be loaded at the same time with the update to the state.

```python
@resource()
def persons():
	# Get a last value from loaded metadata. If not exist, get None
	last_val = dlt.state().setdefault("last_updated_person", None)
	# get data and yield it
	data = get_data(start_from=last_val)
	yield data
	# set last val
	last_val = data["last_updated_at"]
```

### Using the dlt state

Step by step explanation of how to get or set the state:
1. We can use the function `var = dlt.state().setdefault("key", [])`. This allows us to retrieve the values of `key`. If `key` was not set yet, we will get the default value `[]` instead
2. We now can treat `var` as a python list - We can append new values to it, or if applicable we can read the values from previous loads.
3. On pipeline run, the data will load, and the new `var`'s value will get saved in the state. The state is stored at the destination, so it will be available on subsequent runs.


### Examining an incremental pipeline

Let’s look at the `player_games` resource from the chess pipeline:
- dlt state is like a python dictionary that is preserved in the destination
- Even if our run environment is not persistent, the state is persisted anyway
- It’s accessible in Python like any global variable dictionary (i.e. usable in any function)
- You can set a default for the case where there is no previous run history (i.e. first run)
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

    loaded_archives_cache = dlt.state().setdefault("archives", [])

		# as far as python is concerned, this variable behaves like
    # loaded_archives_cache = state['archives'] or []
    # and when we can add to this list, and finally
    # when the data is loaded, the cache is updated with our loaded_archives_cache

		# get archives
		# if not in cache, yield the data and cache the URL
    archives = players_archives(chess_url, players)
    for url in archives:
        if url not in loaded_archives_cache:
						# add URL to cache and yield the associated data
						loaded_archives_cache.append(url)
		        r = requests.get(url)
		        r.raise_for_status()
		        yield r.json().get("games", [])
        else:
            print(f"skipping archive {url}")
```

In the twitter search case, we can do it for every search term:

```python
@dlt.resource(write_disposition="append")
def search_tweets(twitter_bearer_token=dlt.secrets.value, search_terms=None, start_time=None, end_time=None, last_value=None):
    headers = _headers(twitter_bearer_token)
    for search_term in search_terms:
				# make cache for each term
        last_value_cache = dlt.state().setdefault(f"last_value_{search_term}", None)
        print(f'last_value_cache: {last_value_cache}')
        params = {...
                  }
        url = "https://api.twitter.com/2/tweets/search/recent"
        response = _paginated_get(url, headers=headers, params=params)
        for page in response:
            page['search_term'] = search_term
            last_id = page.get('meta', {}).get('newest_id', 0)
            last_value_cache = max(last_value_cache or 0, int(last_id))
						# print the value for each search term
            print(f'new_last_value_cache for term {search_term}: {last_value_cache}')
            yield page
```

- To reset the dlt state, you need to delete it from the destination
- Dropping the dataset or the state table would do it
- If you drop only the state table, be careful about not duplicating data on the next load
- Alternatively, we could get the state from the data itself via a SQL query
- The dlt pipeline exposes the destination’s SQL client:

```python
if __name__ == "__main__" :

    dataset_name ='tweets'
    last_value_query = f'select max(id) from {dataset_name}.search_tweets'
		# init pipeline with credentials, so we can use its client
    pipeline = dlt.pipeline(destination="bigquery", dataset_name="tweets")
		# try to get data.
    # Might fail for multiple reasons so we should do some error handling

    try:
        with pipeline.sql_client() as client:
            res = client.execute_sql(last_value_query)
            last_value = res[0][0]
    except:
		# todo: error handling - we would need to make sure this only happens for a "table not found" error.
        last_value = None

    info = pipeline.run(twitter_search(search_terms=search_terms, last_value = last_value))
```
