---
title: Advanced state management for incremental loading
description: Custom state tracking and lag/attribution windows
keywords: [incremental loading, state management, lag, attribution]
---

# Advanced state management for incremental loading

## Custom incremental loading with pipeline state

The pipeline state is a Python dictionary that gets committed atomically with the data; you can set values in it in your resources and on the next pipeline run, request them back.

The pipeline state is, in principle, scoped to the resource - all values of the state set by a resource are private and isolated from any other resource. You can also access the source-scoped state, which can be shared across resources.
[You can find more information on pipeline state here](../state.md#when-to-use-pipeline-state).

### Preserving the last value in resource state

For the purpose of preserving the "last value" or similar loading checkpoints, we can open a dlt state dictionary with a key and a default value as below. When the resource is executed and the data is loaded, the yielded resource data will be loaded at the same time with the update to the state.

In the two examples below, you see how the `dlt.sources.incremental` is working under the hood.

```py
@resource()
def tweets():
    # Get the last value from loaded metadata. If it does not exist, get None
    last_val = dlt.current.resource_state().setdefault("last_updated", None)
    # Get data and yield it
    data = _get_data(start_from=last_val)
    yield data
    # Change the state to the new value
    dlt.current.resource_state()["last_updated"] = data["last_timestamp"]
```

If we keep a list or a dictionary in the state, we can modify the underlying values in the objects, and thus we do not need to set the state back explicitly.

```py
@resource()
def tweets():
    # Get the last value from loaded metadata. If it does not exist, get None
    loaded_dates = dlt.current.resource_state().setdefault("days_loaded", [])
    # Do stuff: get data and add new values to the list
    # `loaded_date` is a reference to the `dlt.current.resource_state()["days_loaded"]` list
    # and thus modifying it modifies the state
    yield data
    loaded_dates.append('2023-01-01')
```

Step by step explanation of how to get or set the state:

1. We can use the function `var = dlt.current.resource_state().setdefault("key", [])`. This allows us to retrieve the values of `key`. If `key` was not set yet, we will get the default value `[]` instead.
2. We can now treat `var` as a Python list - We can append new values to it, or if applicable, we can read the values from previous loads.
3. On pipeline run, the data will load, and the new `var`'s value will get saved in the state. The state is stored at the destination, so it will be available on subsequent runs.

### Advanced state usage: storing a list of processed entities

Let's look at the `player_games` resource from the chess pipeline. The chess API has a method to request games archives for a given month. The task is to prevent the user from loading the same month data twice - even if the user makes a mistake and requests the same months range again:

- Our data is requested in 2 steps:
  - Get all available archives URLs.
  - Get the data from each URL.
- We will add the "chess archives" URLs to this list we created.
- This will allow us to track what data we have loaded.
- When the data is loaded, the list of archives is loaded with it.
- Later we can read this list and know what data has already been loaded.

In the following example, we initialize a variable with an empty list as a default:

```py
@dlt.resource(write_disposition="append")
def players_games(chess_url, players, start_month=None, end_month=None):
    loaded_archives_cache = dlt.current.resource_state().setdefault("archives", [])

    # As far as Python is concerned, this variable behaves like
    # loaded_archives_cache = state['archives'] or []
    # Afterwards, we can modify the list, and finally
    # when the data is loaded, the cache is updated with our loaded_archives_cache

    # Get archives for a given player
    archives = _get_players_archives(chess_url, players)
    for url in archives:
        # If not in cache, yield the data and cache the URL
        if url not in loaded_archives_cache:
            # Add URL to cache and yield the associated data
            loaded_archives_cache.append(url)
            r = requests.get(url)
            r.raise_for_status()
            yield r.json().get("games", [])
        else:
            print(f"Skipping archive {url}")
```

### Advanced state usage: tracking the last value for all search terms in Twitter API

```py
@dlt.resource(write_disposition="append")
def search_tweets(twitter_bearer_token=dlt.secrets.value, search_terms=None, start_time=None, end_time=None, last_value=None):
    headers = _headers(twitter_bearer_token)
    for search_term in search_terms:
        # Make cache for each term
        last_value_cache = dlt.current.resource_state().setdefault(f"last_value_{search_term}", None)
        print(f'last_value_cache: {last_value_cache}')
        params = {...}
        url = "https://api.twitter.com/2/tweets/search/recent"
        response = _get_paginated(url, headers=headers, params=params)
        for page in response:
            page['search_term'] = search_term
            last_id = page.get('meta', {}).get('newest_id', 0)
            # Set it back - not needed if we
            dlt.current.resource_state()[f"last_value_{search_term}"] = max(last_value_cache or 0, int(last_id))
            # Print the value for each search term
            print(f'new_last_value_cache for term {search_term}: {last_value_cache}')

            yield page
```
