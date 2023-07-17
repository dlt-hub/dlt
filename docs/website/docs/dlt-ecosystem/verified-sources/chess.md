---
title: Chess.com
description: dlt verified source for Chess.com API
keywords: [chess.com api, chess.com verified source, chess.com]
---

# Chess.com

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

Chess.com is an online platform that offers services for chess enthusiasts. It includes online chess games, tournaments, lessons, and more.

Resources that can be loaded using this verified source are:

| Name              | Description                                                              |
|-------------------|--------------------------------------------------------------------------|
| players_profiles  | retrives player profiles for a list of player usernames                  |
| players_archives  | retrives url to game archives for specified players                      |
| players_games     | retrives players games that happened between start_month and end_month   |


## Setup Guide

### Grab credentials
Chess.com API is a public API that does not require authentication or including secrets in secrets.toml.

### Initialize the verified source

To get started with your data pipeline, follow these steps:
1. Enter the following command:

   ```bash
   dlt init chess duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/asana_dlt_pipeline.py) with Chess.com as the source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

2. If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../destinations).

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

For more information, read the [Walkthrough: Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

To add credentials to your destination, follow the instructions in the [destination documentation](../../dlt-ecosystem/destinations). This will ensure that your data is properly routed to its final destination.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```bash
   pip install -r requirements.txt
   ```

2. You're now ready to run the pipeline! To get started, run the following command:

   ```bash
   python3 chess_pipeline.py
   ```

3. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```
   
   For example, the `pipeline_name` for the above pipeline example is `chess_pipeline`, you may also use any
   custom name instead.

For more information, read the [Walkthrough: Run a pipeline.](../../walkthroughs/run-a-pipeline)

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Source `source`

```python
dlt.source(name="chess")
def source(
    players: List[str], start_month: str = None, end_month: str = None
) -> Sequence[DltResource]: 
   return (
         players_profiles(players),
         players_archives(players),
         players_games(players, start_month=start_month, end_month=end_month),
         players_online_status(players),
         )
```
`players`: This is a list of player usernames for which you want to fetch data.
`start_month` and `end_month`: These optional parameters specify the time period for which you want to fetch game data. (In  "YYYY/MM" format).

The above function is a dlt.source function for the Chess.com API named "chess", which returns a sequence of DltResource objects. That we'll discuss subsequently as resources. 

### Resource `players_profiles`

```python
@dlt.resource(write_disposition="replace")
def players_profiles(players: List[str]) -> Iterator[TDataItem]:
    
    @dlt.defer
      def _get_profile(username: str) -> TDataItem:
          return get_path_with_retry(f"player/{username}")

       for username in players:
         yield _get_profile(username)
```

 `players`: is a list of player usernames for which you want to fetch profile data.
 
 The `_get_profile` function fetches profile data for a single player using an API request. The `get_path_with_retry` function handles errors and makes the request. The `for` loop iterates through a list of players, yielding their profile data.

### Resource `players_archives`

```python
@dlt.resource(write_disposition="replace", selected=False)
def players_archives(players: List[str]) -> Iterator[List[TDataItem]]:
    
     for username in players:
        data = get_path_with_retry(f"player/{username}/games/archives")
        yield data.get("archives", [])
```

`players`: is a list of player usernames for which you want to fetch archives.

`selected=False`: parameter means that this resource is not selected by default when the pipeline runs.

 The `get_path_with_retry` function fetches their archives using an API request within a `for` loop iterating over a list of players. The loop yields the list of archives or an empty list if there are none.

### Resource `players_games`

```python
@dlt.resource(write_disposition="append")
def players_games(
    players: List[str], start_month: str = None, end_month: str = None
) -> Iterator[Callable[[], List[TDataItem]]]:

 # Yields `players` games that happened between `start_month` and `end_month`.
```
`players`: is a list of player usernames for which you want to fetch games.

The `players_games` function gets chess games for a group of players during a set time period. Provides player usernames and specify start/end month. It checks for valid archives within the time range and creates a callable URL for the games. The `get_path_with_retry` function fetches their archives using an API request with a `for` loop iterating over a list of players. The loop yields the list of archives or an empty list if there are none. 

### Resource `players_online_status`

```python
@dlt.resource(write_disposition="append")
def players_online_status(players: List[str]) -> Iterator[TDataItem]:

    for player in players:
        status = get_url_with_retry(
            "%suser/popup/%s" % (UNOFFICIAL_CHESS_API_URL, player)
        )
        yield {
            "username": player,
            "onlineStatus": status["onlineStatus"],
            "lastLoginDate": status["lastLoginDate"],
            "check_time": pendulum.now(),  # dlt can deal with native python dates
        }
```

`players`: is a list of player usernames for which you want to fetch online status.

The `players_online_status` function to check the online status of multiple chess players. It retrieves their username, status, last login date, and check time using the Chess.com API, without altering any existing data.

## Customization
### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

To create your data pipeline using incremental loading for players as mentioned below and load data, follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

    ```python
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline", # Use a custom name if desired
        destination="duckdb", # Choose the appropriate destination (e.g., duckdb, redshift, post) 
        dataset_name="chess_players_games_data", # Use a custom name if desired
    )
    ```
    
    To read more about pipeline configuration, please refer to our [documentation](../../general-usage/pipeline).
    
2.  To load the data from all the resources, you can utilize the `source` method as follows:
    
    ```python
    data = source(
        ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
        start_month="2022/11"",
        end_month="2022/11",
    )
    # Loads games for Nov 2022
    ```
    
3. Use the method `pipeline.run()` to execute the pipeline.

    ```python
    info = pipeline.run(data)
    # print the information on data that was loaded
    print(info)
    ```
    
4. To load data from specific resources like "players_games" and "player_profiles", modify the above code as:

      ```python
      info = pipeline.run(data.with_resources("players_games", "players_profiles"))
    # print the information on data that was loaded
    print(info)
    ```
    
5. To load the data incrementally, modify the `source` method as follows and re-run the pipeline.

    ```python
    data = source(
        ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
        start_month="2022/11"",
        end_month="2022/12",
    )
    # Loads games for Nov 2022 to Dec 2022
    ```
    >Maintaining the same pipeline and dataset names is crucial for preserving the [state](../../general-usage/state) of the last run, including the end date required for [incremental data loading.](../../general-usage/incremental-loading). Modifying these names can cause a ["full_refresh"](../../general-usage/pipeline#do-experiments-with-full-refresh).
    
