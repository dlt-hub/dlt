---
title: Chess.com
description: dlt verified source for Chess.com API
keywords: [chess.com api, chess.com verified source, verified source, chess.com, chess]
---
import Header from './_source-info-header.md';

# Chess.com

<Header/>

[Chess.com](https://www.chess.com/) is an online platform that offers services for chess
enthusiasts. It includes online chess games, tournaments, lessons, and more.

Resources that can be loaded using this verified source are:

| Name             | Description                                                            |
| ---------------- | ---------------------------------------------------------------------- |
| players_profiles | retrieves player profiles for a list of player usernames                |
| players_archives | retrieves URL to game archives for specified players                    |
| players_games    | retrieves players' games that happened between start_month and end_month |

## Setup guide

### Grab credentials

[Chess.com API](https://www.chess.com/news/view/published-data-api) is a public API that does not
require authentication or including secrets in `secrets.toml`.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init chess duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/chess_pipeline.py)
   with Chess.com as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source.md).

### Add credentials

To add credentials to your destination, follow the instructions in the
[destination](../../dlt-ecosystem/destinations) documentation. This will ensure that your data is
properly routed to its final destination.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```sh
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python chess_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `chess_pipeline`, you may also
   use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `source`

This is a `dlt.source` function for the Chess.com API named "chess", which returns a sequence of
DltResource objects. We'll discuss these in subsequent sections as resources.

```py
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

`start_month` and `end_month`: These optional parameters specify the time period for which you want
to fetch game data (in "YYYY/MM" format).

### Resource `players_profiles`

This is a `dlt.resource` function, which returns player profiles for a list of player usernames.

```py
@dlt.resource(write_disposition="replace")
def players_profiles(players: List[str]) -> Iterator[TDataItem]:

    @dlt.defer
    def _get_profile(username: str) -> TDataItem:
        return get_path_with_retry(f"player/{username}")
    
    for username in players:
        yield _get_profile(username)
```

`players`: This is a list of player usernames for which you want to fetch profile data.

It uses the `@dlt.defer` decorator to enable parallel run in a thread pool.

### Resource `players_archives`

This is a `dlt.resource` function, which returns a URL to game archives for specified players.

```py
@dlt.resource(write_disposition="replace", selected=False)
def players_archives(players: List[str]) -> Iterator[List[TDataItem]]:
    ...
```

`players`: This is a list of player usernames for which you want to fetch archives.

`selected=False`: This parameter means that this resource is not selected by default when the pipeline
runs.

### Resource `players_games`

This incremental resource takes data from players and returns games for the last month if not
specified otherwise.

```py
@dlt.resource(write_disposition="append")
def players_games(
    players: List[str], start_month: str = None, end_month: str = None
) -> Iterator[TDataItems]:
    # gets a list of already checked (loaded) archives.
    checked_archives = dlt.current.resource_state().setdefault("archives", [])
    yield {}  # return your retrieved data here
```

`players`: This is a list of player usernames for which you want to fetch games.

The list `checked_archives` is used to load new archives and skip the ones already loaded. It uses state
to initialize a list called "checked_archives" from the current resource
[state](../../general-usage/state).

### Resource `players_online_status`

The `players_online_status` is a `dlt.resource` function that checks the current online status of multiple chess players. It
retrieves their username, status, last login date, and check time.

## Customization



### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

To create your data loading pipeline for players and load data, follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="chess_pipeline", # Use a custom name if desired
       destination="duckdb", # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="chess_players_games_data", # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our [documentation](../../general-usage/pipeline).

1. To load the data from all the resources for specific players (e.g., for November), you can utilize the `source` method as follows:

   ```py
   # Loads games for Nov 2022
   data = source(
       ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
       start_month="2022/11",
       end_month="2022/11",
   )
   ```

1. Use the method `pipeline.run()` to execute the pipeline.

   ```py
   info = pipeline.run(data)
   # print the information on data that was loaded
   print(info)
   ```

1. To load data from specific resources like "players_games" and "player_profiles", modify the above code as:

   ```py
   info = pipeline.run(data.with_resources("players_games", "players_profiles"))
   # print the information on data that was loaded
   print(info)
   ```

<!--@@@DLT_TUBA chess-->

