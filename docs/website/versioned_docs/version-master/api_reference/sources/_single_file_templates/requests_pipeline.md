---
sidebar_label: requests_pipeline
title: sources._single_file_templates.requests_pipeline
---

The Requests Pipeline Template provides a simple starting point for a dlt pipeline with the requests library

## players

```python
@dlt.resource(primary_key="player_id")
def players()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/requests_pipeline.py#L19)

Load player profiles from the chess api.

## players\_games

```python
@dlt.transformer(data_from=players, write_disposition="append")
def players_games(player: Any) -> Iterator[TDataItems]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/requests_pipeline.py#L30)

Load all games for each player in october 2022

## chess

```python
@dlt.source(name="chess")
def chess()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/_single_file_templates/requests_pipeline.py#L40)

A source function groups all resources into one schema.

