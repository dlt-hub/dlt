---
title: Lag / Attribution window
description: Use lag to refresh data within a specific time window
keywords: [incremental loading, lag, attribution window]
---

In many cases, certain data should be reacquired during incremental loading. For example, you may want to always capture the last 7 days of data when fetching daily analytics reports, or refresh Slack message replies with a moving window of 7 days. This is where the concept of "lag" or "attribution window" comes into play.

The `lag` parameter is a float that supports several types of incremental cursors: `datetime`, `date`, `integer`, and `float`. It can only be used with `last_value_func` set to `min` or `max` (default is `max`).

### How `lag` works

- **Datetime cursors**: `lag` is the number of seconds added or subtracted from the `last_value` loaded.
- **Date cursors**: `lag` represents days.
- **Numeric cursors (integer or float)**: `lag` respects the given unit of the cursor.

This flexibility allows `lag` to adapt to different data contexts.


### Example using `datetime` incremental cursor with `merge` as `write_disposition`

This example demonstrates how to use a `datetime` cursor with a `lag` parameter, applying `merge` as the `write_disposition`. The setup runs twice, and during the second run, the `lag` parameter re-fetches recent entries to capture updates.

1. **First Run**: Loads `initial_entries`.
2. **Second Run**: Loads `second_run_events` with the specified lag, refreshing previously loaded entries.

This setup demonstrates how `lag` ensures that a defined period of data remains refreshed, capturing updates or changes within the attribution window.

```py
pipeline = dlt.pipeline(
    destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
)

# Flag to indicate the second run
is_second_run = False

@dlt.resource(name="events", primary_key="id", write_disposition="merge")
def events_resource(
    _=dlt.sources.incremental("created_at", lag=3600, last_value_func=max)
):
    global is_second_run

    # Data for the initial run
    initial_entries = [
        {"id": 1, "created_at": "2023-03-03T01:00:00Z", "event": "1"},
        {"id": 2, "created_at": "2023-03-03T02:00:00Z", "event": "2"},  # lag applied during second run
    ]

    # Data for the second run
    second_run_events = [
        {"id": 1, "created_at": "2023-03-03T01:00:00Z", "event": "1_updated"},
        {"id": 2, "created_at": "2023-03-03T02:00:01Z", "event": "2_updated"},
        {"id": 3, "created_at": "2023-03-03T03:00:00Z", "event": "3"},
    ]

    # Yield data based on the current run
    yield from second_run_events if is_second_run else initial_entries

# Run the pipeline twice
pipeline.run(events_resource)
is_second_run = True  # Update flag for second run
pipeline.run(events_resource)
```
