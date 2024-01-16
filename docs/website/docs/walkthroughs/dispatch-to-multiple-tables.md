---
title: Dispatch stream of events to multiple tables
description: dispatch stream of events to tables by event type
keywords: [dispatch, stream, events, tables, event type]
---

This is a fun but practical example that reads GitHub events from **dlt** repository (such as issue or pull request created, comment added etc.).
Each event type is sent to a different table in `duckdb`.

```py
import dlt
from dlt.sources.helpers import requests

@dlt.resource(
    primary_key="id",
    table_name=lambda i: i["type"],
    write_disposition="append",
)
def repo_events(last_created_at=dlt.sources.incremental("created_at")):
    url = "https://api.github.com/repos/dlt-hub/dlt/events?per_page=100"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # stop requesting pages if the last element was already older than
        # the initial value
        # note: incremental will skip those items anyway, we just do not
        # want to use the api limits
        if last_created_at.start_out_of_range:
            break

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]


pipeline = dlt.pipeline(
    pipeline_name="github_events",
    destination="duckdb",
    dataset_name="github_events_data",
)
load_info = pipeline.run(repo_events)
row_counts = pipeline.last_trace.last_normalize_info

print(row_counts)
print("------")
print(load_info)
```

Events content never changes so we can use `append` write disposition and track new events using `created_at` field.

We name the tables using a function that receives an event data and returns table name: `table_name=lambda i: i["type"]`

Now run the script:

```shell
python github_events_dispatch.py
```

Peek at created tables:

```shell
dlt pipeline -v github_events info
dlt pipeline github_events trace
```

And preview the data:

```shell
dlt pipeline -v github_events show
```

:::tip
Some of the events produce tables with really many child tables. You can [control the level of table nesting](general-usage/source.md#reduce-the-nesting-level-of-generated-tables) with a decorator.


Another fun [Colab Demo](https://colab.research.google.com/drive/1BXvma_9R9MX8p_iSvHE4ebg90sUroty2#scrollTo=a3OcZolbaWGf) - we analyze reactions on duckdb repo!

:::

Learn more:
* [Change nesting of the tables](general-usage/source.md#reduce-the-nesting-level-of-generated-tables) with a decorator.
