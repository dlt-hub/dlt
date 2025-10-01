---
title: Dispatch stream of events to multiple tables in DuckDB
description: Learn how to efficiently dispatch a stream of GitHub events, categorized by event type, to different tables in DuckDB
keywords: [dispatch, stream, events, tables, event type]
---

This is a practical example of how to process [GitHub events](https://docs.github.com/en/rest/activity/events?apiVersion=2022-11-28) from the [dlt](https://github.com/dlt-hub/dlt) repository, such as issues or pull request creation, comments addition, etc.
We'll use the [GitHub API](https://docs.github.com/en/rest) to fetch the events and [duckdb](https://duckdb.org/) as a destination. Each event type will be sent to a separate table in DuckDB.

# Setup

1. Install dlt with duckdb support:

```sh
pip install "dlt[duckdb]"
```

2. Create a new file `github_events_dispatch.py` and paste the following code:

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

        # Stop requesting pages if the last element was already older than
        # the initial value.
        # Note: incremental will skip those items anyway, we just do not
        # want to use the API limits.
        if last_created_at.start_out_of_range:
            break

        # Get the next page.
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

In the code above, we define a resource `repo_events` that fetches events from the GitHub API.

Events content never changes, so we can use the `append` write disposition and track new events using the `created_at` field.

We name the tables using a function that receives event data and returns the table name: `table_name=lambda i: i["type"]`

3. Now run the script:

```sh
python github_events_dispatch.py
```

4. Peek at the created tables:

```sh
dlt pipeline -v github_events info
dlt pipeline github_events trace
```

5. And preview the data:

```sh
dlt pipeline -v github_events show
```

:::tip
Some of the events produce tables with many nested tables. You can [control the level of table nesting](general-usage/source.md#reduce-the-nesting-level-of-generated-tables) with a decorator.


Another fun [Colab Demo](https://colab.research.google.com/drive/1BXvma_9R9MX8p_iSvHE4ebg90sUroty2#scrollTo=a3OcZolbaWGf) - we analyze reactions on the duckdb repo!

:::

Learn more:
* [Change the nesting of the tables](general-usage/source.md#reduce-the-nesting-level-of-generated-tables) with a decorator.


