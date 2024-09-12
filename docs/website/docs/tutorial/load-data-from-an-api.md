---
title: "Build a dlt pipeline"
description: quick start with dlt
keywords: [getting started, quick start, basic examples]
---

This guide explores core dlt concepts and shows how to build a custom pipeline that loads data from pure Python data structures into DuckDB. We're start with a simple example and then dive into more advanced topics using data from the GitHub API.

## Prerequisites

- Python 3.9 or higher installed
- Virtual environment set up

## Installing dlt

Before we start, make sure you have a Python virtual environment set up. Follow the instructions in the [installation guide](../reference/installation) to create a new virtual environment and install dlt.

Verify that dlt is installed by running the following command in your terminal:

```sh
dlt --version
```

## Quick start

For starters, let's load a list of Python dictionaries into DuckDB and inspect the created dataset. Here is the code:

```py
import dlt

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

pipeline = dlt.pipeline(
    pipeline_name="quick_start", destination="duckdb", dataset_name="mydata"
)
load_info = pipeline.run(data, table_name="users")

print(load_info)
```


When you look at the code above, you can see that we:
1. Import the `dlt` library.
2. Define our data to load.
3. Create a pipeline that loads data into DuckDB. Here we also specify the `pipeline_name` and `dataset_name`. We'll use both in a moment.
4. Run the pipeline.

Save this Python script with the name `quick_start_pipeline.py` and run the following command:

```sh
python quick_start_pipeline.py
```

The output should look like:

```sh
Pipeline quick_start completed in 0.59 seconds
1 load package(s) were loaded to destination duckdb and into dataset mydata
The duckdb destination used duckdb:////home/user-name/quick_start/quick_start.duckdb location to store data
Load package 1692364844.460054 is LOADED and contains no failed jobs
```

`dlt` just created a database schema called **mydata** (the `dataset_name`) with a table **users** in it.

### Explore the data

To allow sneak peek and basic discovery you can take advantage of [built-in integration with Strealmit](reference/command-line-interface#show-tables-and-data-in-the-destination):

```sh
dlt pipeline quick_start show
```

**quick_start** is the name of the pipeline from the script above. If you do not have Streamlit installed yet do:

```sh
pip install streamlit
```

Now you should see the **users** table:

![Streamlit Explore data](/img/streamlit-new.png)
Streamlit Explore data. Schema and data for a test pipeline “quick_start”.

:::tip
`dlt` works in Jupyter Notebook and Google Colab! See our [Quickstart Colab Demo.](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)

Looking for source code of all the snippets? You can find and run them [from this repository](https://github.com/dlt-hub/dlt/blob/devel/docs/website/docs/getting-started-snippets.py).
:::


Now that you have a basic understanding of how to get started with dlt, you might be eager to dive deeper. For that we need to switch to a more advanced data source - the GitHub API. We will load issues from our [dlt-hub/dlt](https://github.com/dlt-hub/dlt) repository.

## Create a pipeline

First, we need to create a [pipeline](../general-usage/pipeline). Pipelines are the main building blocks of `dlt` and are used to load data from sources to destinations. Open your favorite text editor and create a file called `github_issues.py`. Add the following code to it:

<!--@@@DLT_SNIPPET basic_api-->


Here's what the code above does:
1. It makes a request to the GitHub API endpoint and checks if the response is successful.
2. Then it creates a dlt pipeline with the name `github_issues` and specifies that the data should be loaded to the `duckdb` destination and the `github_data` dataset. Nothing gets loaded yet.
3. Finally, it runs the pipeline with the data from the API response (`response.json()`) and specifies that the data should be loaded to the `issues` table. The `run` method returns a `LoadInfo` object that contains information about the loaded data.

## Run the pipeline

Save `github_issues.py` and run the following command:

```sh
python github_issues.py
```

Once the data has been loaded, you can inspect the created dataset using the Streamlit app:

```sh
dlt pipeline github_issues show
```

## Append or replace your data

Try running the pipeline again with `python github_issues.py`. You will notice that the **issues** table contains two copies of the same data. This happens because the default load mode is `append`. It is very useful, for example, when you have daily data updates and you want to ingest them.

To get the latest data, we'd need to run the script again. But how to do that without duplicating the data?
One option is to tell `dlt` to replace the data in existing tables in the destination by using `replace` write disposition. Change the `github_issues.py` script to the following:

```py
import dlt
from dlt.sources.helpers import requests

# Specify the URL of the API endpoint
url = "https://api.github.com/repos/dlt-hub/dlt/issues"
# Make a request and check if it was successful
response = requests.get(url)
response.raise_for_status()

pipeline = dlt.pipeline(
    pipeline_name='github_issues',
    destination='duckdb',
    dataset_name='github_data',
)
# The response contains a list of issues
load_info = pipeline.run(
    response.json(),
    table_name="issues",
    write_disposition="replace"  # <-- Add this line
)

print(load_info)
```

Run this script twice to see that **issues** table still contains only one copy of the data.

:::tip
What if the API has changed and new fields get added to the response?
`dlt` will migrate your tables!
See the `replace` mode and table schema migration in action in our [Schema evolution colab demo](https://colab.research.google.com/drive/1H6HKFi-U1V4p0afVucw_Jzv1oiFbH2bu#scrollTo=e4y4sQ78P_OM).
:::

Learn more:

- [Full load - how to replace your data](../general-usage/full-loading).
- [Append, replace and merge your tables](../general-usage/incremental-loading).

## Declare loading behavior

So far we have been passing the data to the `run` method directly. This is a quick way to get started. However, frequently, you receive data in chunks, and you want to load it as it arrives. For example, you might want to load data from an API endpoint with pagination or a large file that does not fit in memory. In such cases, you can use Python generators as a data source.

You can pass a generator to the `run` method directly or use the `@dlt.resource` decorator to turn the generator into a [dlt resource](../general-usage/resource). The decorator allows you to specify the loading behavior and relevant resource parameters.

### Load only new data (incremental loading)

Let's improve our GitHub API example and get only issues that were created since last load.
Instead of using `replace` write disposition and downloading all issues each time the pipeline is run, we do the following:

<!--@@@DLT_SNIPPET incremental-->


Let's take a closer look at the code above.

We use the `@dlt.resource` decorator to declare the table name into which data will be loaded and specify the `append` write disposition.

We request issues for dlt-hub/dlt repository ordered by **created_at** field (descending) and yield them page by page in `get_issues` generator function.

We also use `dlt.sources.incremental` to track `created_at` field present in each issue to filter in the newly created.

Now run the script. It loads all the issues from our repo to `duckdb`. Run it again, and you can see that no issues got added (if no issues were created in the meantime).

Now you can run this script on a daily schedule and each day you’ll load only issues created after the time of the previous pipeline run.

:::tip
Between pipeline runs, `dlt` keeps the state in the same database it loaded data to.
Peek into that state, the tables loaded and get other information with:

```sh
dlt pipeline -v github_issues_incremental info
```
:::

Learn more:

- Declare your [resources](../general-usage/resource) and group them in [sources](../general-usage/source) using Python decorators.
- [Set up "last value" incremental loading.](../general-usage/incremental-loading#incremental_loading-with-last-value)
- [Inspect pipeline after loading.](../walkthroughs/run-a-pipeline#4-inspect-a-load-process)
- [`dlt` command line interface.](../reference/command-line-interface)

### Update and deduplicate your data

The script above finds **new** issues and adds them to the database.
It will ignore any updates to **existing** issue text, emoji reactions etc.
To get always fresh content of all the issues you combine incremental load with `merge` write disposition,
like in the script below.

<!--@@@DLT_SNIPPET incremental_merge-->


Above we add `primary_key` argument to the `dlt.resource()` that tells `dlt` how to identify the issues in the database to find duplicates which content it will merge.

Note that we now track the `updated_at` field — so we filter in all issues **updated** since the last pipeline run (which also includes those newly created).

Pay attention how we use **since** parameter from [GitHub API](https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues)
and `updated_at.last_value` to tell GitHub to return issues updated only **after** the date we pass. `updated_at.last_value` holds the last `updated_at` value from the previous run.

[Learn more about merge write disposition](../general-usage/incremental-loading#merge-incremental_loading).

## Using pagination helper

In the previous examples, we used the `requests` library to make HTTP requests to the GitHub API and handled pagination manually. `dlt` has the built-in [REST client](../general-usage/http/rest-client.md) that simplifies API requests. We'll pick the `paginate()` helper from it for the next example. The `paginate` function takes a URL and optional parameters (quite similar to `requests`) and returns a generator that yields pages of data.

Here's how the updated script looks:

```py
import dlt
from dlt.sources.helpers.rest_client import paginate

@dlt.resource(
    table_name="issues",
    write_disposition="merge",
    primary_key="id",
)
def get_issues(
    updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/issues",
        params={
            "since": updated_at.last_value,
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
            "state": "open",
        },
    ):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="github_issues_merge",
    destination="duckdb",
    dataset_name="github_data_merge",
)
load_info = pipeline.run(get_issues)
row_counts = pipeline.last_trace.last_normalize_info

print(row_counts)
print("------")
print(load_info)
```

Let's zoom in on the changes:

1. The `while` loop that handled pagination is replaced with reading pages from the `paginate()` generator.
2. `paginate()` takes the URL of the API endpoint and optional parameters. In this case, we pass the `since` parameter to get only issues updated after the last pipeline run.
3. We're not explicitly setting up pagination, `paginate()` handles it for us. Magic! Under the hood, `paginate()` analyzes the response and detects the pagination method used by the API. Read more about pagination in the [REST client documentation](../general-usage/http/rest-client.md#paginating-api-responses).

## Next steps

Continue your journey with the [Resource Grouping and Secrets](grouping-resources) tutorial.

If you want to take full advantage of the `dlt` library, then we strongly suggest that you build your sources out of existing **building blocks:**

- Pick your [destinations](../dlt-ecosystem/destinations/).
- Check [verified sources](../dlt-ecosystem/verified-sources/) provided by us and community.
- Access your data with [SQL](../dlt-ecosystem/transformations/sql) or [Pandas](../dlt-ecosystem/transformations/sql).
- [Append, replace and merge your tables](../general-usage/incremental-loading).
- [Set up "last value" incremental loading](../general-usage/incremental-loading#incremental_loading-with-last-value).
- [Set primary and merge keys, define the columns nullability and data types](../general-usage/resource#define-schema).
- [Use built-in requests client](../reference/performance#using-the-built-in-requests-client).