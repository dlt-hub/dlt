---
title: "Build a dlt pipeline"
description: Build a data pipeline with dlt
keywords: [getting started, quick start, basic examples]
---

This tutorial introduces you to foundational dlt concepts, demonstrating how to build a custom data pipeline that loads data from pure Python data structures to DuckDB. It starts with a simple example and progresses to more advanced topics and usage scenarios.

## What you will learn

- Loading data from a list of Python dictionaries into DuckDB.
- Low-level API usage with a built-in HTTP client.
- Understand and manage data loading behaviors.
- Incrementally load new data and deduplicate existing data.
- Dynamic resource creation and reducing code redundancy.
- Group resources into sources.
- Securely handle secrets.
- Make reusable data sources.

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

To allow a sneak peek and basic discovery, you can take advantage of [built-in integration with Streamlit](../reference/command-line-interface#show-tables-and-data-in-the-destination):

```sh
dlt pipeline quick_start show
```

**quick_start** is the name of the pipeline from the script above. If you do not have Streamlit installed yet, do:

```sh
pip install streamlit
```

Now you should see the **users** table:

![Streamlit Explore data](/img/streamlit-new.png)
Streamlit Explore data. Schema and data for a test pipeline “quick_start”.

:::tip
`dlt` works in Jupyter Notebook and Google Colab! See our [Quickstart Colab Demo.](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)

Looking for the source code of all the snippets? You can find and run them [from this repository](https://github.com/dlt-hub/dlt/blob/devel/docs/website/docs/getting-started-snippets.py).
:::

Now that you have a basic understanding of how to get started with dlt, you might be eager to dive deeper. For that, we need to switch to a more advanced data source - the GitHub API. We will load issues from our [dlt-hub/dlt](https://github.com/dlt-hub/dlt) repository.

:::note
This tutorial uses the GitHub REST API for demonstration purposes only. If you need to read data from a REST API, consider using dlt's REST API source. Check out the [REST API source tutorial](./rest-api) for a quick start or the [REST API source reference](../dlt-ecosystem/verified-sources/rest_api) for more details.
:::

## Create a pipeline

First, we need to create a [pipeline](../general-usage/pipeline). Pipelines are the main building blocks of `dlt` and are used to load data from sources to destinations. Open your favorite text editor and create a file called `github_issues.py`. Add the following code to it:

<!--@@@DLT_SNIPPET basic_api-->


Here's what the code above does:
1. It makes a request to the GitHub API endpoint and checks if the response is successful.
2. Then, it creates a dlt pipeline with the name `github_issues` and specifies that the data should be loaded to the `duckdb` destination and the `github_data` dataset. Nothing gets loaded yet.
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
One option is to tell `dlt` to replace the data in existing tables in the destination by using the `replace` write disposition. Change the `github_issues.py` script to the following:

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

Run this script twice to see that the **issues** table still contains only one copy of the data.

:::tip
What if the API has changed and new fields get added to the response?
`dlt` will migrate your tables!
See the `replace` mode and table schema migration in action in our [Schema evolution colab demo](https://colab.research.google.com/drive/1H6HKFi-U1V4p0afVucw_Jzv1oiFbH2bu#scrollTo=e4y4sQ78P_OM).
:::

Learn more:

- [Full load - how to replace your data](../general-usage/full-loading).
- [Append, replace, and merge your tables](../general-usage/incremental-loading).

## Declare loading behavior

So far, we have been passing the data to the `run` method directly. This is a quick way to get started. However, frequently, you receive data in chunks, and you want to load it as it arrives. For example, you might want to load data from an API endpoint with pagination or a large file that does not fit in memory. In such cases, you can use Python generators as a data source.

You can pass a generator to the `run` method directly or use the `@dlt.resource` decorator to turn the generator into a [dlt resource](../general-usage/resource). The decorator allows you to specify the loading behavior and relevant resource parameters.

### Load only new data (incremental loading)

Let's improve our GitHub API example and get only issues that were created since the last load.
Instead of using the `replace` write disposition and downloading all issues each time the pipeline is run, we do the following:

<!--@@@DLT_SNIPPET incremental-->


Let's take a closer look at the code above.

We use the `@dlt.resource` decorator to declare the table name into which data will be loaded and specify the `append` write disposition.

We request issues for the dlt-hub/dlt repository ordered by the **created_at** field (descending) and yield them page by page in the `get_issues` generator function.

We also use `dlt.sources.incremental` to track the `created_at` field present in each issue to filter in the newly created ones.

Now run the script. It loads all the issues from our repo to `duckdb`. Run it again, and you can see that no issues got added (if no issues were created in the meantime).

Now you can run this script on a daily schedule, and each day you’ll load only issues created after the time of the previous pipeline run.

:::tip
Between pipeline runs, `dlt` keeps the state in the same database it loaded data into.
Peek into that state, the tables loaded, and get other information with:

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
It will ignore any updates to **existing** issue text, emoji reactions, etc.
To always get fresh content of all the issues, combine incremental load with the `merge` write disposition,
like in the script below.

<!--@@@DLT_SNIPPET incremental_merge-->


Above, we add the `primary_key` argument to the `dlt.resource()` that tells `dlt` how to identify the issues in the database to find duplicates whose content it will merge.

Note that we now track the `updated_at` field — so we filter in all issues **updated** since the last pipeline run (which also includes those newly created).

Pay attention to how we use the **since** parameter from the [GitHub API](https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues)
and `updated_at.last_value` to tell GitHub to return issues updated only **after** the date we pass. `updated_at.last_value` holds the last `updated_at` value from the previous run.

[Learn more about merge write disposition](../general-usage/incremental-loading#merge-incremental_loading).

## Using pagination helper

In the previous examples, we used the `requests` library to make HTTP requests to the GitHub API and handled pagination manually. `dlt` has a built-in [REST client](../general-usage/http/rest-client.md) that simplifies API requests. We'll use the `paginate()` helper from it for the next example. The `paginate` function takes a URL and optional parameters (quite similar to `requests`) and returns a generator that yields pages of data.

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
3. We're not explicitly setting up pagination; `paginate()` handles it for us. Magic! Under the hood, `paginate()` analyzes the response and detects the pagination method used by the API. Read more about pagination in the [REST client documentation](../general-usage/http/rest-client.md#paginating-api-responses).

If you want to take full advantage of the `dlt` library, then we strongly suggest that you build your sources out of existing building blocks:
To make the most of `dlt`, consider the following:

## Use source decorator

In the previous step, we loaded issues from the GitHub API. Now we'll load comments from the API as well. Here's a sample [dlt resource](../general-usage/resource) that does that:

```py
import dlt
from dlt.sources.helpers.rest_client import paginate

@dlt.resource(
    table_name="comments",
    write_disposition="merge",
    primary_key="id",
)
def get_comments(
    updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/comments",
        params={"per_page": 100}
    ):
        yield page
```

We can load this resource separately from the issues resource; however, loading both issues and comments in one go is more efficient. To do that, we'll use the `@dlt.source` decorator on a function that returns a list of resources:

```py
@dlt.source
def github_source():
    return [get_issues, get_comments]
```

`github_source()` groups resources into a [source](../general-usage/source). A dlt source is a logical grouping of resources. You use it to group resources that belong together, for example, to load data from the same API. Loading data from a source can be run in a single pipeline. Here's what our updated script looks like:

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
        }
    ):
        yield page


@dlt.resource(
    table_name="comments",
    write_disposition="merge",
    primary_key="id",
)
def get_comments(
    updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/comments",
        params={
            "since": updated_at.last_value,
            "per_page": 100,
        }
    ):
        yield page


@dlt.source
def github_source():
    return [get_issues, get_comments]


pipeline = dlt.pipeline(
    pipeline_name='github_with_source',
    destination='duckdb',
    dataset_name='github_data',
)

load_info = pipeline.run(github_source())
print(load_info)
```

### Dynamic resources

You've noticed that there's a lot of code duplication in the `get_issues` and `get_comments` functions. We can reduce that by extracting the common fetching code into a separate function and using it in both resources. Even better, we can use `dlt.resource` as a function and pass it the `fetch_github_data()` generator function directly. Here's the refactored code:

```py
import dlt
from dlt.sources.helpers.rest_client import paginate

BASE_GITHUB_URL = "https://api.github.com/repos/dlt-hub/dlt"

def fetch_github_data(endpoint, params={}):
    url = f"{BASE_GITHUB_URL}/{endpoint}"
    return paginate(url, params=params)

@dlt.source
def github_source():
    for endpoint in ["issues", "comments"]:
        params = {"per_page": 100}
        yield dlt.resource(
            fetch_github_data(endpoint, params),
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )

pipeline = dlt.pipeline(
    pipeline_name='github_dynamic_source',
    destination='duckdb',
    dataset_name='github_data',
)
load_info = pipeline.run(github_source())
row_counts = pipeline.last_trace.last_normalize_info
```

## Handle secrets

For the next step, we'd want to get the [number of repository clones](https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones) for our dlt repo from the GitHub API. However, the `traffic/clones` endpoint that returns the data requires [authentication](https://docs.github.com/en/rest/overview/authenticating-to-the-rest-api?apiVersion=2022-11-28).

Let's handle this by changing our `fetch_github_data()` function first:

```py
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

def fetch_github_data(endpoint, params={}, access_token=None):
    url = f"{BASE_GITHUB_URL}/{endpoint}"
    return paginate(
        url,
        params=params,
        auth=BearerTokenAuth(token=access_token) if access_token else None,
    )


@dlt.source
def github_source(access_token):
    for endpoint in ["issues", "comments", "traffic/clones"]:
        params = {"per_page": 100}
        yield dlt.resource(
            fetch_github_data(endpoint, params, access_token),
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )

...
```

Here, we added an `access_token` parameter and now we can use it to pass the access token to the request:

```py
load_info = pipeline.run(github_source(access_token="ghp_XXXXX"))
```

It's a good start. But we'd want to follow the best practices and not hardcode the token in the script. One option is to set the token as an environment variable, load it with `os.getenv()`, and pass it around as a parameter. dlt offers a more convenient way to handle secrets and credentials: it lets you inject the arguments using a special `dlt.secrets.value` argument value.

To use it, change the `github_source()` function to:

```py
@dlt.source
def github_source(
    access_token: str = dlt.secrets.value,
):
    ...
```

When you add `dlt.secrets.value` as a default value for an argument, `dlt` will try to load and inject this value from different configuration sources in the following order:

1. Special environment variables.
2. `secrets.toml` file.

The `secrets.toml` file is located in the `~/.dlt` folder (for global configuration) or in the `.dlt` folder in the project folder (for project-specific configuration).

Let's add the token to the `~/.dlt/secrets.toml` file:

```toml
[github_with_source_secrets]
access_token = "ghp_A...3aRY"
```

Now we can run the script and it will load the data from the `traffic/clones` endpoint:

```py
...

@dlt.source
def github_source(
    access_token: str = dlt.secrets.value,
):
    for endpoint in ["issues", "comments", "traffic/clones"]:
        params = {"per_page": 100}
        yield dlt.resource(
            fetch_github_data(endpoint, params, access_token),
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )


pipeline = dlt.pipeline(
    pipeline_name="github_with_source_secrets",
    destination="duckdb",
    dataset_name="github_data",
)
load_info = pipeline.run(github_source())
```

## Configurable sources

The next step is to make our dlt GitHub source reusable so it can load data from any GitHub repo. We'll do that by changing both the `github_source()` and `fetch_github_data()` functions to accept the repo name as a parameter:

```py
import dlt
from dlt.sources.helpers.rest_client import paginate

BASE_GITHUB_URL = "https://api.github.com/repos/{repo_name}"


def fetch_github_data(repo_name, endpoint, params={}, access_token=None):
    """Fetch data from the GitHub API based on repo_name, endpoint, and params."""
    url = BASE_GITHUB_URL.format(repo_name=repo_name) + f"/{endpoint}"
    return paginate(
        url,
        params=params,
        auth=BearerTokenAuth(token=access_token) if access_token else None,
    )


@dlt.source
def github_source(
    repo_name: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
):
    for endpoint in ["issues", "comments", "traffic/clones"]:
        params = {"per_page": 100}
        yield dlt.resource(
            fetch_github_data(repo_name, endpoint, params, access_token),
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )


pipeline = dlt.pipeline(
    pipeline_name="github_with_source_secrets",
    destination="duckdb",
    dataset_name="github_data",
)
load_info = pipeline.run(github_source())
```

Next, create a `.dlt/config.toml` file in the project folder and add the `repo_name` parameter to it:

```toml
[github_with_source_secrets]
repo_name = "dlt-hub/dlt"
```

That's it! Now you have a reusable source that can load data from any GitHub repo.

## What’s next

Congratulations on completing the tutorial! You've come a long way since the [getting started](../intro) guide. By now, you've mastered loading data from various GitHub API endpoints, organizing resources into sources, managing secrets securely, and creating reusable sources. You can use these skills to build your own pipelines and load data from any source.

Interested in learning more? Here are some suggestions:
1. You've been running your pipelines locally. Learn how to [deploy and run them in the cloud](../walkthroughs/deploy-a-pipeline/).
2. Dive deeper into how dlt works by reading the [Using dlt](../general-usage) section. Some highlights:
    - [Set up "last value" incremental loading](../general-usage/incremental-loading#incremental_loading-with-last-value).
    - Learn about data loading strategies: [append, replace, and merge](../general-usage/incremental-loading).
    - [Connect the transformers to the resources](../general-usage/resource#feeding-data-from-one-resource-into-another) to load additional data or enrich it.
    - [Customize your data schema—set primary and merge keys, define column nullability, and specify data types](../general-usage/resource#define-schema).
    - [Create your resources dynamically from data](../general-usage/source#create-resources-dynamically).
    - [Transform your data before loading](../general-usage/resource#customize-resources) and see some [examples of customizations like column renames and anonymization](../general-usage/customising-pipelines/renaming_columns).
    - Employ data transformations using [SQL](../dlt-ecosystem/transformations/sql) or [Pandas](../dlt-ecosystem/transformations/sql).
    - [Pass config and credentials into your sources and resources](../general-usage/credentials).
    - [Run in production: inspecting, tracing, retry policies, and cleaning up](../running-in-production/running).
    - [Run resources in parallel, optimize buffers, and local storage](../reference/performance.md)
    - [Use REST API client helpers](../general-usage/http/rest-client.md) to simplify working with REST APIs.
3. Explore [destinations](../dlt-ecosystem/destinations/) and [sources](../dlt-ecosystem/verified-sources/) provided by us and the community.
4. Explore the [Examples](../examples) section to see how dlt can be used in real-world scenarios.

