---
title: Resource groupping and secrets
description: Advanced tutorial on loading data from an API
keywords: [api, source, decorator, dynamic resource, github, tutorial]
---

This tutorial continues the [previous](load-data-from-an-api) part. We'll use the same GitHub API example to show you how to:
1. Load from other GitHub API endpoints.
1. Group your resources into sources for easier management.
2. Handle secrets.

## Use source decorator

In the previous tutorial, we loaded issues from the GitHub API. Now we'll prepare to load comments from the API as well. Here's a resource that does that:

```py
@dlt.resource(
    table_name="comments",
    write_disposition="merge",
    primary_key="id",
)
def get_comments(
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    url = f"https://api.github.com/repos/dlt-hub/dlt/comments?per_page=100"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]
```

We can load this resource separately, but we want to load both issues and comments in one go. To do that, we'll use the `@dlt.source` decorator on a function that returns a list of resources:

```py
@dlt.source
def github_source():
    return [get_issues, get_comments]
```

`github_source()` groups resources into a source that can be run as a single pipeline. Here's our new script:

```py
import dlt
from dlt.sources.helpers import requests

@dlt.resource(
    table_name="issues",
    write_disposition="merge",
    primary_key="id",
)
def get_issues(
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    url = f"https://api.github.com/repos/dlt-hub/dlt/issues?since={updated_at.last_value}&per_page=100&sort=updated&directions=desc&state=open"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]


@dlt.resource(
    table_name="comments",
    write_disposition="merge",
    primary_key="id",
)
def get_comments(
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    url = f"https://api.github.com/repos/dlt-hub/dlt/comments?per_page=100"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]


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

### Dynamic resource

You've noticed that there's a lot of code duplication in the `get_issues` and `get_comments` functions. We can reduce that by extracting the common fetching code into a separate function and use it in both resources. Even better, we can use `dlt.resource` as a function and pass it the `fetch_github_data()` generator function directly. Here's refactored code:

```python
import dlt
from dlt.sources.helpers import requests

BASE_GITHUB_URL = "https://api.github.com/repos/dlt-hub/dlt"

def fetch_github_data(endpoint, params={}):
    """Fetch data from GitHub API based on endpoint and params."""
    url = f"{BASE_GITHUB_URL}/{endpoint}"

    while True:
        response = requests.get(url, params=params)
        response.raise_for_status()
        yield response.json()

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]

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

For the next step we'd want to get the [number of repository clones](https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones) for our dlt repo from the GitHub API. However, the `traffic/clones` endpoint that returns the data requires authentication.

Let's handle this by changing our `fetch_github_data()` first:

```python
def fetch_github_data(endpoint, params={}, api_token=None):
    """Fetch data from GitHub API based on endpoint and params."""
    headers = {"Authorization": f"Bearer {api_token}"} if api_token else {}

    url = f"{BASE_GITHUB_URL}/{endpoint}"

    while True:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        yield response.json()

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]
```

Here, we added `api_token` parameter and used it to pass the authentication token to the request:

```python
load_info = pipeline.run(github_source(access_token="ghp_A...M"))
```

Good. But we'd want to follow the best practices and not hardcode the token in the script. One option is to set the token as an environment variable, load it with `os.getenv()` and pass it around as a parameter. dlt offers a more convenient way to handle secrets and credentials: it let you inject the arguments using a special `dlt.secrets.value` argument value.

To use it, change the `github_source()` function to:

```python
@dlt.source
def github_source(
    access_token: str = dlt.secrets.value,
):
    ...
```

When you add `dlt.secrets.value` as a default value for a parameter, `dlt` will try to load the value from the configuration (TODO: elaborate):
1. Special environment variable
2. secrets.toml file

Let's add the token to the `~/.dlt/secrets.toml` file:

```toml
[github_with_source_secrets]
access_token = "ghp_A...3aRY"
```

Now we can run the script and it will load the data from the `traffic/clones` endpoint:

```python
import dlt
from dlt.sources.helpers import requests

BASE_GITHUB_URL = "https://api.github.com/repos/dlt-hub/dlt"


def fetch_github_data(endpoint, params={}, api_token=None):
    """Fetch data from GitHub API based on endpoint and params."""
    headers = {"Authorization": f"Bearer {api_token}"} if api_token else {}

    url = f"{BASE_GITHUB_URL}/{endpoint}"

    while True:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        yield response.json()

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]


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

## What’s next

Congratulations on completing the tutorial! You've come a long way since the Getting started guide. By now, you've mastered loading data from various GitHub API endpoints, organizing resources into sources, managing secrets securely, and deploying pipelines to GitHub Actions. You can use these skills to build your own pipelines and load data from any source.

Interested in learning more? Here are some suggestions:
1. Dive deeper into how dlts works by reading the [Using dlt](general-usage) section. Some highlights:
    - [Connect the transformers to the resources](general-usage/resource#feeding-data-from-one-resource-into-another) to load additional data or enrich it.
    - [Create your resources dynamically from data](general-usage/source#create-resources-dynamically).
    - [Transform your data before loading](general-usage/resource#customize-resources) and see some [examples of customizations like column renames and anonymization](general-usage/customising-pipelines/renaming_columns).
    - [Pass config and credentials into your sources and resources](general-usage/credentials).
    - [Run in production: inspecting, tracing, retry policies and cleaning up](running-in-production/running).
    - [Run resources in parallel, optimize buffers and local storage](reference/performance.md)
2. Check out our [how-to guides](walkthroughs) to get answers to some common questions.
3. Explore [Examples](examples) section to see how dlt can be used in real-world scenarios.
