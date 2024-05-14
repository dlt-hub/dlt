---
title: Resource grouping and secrets
description: Advanced tutorial on loading data from an API
keywords: [api, source, decorator, dynamic resource, github, tutorial]
---

This tutorial continues the [previous](load-data-from-an-api) part. We'll use the same GitHub API example to show you how to:
1. Load data from other GitHub API endpoints.
1. Group your resources into sources for easier management.
2. Handle secrets and configuration.

## Use source decorator

In the previous tutorial, we loaded issues from the GitHub API. Now we'll prepare to load comments from the API as well. Here's a sample [dlt resource](../general-usage/resource) that does that:

```py
import dlt
from dlt.sources.helpers.rest_client import paginate

@dlt.resource(
    table_name="comments",
    write_disposition="merge",
    primary_key="id",
)
def get_comments(
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/comments"
        params={"per_page": 100}
    ):
        yield page
```

We can load this resource separately from the issues resource, however loading both issues and comments in one go is more efficient. To do that, we'll use the `@dlt.source` decorator on a function that returns a list of resources:

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
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/issues"
        params={
            "since": updated_at.last_value,
            "per_page": 100,
            "sort": "updated",
            "directions": "desc",
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
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/comments"
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

You've noticed that there's a lot of code duplication in the `get_issues` and `get_comments` functions. We can reduce that by extracting the common fetching code into a separate function and use it in both resources. Even better, we can use `dlt.resource` as a function and pass it the `fetch_github_data()` generator function directly. Here's the refactored code:

```py
import dlt
from dlt.sources.helpers import requests

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

For the next step we'd want to get the [number of repository clones](https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones) for our dlt repo from the GitHub API. However, the `traffic/clones` endpoint that returns the data requires [authentication](https://docs.github.com/en/rest/overview/authenticating-to-the-rest-api?apiVersion=2022-11-28).

Let's handle this by changing our `fetch_github_data()` first:

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

Here, we added `access_token` parameter and now we can use it to pass the access token to the request:

```py
load_info = pipeline.run(github_source(access_token="ghp_XXXXX"))
```

It's a good start. But we'd want to follow the best practices and not hardcode the token in the script. One option is to set the token as an environment variable, load it with `os.getenv()` and pass it around as a parameter. dlt offers a more convenient way to handle secrets and credentials: it lets you inject the arguments using a special `dlt.secrets.value` argument value.

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

The `secret.toml` file is located in the `~/.dlt` folder (for global configuration) or in the `.dlt` folder in the project folder (for project-specific configuration).

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

The next step is to make our dlt GitHub source reusable so it can load data from any GitHub repo. We'll do that by changing both `github_source()` and `fetch_github_data()` functions to accept the repo name as a parameter:

```py
import dlt
from dlt.sources.helpers import requests

BASE_GITHUB_URL = "https://api.github.com/repos/{repo_name}"


def fetch_github_data(repo_name, endpoint, params={}, access_token=None):
    """Fetch data from GitHub API based on repo_name, endpoint, and params."""
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

Congratulations on completing the tutorial! You've come a long way since the [getting started](../getting-started) guide. By now, you've mastered loading data from various GitHub API endpoints, organizing resources into sources, managing secrets securely, and creating reusable sources. You can use these skills to build your own pipelines and load data from any source.

Interested in learning more? Here are some suggestions:
1. You've been running your pipelines locally. Learn how to [deploy and run them in the cloud](../walkthroughs/deploy-a-pipeline/).
2. Dive deeper into how dlt works by reading the [Using dlt](../general-usage) section. Some highlights:
    - [Connect the transformers to the resources](../general-usage/resource#feeding-data-from-one-resource-into-another) to load additional data or enrich it.
    - [Create your resources dynamically from data](../general-usage/source#create-resources-dynamically).
    - [Transform your data before loading](../general-usage/resource#customize-resources) and see some [examples of customizations like column renames and anonymization](../general-usage/customising-pipelines/renaming_columns).
    - [Pass config and credentials into your sources and resources](../general-usage/credentials).
    - [Run in production: inspecting, tracing, retry policies and cleaning up](../running-in-production/running).
    - [Run resources in parallel, optimize buffers and local storage](../reference/performance.md)
    - [Use REST API client helpers](../general-usage/http/rest-client.md) to simplify working with REST APIs.
3. Check out our [how-to guides](../walkthroughs) to get answers to some common questions.
4. Explore the [Examples](../examples) section to see how dlt can be used in real-world scenarios
