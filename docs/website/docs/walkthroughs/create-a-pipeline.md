---
title: Create a pipeline
description: How to create a pipeline
keywords: [how to, create a pipeline, rest client]
---

# Create a pipeline

This guide walks you through creating a pipeline that uses our [REST API Client](../dlt-ecosystem/verified-sources/rest_api/advanced#restclient)
to connect to [DuckDB](../dlt-ecosystem/destinations/duckdb).
:::tip
We're using DuckDB as a destination here, but you can adapt the steps to any [source](../dlt-ecosystem/verified-sources/) and [destination](../dlt-ecosystem/destinations/) by
using the [command](../reference/command-line-interface#dlt-init) `dlt init <source> <destination>` and tweaking the pipeline accordingly.
:::

Please make sure you have [installed `dlt`](../reference/installation) before following the
steps below.

## Task overview

Imagine you want to analyze issues from a GitHub project locally.
To achieve this, you need to write code that accomplishes the following:

1. Constructs a correct request.
2. Authenticates your request.
3. Fetches and handles paginated issue data.
4. Stores the data for analysis.

This may sound complicated, but dlt provides a [REST API Client](../dlt-ecosystem/verified-sources/rest_api/advanced#restclient) that allows you to focus more on your data rather than on managing API interactions.


## 1. Initialize project

Create a new empty directory for your `dlt` project by running:

```sh
mkdir github_api_duckdb && cd github_api_duckdb
```

Start a `dlt` project with a pipeline template that loads data to DuckDB by running:

```sh
dlt init github_api duckdb
```

Install the dependencies necessary for DuckDB:

```sh
pip install -r requirements.txt
```

## 2. Optionally add API credentials from GitHub

The generated pipeline can read public GitHub API data without a token, but adding one increases the GitHub API rate limit. To use authenticated requests, [sign in](https://github.com/login) to your GitHub account and create your access token via the [Personal access tokens page](https://github.com/settings/tokens).

Copy your new access token over to `.dlt/secrets.toml`, replacing the generated placeholder:

```toml
access_token = "<api key value>"
```

To run without authentication, delete that line or set `access_token = ""`.

This token will be used by `github_source()` to authenticate requests.

The **secret name** corresponds to the **argument name** in the source function. Below, `access_token` [will get its value](../general-usage/credentials/advanced) from `secrets.toml` when `github_source()` is called.

## 3. Review the generated GitHub API source

The `dlt init github_api duckdb` command creates `github_api_pipeline.py`. It uses the `dlt` repository as an example GitHub project, but you can replace the organization, repository, or endpoint paths with your own.

```py
from typing import Optional
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.source
def github_source(access_token: Optional[str] = dlt.secrets.value):
    auth = BearerTokenAuth(token=access_token) if access_token else None

    client = RESTClient(
        base_url="https://api.github.com",
        auth=auth,
        paginator=HeaderLinkPaginator(),
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )

    @dlt.resource(name="repos", write_disposition="replace")
    def repos():
        for page in client.paginate("orgs/dlt-hub/repos"):
            yield page

    @dlt.resource(name="issues", write_disposition="append")
    def issues(
        updated_at=dlt.sources.incremental(
            "updated_at",
            initial_value="2026-01-01T00:00:00Z",
        )
    ):
        for page in client.paginate(
            "repos/dlt-hub/dlt/issues",
            params={
                "state": "open",
                "sort": "updated",
                "direction": "desc",
                "since": updated_at.start_value,
                "per_page": "100",
            },
        ):
            yield page

    return [repos, issues]
```

The template defines two resources:

- `repos`, which replaces the destination table with the current list of repositories in the `dlt-hub` organization.
- `issues`, which appends open issues from `dlt-hub/dlt` and tracks them incrementally by `updated_at`.

## 4. Load the data

The generated script is ready to run. Its `run_source` function creates the pipeline and loads `github_source()` into DuckDB:

```py
def run_source() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="github_api_pipeline",
        destination="duckdb",
        dataset_name="github_api_data",
        progress="log",
    )

    load_info = pipeline.run(github_source())
    print(load_info)

if __name__ == "__main__":
    run_source()
```

Run the pipeline script:

```sh
python github_api_pipeline.py
```

This loads the GitHub data and prints the `load_info` object.

Let's explore the loaded data with the [command](../reference/command-line-interface#dlt-pipeline-show) `dlt pipeline <pipeline_name> show`.

:::info
You will need to install `pip dlt[workspace]`
:::

```sh
dlt pipeline github_api_pipeline show
```

This will open the workspace dashboard app that gives you an overview of the data loaded.

## 5. Next steps

With a functioning pipeline, consider exploring:

- Our [REST Client](../dlt-ecosystem/verified-sources/rest_api/advanced#restclient).
- [Deploy this pipeline with GitHub Actions](deploy-a-pipeline/deploy-with-github-actions), so that the data is automatically loaded on a schedule.
- Transform the [loaded data](../dlt-ecosystem/transformations) with dbt or in Python using Pandas, Arrow, or Polars.
- Learn how to [run](../running-in-production/running), [monitor](../running-in-production/monitoring), and [alert](../running-in-production/alerting) when you put your pipeline in production.
- Try loading data to a different destination like [Google BigQuery](../dlt-ecosystem/destinations/bigquery), [Amazon Redshift](../dlt-ecosystem/destinations/redshift), or [Postgres](../dlt-ecosystem/destinations/postgres).
