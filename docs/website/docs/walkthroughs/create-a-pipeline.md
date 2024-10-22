---
title: Create a pipeline
description: How to create a pipeline
keywords: [how to, create a pipeline, rest client]
---

# Create a pipeline

This guide walks you through creating a pipeline that uses our [REST API Client](../general-usage/http/rest-client)
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

This may sound complicated, but dlt provides a [REST API Client](../general-usage/http/rest-client) that allows you to focus more on your data rather than on managing API interactions.


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

## 2. Obtain and add API credentials from GitHub

You will need to [sign in](https://github.com/login) to your GitHub account and create your access token via the [Personal access tokens page](https://github.com/settings/tokens).

Copy your new access token over to `.dlt/secrets.toml`:

```toml
[sources]
api_secret_key = '<api key value>'
```

This token will be used by `github_api_source()` to authenticate requests.

The **secret name** corresponds to the **argument name** in the source function.
Below, `api_secret_key` [will get its value](../general-usage/credentials/advanced)
from `secrets.toml` when `github_api_source()` is called.

```py
@dlt.source
def github_api_source(api_secret_key: str = dlt.secrets.value):
    return github_api_resource(api_secret_key=api_secret_key)
```

Run the `github_api.py` pipeline script to test that authentication headers look fine:

```sh
python github_api.py
```

Your API key should be printed out to stdout along with some test data.

## 3. Request project issues from the GitHub API


:::tip
We will use the `dlt` repository as an example GitHub project https://github.com/dlt-hub/dlt, feel free to replace it with your own repository.
:::

Modify `github_api_resource` in `github_api.py` to request issues data from your GitHub project's API:

```py
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.resource(write_disposition="replace")
def github_api_resource(api_secret_key: str = dlt.secrets.value):
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"

    for page in paginate(
        url,
        auth=BearerTokenAuth(api_secret_key), # type: ignore
        paginator=HeaderLinkPaginator(),
        params={"state": "open"}
    ):
        yield page
```

## 4. Load the data

Uncomment the commented-out code in the `main` function in `github_api.py`, so that running the
`python github_api.py` command will now also run the pipeline:

```py
if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='github_api_pipeline',
        destination='duckdb',
        dataset_name='github_api_data'
    )

    # print credentials by running the resource
    data = list(github_api_resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(github_api_source())

    # pretty print the information on data that was loaded
    print(load_info)
```


Run the `github_api.py` pipeline script to test that the API call works:

```sh
python github_api.py
```

This should print out JSON data containing the issues in the GitHub project.

It also prints the `load_info` object.

Let's explore the loaded data with the [command](../reference/command-line-interface#show-tables-and-data-in-the-destination) `dlt pipeline <pipeline_name> show`.

:::info
Make sure you have `streamlit` installed: `pip install streamlit`
:::

```sh
dlt pipeline github_api_pipeline show
```

This will open a Streamlit app that gives you an overview of the data loaded.

## 5. Next steps

With a functioning pipeline, consider exploring:

- Our [REST Client](../general-usage/http/rest-client).
- [Deploy this pipeline with GitHub Actions](deploy-a-pipeline/deploy-with-github-actions), so that the data is automatically loaded on a schedule.
- Transform the [loaded data](../dlt-ecosystem/transformations) with dbt or in Pandas DataFrames.
- Learn how to [run](../running-in-production/running), [monitor](../running-in-production/monitoring), and [alert](../running-in-production/alerting) when you put your pipeline in production.
- Try loading data to a different destination like [Google BigQuery](../dlt-ecosystem/destinations/bigquery), [Amazon Redshift](../dlt-ecosystem/destinations/redshift), or [Postgres](../dlt-ecosystem/destinations/postgres).

