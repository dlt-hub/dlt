---
title: Create a pipeline
description: How to create a pipeline
keywords: [how to, create a pipeline, rest client]
---

# Create a pipeline

This guide walks you through creating a pipeline that utilizes our REST API client to connect to DuckDB.
Although this example uses DuckDB, you can adapt the steps to any source and destination by
using the command `dlt init <source> <destination>` and tweaking the pipeline accordingly.

Please make sure you have [installed `dlt`](../reference/installation.md) before following the
steps below.

## Task Overview

Imagine you want to analyze issues from a GitHub project locally.
To achieve this, you need to write code that accomplishes the following:

1. Build correct requests.
2. Authenticates your requests.
3. Fetches and handles paginated issue data.
4. Stores the data for analysis.


## 1. Initialize project

Create a new empty directory for your `dlt` project by running:

```sh
mkdir githubapi_duckdb && cd githubapi_duckdb
```

Start a `dlt` project with a pipeline template that loads data to DuckDB by running:

```sh
dlt init githubapi duckdb
```

Install the dependencies necessary for DuckDB:

```sh
pip install -r requirements.txt
```

## 2. Obtain and Add API credentials from GitHub

You will need to [sign in](https://github.com/login) to your github account and create your access token via [Personal access tokens page](https://github.com/settings/tokens).

Copy your new access token over to `.dlt/secrets.toml`:

```toml
[sources]
api_secret_key = '<api key value>'
```

This token will be used by `githubapi_source()` to authenticate requests.

The **secret name** corresponds to the **argument name** in the source function.
Below `api_secret_key` [will get its value](../general-usage/credentials/configuration.md#general-usage-and-an-example) from `secrets.toml` when `githubapi_source()` is called.

```py
@dlt.source
def githubapi_source(api_secret_key=dlt.secrets.value):
    return githubapi_my_repo_issues(api_secret_key=api_secret_key)
```

Run the `githubapi.py` pipeline script to test that authentication headers look fine:

```sh
python3 githubapi.py
```

Your API key should be printed out to stdout along with some test data.

## 3. Request project issues from then GitHub API


>[!NOTE]
> We will use dlt as an example project https://github.com/dlt-hub/dlt, feel free to replace it with your own repository.

Modify `githubapi_my_repo_issues` in `githubapi.py` to request issues data from your GitHub project's API:

```py
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.resource(write_disposition="append")
def githubapi_my_repo_issues(api_secret_key=dlt.secrets.value):
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"

    for page in paginate(
        url,
        auth=BearerTokenAuth(api_secret_key),
        paginator=HeaderLinkPaginator(),
        params={"state": "open"}
    ):
        print(page)
        yield page
```

Run the `githubapi.py` pipeline script to test that the API call works:

```sh
python3 githubapi.py
```

This should print out json data containig the issues in the GitHub project.

Then, confirm the data is loaded

>[!NOTE]
> Make sure you have `streamlit` installed `pip install streamlit`

```sh
dlt pipeline githubapi show
```

## 4. Load the data

Remove the `exit()` call from the `main` function in `githubapi.py`, so that running the
`python3 githubapi.py` command will now also run the pipeline:

```py
if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='githubapi_repo_issues',
        destination='duckdb',
        dataset_name='repo_issues_data'
    )

    # print credentials by running the resource
    data = list(githubapi_my_repo_issues())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(githubapi_source())

    # pretty print the information on data that was loaded
    print(load_info)
```

Load your GitHub issues into DuckDB:

```sh
python3 githubapi.py
```

Then this command to see that the data loaded:

```sh
dlt pipeline githubapi show
```

This will open a Streamlit app that gives you an overview of the data loaded.

## 5. Next steps

With a functioning pipeline, consider exploring:

- Learn more about our [rest client](https://dlthub.com/devel/general-usage/http/rest-client).
- [Deploy this pipeline with GitHub Actions](deploy-a-pipeline/deploy-with-github-actions), so that
  the data is automatically loaded on a schedule.
- Transform the [loaded data](../dlt-ecosystem/transformations) with dbt or in
  Pandas DataFrames.
- Learn how to [run](../running-in-production/running.md),
  [monitor](../running-in-production/monitoring.md), and
  [alert](../running-in-production/alerting.md) when you put your pipeline in production.
- Try loading data to a different destination like
  [Google BigQuery](../dlt-ecosystem/destinations/bigquery.md),
  [Amazon Redshift](../dlt-ecosystem/destinations/redshift.md), or
  [Postgres](../dlt-ecosystem/destinations/postgres.md).
