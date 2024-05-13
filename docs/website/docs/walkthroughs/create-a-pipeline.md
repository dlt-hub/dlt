---
title: Create a pipeline
description: How to create a pipeline
keywords: [how to, create a pipeline]
---

# Create a pipeline

Follow the steps below to create a [pipeline](../general-usage/glossary.md#pipeline) using the
our rest API client to DuckDB from scratch. The same steps can be repeated for any source and
destination of your choice—use `dlt init <source> <destination>` and then build the pipeline for
that API instead.

Please make sure you have [installed `dlt`](../reference/installation.md) before following the
steps below.


## Task
Let's suppose you have a github project and would like to download all issues to analyze them in
your local machine, thus you need to write some code which does the following things:

1. Authenticates requests,
2. Fetches and paginates over the issues,
3. Saves the data somewhere.

With this in mind let's continue.

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

The **secret name** corresponds to the **argument name** in the source function.
Below `api_secret_key` [will get its value](../general-usage/credentials/configuration.md#general-usage-and-an-example) from `secrets.toml` when `githubapi_source()` is called.

```py
@dlt.source
def githubapi_source(api_secret_key=dlt.secrets.value):
    return repo_issues_resource(api_secret_key=api_secret_key)
```

Run the `githubapi.py` pipeline script to test that authentication headers look fine:

```sh
python3 githubapi.py
```

Your API key should be printed out to stdout along with some test data.

## 3. Request project issues from then GitHub API

Replace the definition of the `githubapi_resource` function definition in the `githubapi.py`
pipeline script with a call to the GitHub API:

>[!NOTE]
> We will use dlt as an example project https://github.com/dlt-hub/dlt, feel free to replace it with your own repository.

```py
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.resource(write_disposition="append")
def repo_issues_resource(api_secret_key=dlt.secrets.value):
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

This should print out the weather in New York City right now.

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
    data = list(repo_issues_resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(githubapi_source())

    # pretty print the information on data that was loaded
    print(load_info)
```

Run the `githubapi.py` pipeline script to load data into DuckDB:

```sh
python3 githubapi.py
```

Then this command to see that the data loaded:

```sh
dlt pipeline githubapi show
```

This will open a Streamlit app that gives you an overview of the data loaded.

## 5. Next steps

Now that you have a working pipeline, you have options for what to learn next:

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
