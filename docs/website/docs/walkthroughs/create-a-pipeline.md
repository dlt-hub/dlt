---
title: Create a pipeline
description: How to create a pipeline
keywords: [how to, create a pipeline]
---

# Create a pipeline

Follow the steps below to create a [pipeline](../general-usage/glossary.md#pipeline) from the
WeatherAPI.com API to DuckDB from scratch. The same steps can be repeated for any source and
destination of your choice—use `dlt init <source> <destination>` and then build the pipeline for
that API instead.

Please make sure you have [installed `dlt`](../reference/installation.md) before following the
steps below.

## 1. Initialize project

Create a new empty directory for your `dlt` project by running:

```bash
mkdir weatherapi_duckdb && cd weatherapi_duckdb
```

Start a `dlt` project with a pipeline template that loads data to DuckDB by running:

```bash
dlt init weatherapi duckdb
```

Install the dependencies necessary for DuckDB:

```bash
pip install -r requirements.txt
```

## 2. Add WeatherAPI.com API credentials

You will need to [sign up for the WeatherAPI.com API](https://www.weatherapi.com/signup.aspx).

Once you do this, you should see your `API Key` at the top of your
[user page](https://www.weatherapi.com/my/).

Copy the value of the API key into `.dlt/secrets.toml`:

```toml
[sources]
api_secret_key = '<api key value>'
```
The **secret name** corresponds to the **argument name** in the source function. Below `api_secret_key` [will get its value](../general-usage/credentials/configuration.md#general-usage-and-an-example) from `secrets.toml` when `weatherapi_source()` is called.
```python
@dlt.source
def weatherapi_source(api_secret_key=dlt.secrets.value):
  ...
```

Run the `weatherapi.py` pipeline script to test that authentication headers look fine:

```bash
python3 weatherapi.py
```

Your API key should be printed out to stdout along with some test data.

## 3. Request data from the WeatherAPI.com API

Replace the definition of the `weatherapi_resource` function definition in the `weatherapi.py`
pipeline script with a call to the WeatherAPI.com API:

```python
@dlt.resource(write_disposition="append")
def weatherapi_resource(api_secret_key=dlt.secrets.value):
    url = "https://api.weatherapi.com/v1/current.json"
    params = {
        "q": "NYC",
        "key": api_secret_key
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    yield response.json()
```

Run the `weatherapi.py` pipeline script to test that the API call works:

```bash
python3 weatherapi.py
```

This should print out the weather in New York City right now.

## 4. Load the data

Remove the `exit()` call from the `main` function in `weatherapi.py`, so that running the
`python3 weatherapi.py` command will now also run the pipeline:

```python
if __name__=='__main__':

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='weatherapi',
        destination='duckdb',
        dataset_name='weatherapi_data'
    )

    # print credentials by running the resource
    data = list(weatherapi_resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(weatherapi_source())

    # pretty print the information on data that was loaded
    print(load_info)
```

Run the `weatherapi.py` pipeline script to load data into DuckDB:

```bash
python3 weatherapi.py
```

Then this command to see that the data loaded:

```bash
dlt pipeline weatherapi show
```

This will open a Streamlit app that gives you an overview of the data loaded.

## 5. Next steps

Now that you have a working pipeline, you have options for what to learn next:

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
