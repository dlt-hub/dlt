---
title: Strapi
description: dlt verified source for Strapi API
keywords: [strapi api, strapi verified source, strapi]
---

# Strapi

Strapi is a headless CMS (Content Management System) that allows developers to create powerful API-driven content management systems without having to write a lot of custom code.

Since the endpoints that will be available in Strapi depend on your setup, in order to get data from strapi to your warehouse, you need to be aware of which endpoints you will ingest.

## Grab API token

1. Log in to your Strapi account
2. Click on `⚙️ settings` in the left-hand sidebar menu
3. In the settings menu, go to the `API tokens` option that is present under global settings
4. Click on `Create new API token`
5. Fill in the details like `Name`, `description`, and `token duration`
6. In the token type drop-down menu, you can select `Read Only`, `Full access` or custom permissions for the API (note: if setting a custom permission, please make sure to select `find` and `findOne`.
7. API token will be displayed on clicking the “save” button
8. Copy the token displayed (i.e. the token will be used in configuring `dlt` secrets)

## Initialize the pipeline with Strapi verified source

Initialize the pipeline with the following command:

`dlt init strapi <destination>`

For destination, you can use `bigquery`, `duckdb`, or one of the other [destinations](../general-usage/glossary.md#destination).

## **Add credentials**

In the `.dlt` folder, you will find `secrets.toml`, which looks like this:

```python
# put your secret values and credentials here. do not share this file and do not push it to github
[sources.strapi]
api_secret_key = "api_secret_key" # please set me up!
domain = "domain" # please set me up!

[destination.bigquery.credentials] # the credentials required will change based on the destination
project_id = "set me up" # GCP project ID
private_key = "set me up" # Unique private key (including `BEGINand END PRIVATE KEY`)
client_email = "set me up" # Service account email
location = "set me up" # Project location (e.g. “US”)
```

1. Replace `api_secret_key` with [the API token you copied above](strapi.md#grab-api-token)
2. The domain is created automatically by Strapi
3. When you run the Strapi project and a new tab opens in the browser, the URL in the address bar of that tab is the domain). For example, `[my-strapi.up.railway.app](http://my-strapi.up.railway.app)`
4. Follow the instructions in [Destinations](https://dlthub.com/docs/destinations) to add credentials for your chosen destination

## Add your endpoint and run  **`strapi_pipeline.py`**

After initializing the pipeline a file named `strapi_pipeline.py` is created.

```python
import dlt
from strapi import strapi_source

def load(endpoints=None):
    endpoints = ['athletes'] or endpoints
    pipeline = dlt.pipeline(pipeline_name='strapi', destination='bigquery', dataset_name='strapi_data')

    # run the pipeline with your parameters
    load_info = pipeline.run(strapi_source(endpoints=endpoints))
    # pretty print the information on data that was loaded
    print(load_info)

if __name__ == "__main__" :
    # add your desired endpoints to the list
    endpoints = ['athletes']
    load(endpoints)
```

In the sample script above, we have one list with one endpoint called “athletes”. Add other endpoints (i.e. the endpoints your Strapi setup has) to this list to load them. Afterwards, run this file to load the data.

## Run the pipeline

1. Install the necessary dependencies by running the following command:

`pip install -r requirements.txt`

2. Now the pipeline can be run by using the command:

`python3 strapi_pipeline.py`

3. To make sure that everything is loaded as expected, use the command:

`dlt pipeline <pipeline_name> show`
(For example, the pipeline_name for the above pipeline is `strapi_pipeline`, you may also use any custom name instead)