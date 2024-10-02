---
title: Strapi
description: dlt verified source for Strapi API
keywords: [strapi api, strapi verified source, strapi]
---
import Header from './_source-info-header.md';

# Strapi

<Header/>

[Strapi](https://strapi.io/) is a headless CMS (Content Management System) that allows developers to create API-driven
content management systems without having to write a lot of custom code.

Since Strapi's available endpoints vary based on your Strapi setup, ensure you recognize the ones
you'll ingest to transfer data to your warehouse.

This Strapi `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/strapi_pipeline.py)
loads data using the “Strapi API” to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name          | Description                |
| ------------- | -------------------------- |
| strapi_source | Retrieves data from Strapi |

## Setup guide

### Grab API token

1. Log in to Strapi.
1. Click ⚙️ in the sidebar.
1. Go to API tokens under global settings.
1. Create a new API token.
1. Fill in Name, Description, and Duration.
1. Choose a token type: Read Only, Full Access, or custom (with find and findOne selected).
1. Save to view your API token.
1. Copy it for dlt secrets setup.

> Note: The Strapi UI, which is described here, might change.
> The full guide is available at [this link.](https://docs.strapi.io/user-docs/settings/API-tokens)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init strapi duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/strapi_pipeline.py)
   with Strapi as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```py
   # put your secret values and credentials here. do not share this file and do not push it to github
   [sources.strapi]
   api_secret_key = "api_secret_key" # please set me up!
   domain = "domain" # please set me up!
   ```

1. Replace `api_secret_key` with [the API token you copied above](strapi.md#grab-api-token).

1. Strapi auto-generates the domain.

1. The domain is the URL opened in a new tab when you run Strapi, e.g.,
   \[my-strapi.up.your_app.app\].

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

For more information, read the [General usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```sh
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python strapi_pipeline.py
   ```

   > In the provided script, we've included a list with one endpoint, "athletes." Simply add any
   > other endpoints from your Strapi setup to this list in order to load them. Then, execute this
   > file to initiate the data loading process.

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `strapi`, you may also use any
   custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `strapi_source`

This function retrieves data from Strapi.

```py
@dlt.source
def strapi_source(
    endpoints: List[str],
    api_secret_key: str = dlt.secrets.value,
    domain: str = dlt.secrets.value,
) -> Iterable[DltResource]:
   ...
```

`endpoints`: Collections to fetch data from.

`api_secret_key`: API secret key for authentication, defaults to dlt secrets.

`domain`: Strapi API domain name, defaults to dlt secrets.


## Customization
### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
        pipeline_name="strapi",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="strapi_data"  # Use a custom name if desired
   )
   ```

1. To load the specified endpoints:

   ```py
   endpoints = ["athletes"]
   load_data = strapi_source(endpoints=endpoints)

   load_info = pipeline.run(load_data)
   # pretty print the information on data that was loaded
   print(load_info)
   ```

> We loaded the "athletes" endpoint above, which can be customized to suit our specific
> requirements.

<!--@@@DLT_TUBA strapi-->

