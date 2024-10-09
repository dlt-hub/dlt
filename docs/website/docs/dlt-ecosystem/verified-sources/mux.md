---
title: Mux
description: dlt verified source for Mux
keywords: [mux api, mux verified source, mux]
---
import Header from './_source-info-header.md';

# Mux

<Header/>


[Mux.com](http://mux.com/) is a video technology platform that provides infrastructure and tools for developers to build and stream high-quality video content.

This Mux `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/mux_pipeline.py)
loads data using the “Mux API” to the destination of your choice.


| Name        | Description                                                                                         |
|-------------|-----------------------------------------------------------------------------------------------------|
| asset       | Refers to the video content that you want to upload, encode, store, and stream using their platform |
| video view  | Represents a single instance of a video being watched or streamed                                   |

> Note: The source `mux_source` loads all video assets, but each video view is for yesterday only!

## Setup guide

### Grab credentials

1. Sign in to [mux.com.](http://mux.com/)

1. Click "Settings" at the bottom left, then select "Access Token".

1. Select "Generate new token".

1. Assign read permissions for Mux videos and data, and name the token.

1. Click "Generate token".

1. Copy the API access token and secret key for later configuration.

> Note: The Mux UI, which is described here, might change.
The full guide is available at [this link.](https://docs.mux.com/guides/system/make-api-requests)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init mux duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/mux_pipeline.py)
   with Mux as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source.](../../walkthroughs/add-a-verified-source)


### Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

    Here's what the file looks like:

    ```toml
    # Put your secret values and credentials here. Do not share this file and do not push it to GitHub
    [sources.mux]
    mux_api_access_token = "please set me up" # Mux API access token
    mux_api_secret_key = "please set me up!" # Mux API secret key
    ```

1. Replace the API access and secret key with the ones that you [copied above](#grab-credentials).
   This will ensure that this source can access your Mux resources securely.

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```sh
   pip install -r requirements.txt
   ```
2. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python mux_pipeline.py
   ```
3. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is
   `mux`, you may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).


### Source `mux_source`

This function yields resources "asset_resource" and "views_resource" to load video assets and views.

```py
@dlt.source
def mux_source() -> Iterable[DltResource]:
    yield assets_resource
    yield views_resource
```

### Resource `assets_resource`

The assets_resource function fetches metadata about video assets from the Mux API's "assets" endpoint.

```py
@dlt.resource(write_disposition="merge")
def assets_resource(
    mux_api_access_token: str = dlt.secrets.value,
    mux_api_secret_key: str = dlt.secrets.value,
    limit: int = DEFAULT_LIMIT,
) -> Iterable[TDataItem]:
    ...
```

`mux_api_access_token`: Mux API token for authentication, defaults to ".dlt/secrets.toml".

`mux_api_secret_key`: Mux API secret key for authentication, defaults to ".dlt/secrets.toml".

`limit`: Sets the cap on the number of video assets fetched. "DEFAULT_LIMIT" set to 100.

### Resource `views_resource`

This function yields data about every video view from yesterday to be loaded.

```py
@dlt.resource(write_disposition="append")
def views_resource(
    mux_api_access_token: str = dlt.secrets.value,
    mux_api_secret_key: str = dlt.secrets.value,
    limit: int = DEFAULT_LIMIT,
) -> Iterable[DltResource]:
    ...
```

The arguments `mux_api_access_token`, `mux_api_secret_key`, and `limit` are the same as described [above](#resource-assets_resource) in "asset_resource".


## Customization
### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

    ```py
    pipeline = dlt.pipeline(
        pipeline_name="mux_pipeline", # Use a custom name if desired
        destination="bigquery", # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="mux_dataset" # Use a custom name if desired
    )
    ```

2. To load metadata about every asset to be loaded:

    ```py
    load_info = pipeline.run(mux_source().with_resources("assets_resource"))
    print(load_info)
    ```

3. To load data for each video view from yesterday:

    ```py
    load_info = pipeline.run(mux_source().with_resources("views_resource"))
    print(load_info)
    ```

4. To load both metadata about assets and video views from yesterday:

    ```py
    load_info = pipeline.run(mux_source())
    print(load_info)
    ```

<!--@@@DLT_TUBA mux-->

