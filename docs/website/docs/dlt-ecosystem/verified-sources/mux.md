# Mux

[Mux.com](http://mux.com/) is a video technology platform that provides infrastructure and tools for developers to build and stream high-quality video content.
It offers services such as video encoding, storage, delivery, and analytics, aiming to simplify the complexities of video streaming and enhance the viewing experience for end-users.

Using this Mux `dlt` verified source and pipeline example, you can load metadata about the assets and every video view to be loaded to the destination of your choice.

| Endpoint    | Description                                                                                         |
|-------------|-----------------------------------------------------------------------------------------------------|
| asset       | Refers to the video content that you want to upload, encode, store, and stream using their platform |
| video view  | Represents a single instance of a video being watched or streamed                                   |

## Grab API credentials

1. Sign in to your [mux.com](http://mux.com/) account.
2. Navigate to the bottom left corner of the page and click on "Settings". Then, choose "Access Token".
3. Click on "Generate new token".
4. Provide the required read permissions for Mux videos and Mux data. Additionally, give a meaningful name to the token.
5. Click on "Generate token".
6. Copy both the API access token and API secret key. You will need these for configuring the verified source later on.

## Initialize the Mux verified source and the pipeline example

To get started with this verified source, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:

    ```bash
    dlt init mux bigquery
    ```

    This command will initialize your verified source with Mux and create a pipeline example with BigQuery as the destination.
    If you'd like to use a different destination, simply replace `bigquery` with the name of your preferred destination.
    You can find supported destinations and their configuration options in our [documentation](../destinations/duckdb).

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

    ```toml
    mux_source
    ├── .dlt
    │   ├── config.toml
    │   └── secrets.toml
    ├── mux
    │   └── __init__.py
    │   └── settings.py
    ├── .gitignore
    ├── requirements.txt
    └── mux_pipeline.py
    ```


## **Add credentials**

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

    Here's what the file looks like:

    ```toml
    # Put your secret values and credentials here. Do not share this file and do not push it to github
    [sources.mux]
    mux_api_access_token = "please set me up" # Mux API access token
    mux_api_secret_key = "please set me up!" # Mux API secret key

    [destination.bigquery.credentials]
    project_id = "project_id" # GCP project ID
    private_key = "private_key" # Unique private key (including `BEGIN and END PRIVATE KEY`)
    client_email = "client_email" # Service account email
    location = "US" # Project location (e.g. “US”)
    ```

2. Replace the API access and secret key with the ones that you copied above.
   This will ensure that this source can access your Mux resources securely.
3. Finally, follow the instructions in [Destinations](../destinations/duckdb) to add credentials for your chosen destination.
   This will ensure that your data is properly routed to its final destination.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:

    ```bash
    pip install -r requirements.txt
    ```

2. Now the verified source can be run by using the command:

    ```bash
    python3 mux_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:

    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline example is `mux`, you may also use any custom name instead.


## Customizations

To load data to the destination using this verified source, you have the option to write your own methods.

### **Source and resource methods**

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source) and [resources](https://dlthub.com/docs/general-usage/resource)
that for this verified source are found in the `__init__.py` file within the *mux* directory.
This verified source has three default methods that form the basis of loading. The methods are:

**Source** **mux_source:**

```python
@dlt.source
def mux_source() -> Iterable[DltResource]:
    yield assets_resource
    yield views_resource
```

The `mux_source` function serves as a source function and yields instances of ”*DltResource*” objects, which represent video assets and video views to be loaded.
It combines the results from `assets_resource` and `views_resource` and yields them as the data source.

**Resource assets_resource:**

```python
@dlt.resource(write_disposition="merge")
def assets_resource(
    mux_api_access_token: str = dlt.secrets.value,
    mux_api_secret_key: str = dlt.secrets.value,
    limit: int = DEFAULT_LIMIT,
) -> Iterable[TDataItem]:
```

- The `assets_resource` function is a resource function that retrieves metadata about video assets.
- It makes a request to the Mux API “*assets*” endpoint, using the provided ”*mux_api_access_token*” and ”*mux_api_secret_key*” for authentication.
- It retrieves information about video assets from the Mux API's and yields the data for each asset.

**Resource views_resource:**

```python
@dlt.resource(write_disposition="append")
def views_resource(
    mux_api_access_token: str = dlt.secrets.value,
    mux_api_secret_key: str = dlt.secrets.value,
    limit: int = DEFAULT_LIMIT,
) -> Iterable[DltResource]:
```

The `views_resource` function is another resource function that retrieves metadata about video views from yesterday.

It makes a request to the Mux API's ”*video-views*” endpoint, using the provided  ”*mux_api_access_token” and “mux_api_secret_key”* for authentication.

It retrieves video views data for a specific timeframe (yesterday) and yields the data for each view as a “*DltResouce*” object.

### **Create Your Data Loading Pipeline using Mux verified source**

If you wish to create your own pipelines, you can leverage the above source and resource functions.

To create your data pipeline, follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our [documentation here.](https://dlthub.com/docs/general-usage/pipeline)

    ```python
    pipeline = dlt.pipeline(
        pipeline_name="mux_pipeline",# Use a custom name if desired
        destination="bigquery",# Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="mux_dataset"# Use a custom name if desired
    )
    ```

2. Load the metadata to the destination using the source function as follows:

    ```python
    load_info = pipeline.run(mux_source())
    print(load_info)
    ```

3. If you want to load data from a specific source; for example, “*assets”*, you can use the “*with_resources”* method as follows:

    ```python
    load_info = pipeline.run(mux_source().with_resources("assets_resource")
    print(load_info)
    ```

4. This pipeline is responsible for loading metadata related to video assets and views from the previous day into the destination.

That's it! Enjoy running your `dlt` Mux pipeline!