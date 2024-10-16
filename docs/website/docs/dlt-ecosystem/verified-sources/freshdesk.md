---
title: Freshdesk
description: dlt verified source for Freshdesk API
keywords: [freshdesk api, freshdesk verified source, freshdesk]
---
import Header from './_source-info-header.md';


# Freshdesk

<Header/>


[Freshdesk](https://www.freshworks.com/freshdesk/) is a cloud-based customer service software
that provides businesses with tools for managing customer support via multiple channels including
email, phone, websites, and social media.

This Freshdesk `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/freshdesk_pipeline.py)
loads data using the "Freshdesk API" to the destination of your choice.

Resources that can be loaded using this verified source are:

| S.No. | Name      | Description                                                                               |
| ----- | --------- | ----------------------------------------------------------------------------------------- |
| 1.    | agents    | Users responsible for managing and resolving customer inquiries and support tickets.       |
| 2.    | companies | Customer organizations or groups that agents support.                                      |
| 3.    | contacts  | Individuals or customers who reach out for support.                                        |
| 4.    | groups    | Agents organized based on specific criteria.                                               |
| 5.    | roles     | Predefined sets of permissions that determine what actions an agent can perform.           |
| 6.    | tickets   | Customer inquiries or issues submitted via various channels like email, chat, phone, etc.  |

## Setup guide

### Grab credentials

To obtain your Freshdesk credentials, follow these steps:

1. Log in to your Freshdesk account.
1. Click on the profile icon to open "Profile Settings".
1. Copy the API key displayed on the right side.

> Note: The Freshdesk UI, which is described here, might change.
The full guide is available at [this link.](https://support.freshdesk.com/en/support/solutions/articles/215517-how-to-find-your-api-key)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init freshdesk duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/freshdesk_pipeline.py)
   with Freshdesk as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's what the file
   looks like:

   ```toml
   # Put your secret values and credentials here
   # Github access token (must be classic for reactions source)
   [sources.freshdesk]
   domain = "please set me up!" # Enter the Freshdesk domain here
   api_secret_key = "please set me up!" # Enter the Freshdesk API key here
   ```
1. In the `domain`, enter the domain of your Freshdesk account.

1. In `api_secret_key`, enter the API key you [copied above.](#grab-credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```sh
   pip install -r requirements.txt
   ```
2. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python freshdesk_pipeline.py
   ```
3. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is
   `freshdesk_pipeline`. You may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `freshdesk_source`

This function retrieves the data from specified Freshdesk API endpoints.

```py
@dlt.source()
def freshdesk_source(
    endpoints: Optional[List[str]] = None,
    per_page: int = 100,
    domain: str = dlt.secrets.value,
    api_secret_key: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    ...
```
> This source supports pagination and incremental data loading. It fetches data from a list of
> specified endpoints, or defaults to predefined endpoints in
> ["settings.py".](https://github.com/dlt-hub/verified-sources/blob/master/sources/freshdesk/settings.py)

`endpoints`: A list of Freshdesk API endpoints to fetch. Defaults to "settings.py".

`per_page`: The number of items to fetch per page, with a maximum of 100.

`domain`: The Freshdesk domain from which to fetch the data. Defaults to "config.toml".

`api_secret_key`: Freshdesk API key. Defaults to "secrets.toml".

### Resource `endpoints`

This function creates and yields a dlt resource for each endpoint in
["settings.py".](https://github.com/dlt-hub/verified-sources/blob/master/sources/freshdesk/settings.py)

```py
@dlt.source()
def freshdesk_source(
    #args as defined above
) -> Iterable[DltResource]:
    for endpoint in endpoints:
        yield dlt.resource(
            incremental_resource,
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )(endpoint=endpoint)
```

`incremental_resource`: A function that fetches and yields paginated data from a specified API endpoint.

`name`: Specifies the name of the endpoint.

`write_disposition`: Specifies the write disposition to load data.

`primary_key`: Specifies "id" as the primary key of the resource.

## Customization
### Create your own pipeline
If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="freshdesk_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="freshdesk_data"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline).

2. To load data from all the endpoints, specified in ["settings.py".](https://github.com/dlt-hub/verified-sources/blob/master/sources/freshdesk/settings.py)
   ```py
   load_data = freshdesk_source()
   # Run the pipeline
   load_info = pipeline.run(load_data)
   # Print the pipeline run information
   print(load_info)
   ```

3. To load the data from "agents", "contacts", and "tickets":
   ```py
   load_data = freshdesk_source().with_resources("agents", "contacts", "tickets")
   # Run the pipeline
   load_info = pipeline.run(load_data)
   # Print the pipeline run information
   print(load_info)
   ```
<!--@@@DLT_TUBA github-->
