---
title: Slack
description: dlt verified source for Slack API
keywords: [slack api, slack verified source, slack]
---
import Header from './_source-info-header.md';

# Slack

<Header/>

[Slack](https://slack.com/) is a popular messaging and collaboration platform for teams and organizations.

This Slack `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/slack_pipeline.py)
load data using the “Slack API” to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name                  | Description                                                                        |
|-----------------------|------------------------------------------------------------------------------------|
| slack                 | Retrieves all the Slack data: channels, messages for selected channels, users, logs |
| channels              | Retrieves all the channels data                                                     |
| users                 | Retrieves all the users info                                                        |
| get_messages_resource | Retrieves all the messages for a given channel                                      |
| access_logs           | Retrieves the access logs                                                           |

## Setup guide

### Grab user OAuth token

To set up the pipeline, create a Slack app in your workspace to obtain a user token for accessing the Slack API.

1. Navigate to your Slack workspace and click on the name at the top-left.
1. Select Tools > Customize Workspace.
1. From the top-left Menu, choose Configure apps.
1. Click Build (top-right) > Create a New App.
1. Opt for "From scratch", set the "App Name", and pick your target workspace.
1. Confirm with Create App.
1. Navigate to OAuth and Permissions under the Features section.
1. Assign the following scopes:

   | Name             | Description                                                                       |
   |------------------|-----------------------------------------------------------------------------------|
   | admin            | Administer a workspace                                                            |
   | channels:history | View messages and other content in public channels                                |
   | groups:history   | View messages and other content in private channels (where the app is added)      |
   | im:history       | View messages and other content in direct messages (where the app is added)       |
   | mpim:history     | View messages and other content in group direct messages (where the app is added) |
   | channels:read    | View basic information about public channels in a workspace                       |
   | groups:read      | View basic information about private channels (where the app is added)            |
   | im:read          | View basic information about direct messages (where the app is added)             |
   | mpim:read        | View basic information about group direct messages (where the app is added)       |
   | users:read       | View people in a workspace                                                        |
   > Note: These scopes are adjustable; tailor them to your needs.

1. From "OAuth & Permissions" on the left, add the scopes and copy the User OAuth Token.

> Note: The Slack UI, which is described here, might change. The official guide is available at this [link](https://api.slack.com/start/quickstart).

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init slack duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/slack_pipeline.py)
   with Slack as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe.

   Here's its format for service account authentication:

    ```toml
    [sources.slack]
    access_token = "Please set me up!" # please set me up!
    ```

1. Copy the user OAuth token you [copied above](#grab-user-oauth-token).

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```sh
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python slack_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `slack`, you
   may also use any custom name instead.

   For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `slack`

It retrieves data from Slack's API and fetches the Slack data such as channels, messages for selected channels, users, logs.

```py
@dlt.source(name="slack", max_table_nesting=2)
def slack_source(
    page_size: int = MAX_PAGE_SIZE,
    access_token: str = dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = DEFAULT_START_DATE,
    end_date: Optional[TAnyDateTime] = None,
    selected_channels: Optional[List[str]] = dlt.config.value,
) -> Iterable[DltResource]:
   ...
```

`page_size`: Maximum items per page (default: 1000).

`access_token`: OAuth token for authentication.

`start_date`: Range start. (default: January 1, 2000).

`end_date`: Range end.

`selected_channels`: Channels to load; defaults to all if unspecified.

### Resource `channels`

This function yields all the channels data as a `dlt` resource.

```py
@dlt.resource(name="channels", primary_key="id", write_disposition="replace")
def channels_resource() -> Iterable[TDataItem]:
   ...
```

### Resource `users`

This function yields all the users data as a `dlt` resource.

```py
@dlt.resource(name="users", primary_key="id", write_disposition="replace")
def users_resource() -> Iterable[TDataItem]:
   ...
```

### Resource `get_messages_resource`

This method fetches messages for a specified channel from the Slack API. It creates a resource for each channel with the channel's name.

```py
def get_messages_resource(
    channel_data: Dict[str, Any],
    created_at: dlt.sources.incremental[DateTime] = dlt.sources.incremental(
        "ts",
        initial_value=start_dt,
        end_value=end_dt,
        allow_external_schedulers=True,
    ),
) -> Iterable[TDataItem]:
   ...
```

`channel_data`: A dictionary detailing a specific channel to determine where messages are fetched from.

`created_at`: An optional parameter leveraging dlt.sources.incremental to define the timestamp range for message retrieval. Sub-arguments include:

   - `ts`: Timestamp from the Slack API response.

   - `initial_value`: Start of the timestamp range, defaulting to start_dt in slack_source.

   - `end_value`: Timestamp range end, defaulting to end_dt in slack_source.

   - `allow_external_schedulers`: A boolean that, if true, permits [external schedulers](../../general-usage/incremental-loading#using-airflow-schedule-for-backfill-and-incremental-loading) to manage incremental loading.

### Resource `access_logs`

This method retrieves access logs from the Slack API.

```py
@dlt.resource(
    name="access_logs",
    selected=False,
    primary_key="user_id",
    write_disposition="append",
)
# It is not an incremental resource; it just has an end_date filter.
def logs_resource() -> Iterable[TDataItem]:
   ...
```

`selected`: A boolean set to False, indicating the resource isn't loaded by default.

`primary_key`: The unique identifier is "user_id".

`write_disposition`: Set to "append", allowing new data to join existing data in the destination.
> Note: This resource may not function in the pipeline or tests due to its paid status. An error arises for non-paying accounts.

## Customization
### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
        pipeline_name="slack",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="slack_data"  # Use a custom name if desired
   )
   ```
1. To load Slack resources from the specified start date:

   ```py
   source = slack_source(page_size=1000, start_date=datetime(2023, 9, 1), end_date=datetime(2023, 9, 8))

   # Enable below to load only 'access_logs', available for paid accounts only.
   # source.access_logs.selected = True

   # It loads data starting from 1st September 2023 to 8th September 2023.
   load_info = pipeline.run(source)
   print(load_info)
   ```
   > Subsequent runs will load only items updated since the previous run.

1. To load data from selected Slack channels from the specified start date:

   ```py
   # To load data from selected channels.
   selected_channels=["general", "random"] # Enter the channel names here.

   source = slack_source(
       page_size=20,
       selected_channels=selected_channels,
       start_date=datetime(2023, 9, 1),
       end_date=datetime(2023, 9, 8),
   )
   # It loads data starting from 1st September 2023 to 8th September 2023 from the channels: "general" and "random".
   load_info = pipeline.run(source)
   print(load_info)
   ```

1. To load only messages from selected Slack resources:

   ```py
   # To load data from selected channels.
   selected_channels=["general", "random"] # Enter the channel names here.

   source = slack_source(
       page_size=20,
       selected_channels=selected_channels,
       start_date=datetime(2023, 9, 1),
       end_date=datetime(2023, 9, 8),
   )
   # It loads only messages from the channel "general".
   load_info = pipeline.run(source.with_resources("general"))
   print(load_info)
   ```

<!--@@@DLT_TUBA slack-->

