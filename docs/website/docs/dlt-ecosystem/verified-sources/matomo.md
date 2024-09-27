---
title: Matomo
description: dlt verified source for Matomo
keywords: [matomo api, matomo verified source, matomo]
---
import Header from './_source-info-header.md';

# Matomo

<Header/>

Matomo is a free and open-source web analytics platform that provides detailed insights into website and application performance with features like visitor maps, site search analytics, real-time visitor tracking, and custom reports.

This Matomo `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/matomo_pipeline.py)
loads data using the “Matomo API” to the destination of your choice.

The endpoints that this verified source supports are:

| Name              | Description                                                                     |
| ----------------- |---------------------------------------------------------------------------------|
| matomo_reports    | Detailed analytics summaries of website traffic, visitor behavior, and more     |
| matomo_visits     | Individual user sessions on your website, pages viewed, visit duration, and more |

## Setup guide

### Grab credentials

1. Sign in to Matomo.
1. Hit the Administration (⚙) icon, top right.
1. Navigate to "Personal > Security" on the left menu.
1. Find and select "Auth Tokens > Create a New Token."
1. Verify with your password.
1. Add a descriptive label for your new token.
1. Click "Create New Token."
1. Your token is displayed.
1. Copy the access token and update it in the `.dlt/secrets.toml` file.
1. Your Matomo URL is the web address in your browser when logged into Matomo, typically "https://mycompany.matomo.cloud/". Update it in the `.dlt/config.toml`.
1. The site_id is a unique ID for each monitored site in Matomo, found in the URL or via Administration > Measurables > Manage under ID.

> Note: The Matomo UI, which is described here, might change.
The full guide is available at [this link.](https://developer.matomo.org/guides/authentication-in-depth)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init matomo duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/matomo_pipeline.py)
   with Matomo as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here
   # do not share this file and do not push it to GitHub
   [sources.matomo]
   api_token= "access_token" # please set me up!"
   ```

1. Replace the api_token value with the [previously copied one](matomo.md#grab-credentials) to ensure secure access to your Matomo resources.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to add credentials for your chosen destination, ensuring proper routing of your data to the final destination.

1. Next, store your pipeline configuration details in the `.dlt/config.toml`.

   Here's what the `config.toml` looks like:

   ```toml
   [sources.matomo]
   url = "Please set me up !" # please set me up!
   queries = ["a", "b", "c"] # please set me up!
   site_id = 0 # please set me up!
   live_events_site_id = 0 # please set me up!
   ```
1. Replace the value of `url` and `site_id` with the one that [you copied above](matomo.md#grab-url-and-site_id).

1. To monitor live events on a website, enter the `live_event_site_id` (usually it is the same as `site_id`).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python matomo_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `matomo`, you may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Source `matomo_reports`

This function executes and loads a set of reports defined in "queries" for a specific Matomo site identified by "site_id".

```py
@dlt.source(max_table_nesting=2)
def matomo_reports(
    api_token: str = dlt.secrets.value,
    url: str = dlt.config.value,
    queries: List[DictStrAny] = dlt.config.value,
    site_id: int = dlt.config.value,
) -> Iterable[DltResource]:
   ...
```

`api_token`: API access token for Matomo server authentication, defaults to "./dlt/secrets.toml"

`url`: Matomo server URL, defaults to "./dlt/config.toml"

`queries`: List of dictionaries containing info on what data to retrieve from Matomo API.

`site_id`: Website's Site ID as per Matomo account.

>Note: This is an [incremental](../../general-usage/incremental-loading) source method and loads the "last_date" from the state of the last pipeline run.

### Source `matomo_visits`

The function loads visits from the current day and the past `initial_load_past_days` on the first run. In subsequent runs, it continues from the last load and skips active visits until they are closed.

```py
def matomo_visits(
    api_token: str = dlt.secrets.value,
    url: str = dlt.config.value,
    live_events_site_id: int = dlt.config.value,
    initial_load_past_days: int = 10,
    visit_timeout_seconds: int = 1800,
    visit_max_duration_seconds: int = 3600,
    get_live_event_visitors: bool = False,
) -> List[DltResource]:
   ...
```

`api_token`: API token for authentication, defaulting to "./dlt/secrets.toml".

`url`: Matomo server URL, defaulting to ".dlt/config.toml"

`live_events_site_id`: Website ID for live events.

`initial_load_past_days`: Days to load initially, defaulting to 10.

`visit_timeout_seconds`: Session timeout (in seconds) before a visit closes, defaulting to 1800.

`visit_max_duration_seconds`: Max visit duration (in seconds) before a visit closes, defaulting to 3600.

`get_live_event_visitors`: Retrieve unique visitor data, defaulting to False.

>Note: This is an [incremental](../../general-usage/incremental-loading) source method and loads the "last_date" from the state of the last pipeline run.

### Resource `get_last_visits`

This function retrieves site visits within a specified timeframe. If a start date is given, it begins from that date. If not, it retrieves all visits up until now.

```py
@dlt.resource(
    name="visits", write_disposition="append", primary_key="idVisit", selected=True
)
def get_last_visits(
    client: MatomoAPIClient,
    site_id: int,
    last_date: dlt.sources.incremental[float],
    visit_timeout_seconds: int = 1800,
    visit_max_duration_seconds: int = 3600,
    rows_per_page: int = 2000,
) -> Iterator[TDataItem]:
   ...
```

`site_id`: Unique ID for each Matomo site.

`last_date`: Last resource load date, if it exists.

`visit_timeout_seconds`: Time (in seconds) until a session is inactive and deemed closed. Default: 1800.

`visit_max_duration_seconds`: Maximum duration (in seconds) of a visit before closure. Default: 3600.

`rows_per_page`: Number of rows on each page.

:::note
This is an [incremental](../../general-usage/incremental-loading) resource method and loads the "last_date" from the state of the last pipeline run.
:::

### Transformer `visitors`

This function retrieves unique visit information from get_last_visits.

```py
@dlt.transformer(
    data_from=get_last_visits,
    write_disposition="merge",
    name="visitors",
    primary_key="visitorId",
)
def get_unique_visitors(
    visits: List[DictStrAny], client: MatomoAPIClient, site_id: int
) -> Iterator[TDataItem]:
   ...
```

`visits`: Recent visit data within the specified timeframe.

`client`: Interface for Matomo API calls.

`site_id`: Unique Matomo site identifier.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="matomo",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="matomo_data"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our [documentation](../../general-usage/pipeline).

1. To load the data from reports.

   ```py
   data_reports = matomo_reports()
   load_info = pipeline_reports.run(data_reports)
   print(load_info)
   ```
   > "site_id" defined in ".dlt/config.toml"

1. To load custom data from reports using queries.

   ```py
   queries = [
       {
           "resource_name": "custom_report_name",
           "methods": ["CustomReports.getCustomReport"],
           "date": "2023-01-01",
           "period": "day",
           "extra_params": {"idCustomReport": 1}, # ID of the report
       },
   ]

   site_id = 1 # ID of the site for which reports are being loaded

   load_data = matomo_reports(queries=queries, site_id=site_id)
   load_info = pipeline_reports.run(load_data)
   print(load_info)
   ```
   > You can pass queries and site_id in the ".dlt/config.toml" as well.

1. To load data from reports and visits.

   ```py
   data_reports = matomo_reports()
   data_events = matomo_visits()
   load_info = pipeline_reports.run([data_reports, data_events])
   print(load_info)
   ```

1. To load data on live visits and visitors, and only retrieve data from today.

   ```py
   load_data = matomo_visits(initial_load_past_days=1, get_live_event_visitors=True)
   load_info = pipeline_events.run(load_data)
   print(load_info)
   ```

<!--@@@DLT_TUBA matomo-->

