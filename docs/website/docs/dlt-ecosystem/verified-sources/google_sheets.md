---
title: Google Sheets
description: dlt verified source for Google Sheets API
keywords: [google sheets api, google sheets verified source, google sheets]
---
import Header from './_source-info-header.md';

# Google Sheets

<Header/>

[Google Sheets](https://www.google.com/sheets/about/) is a cloud-based spreadsheet application
offered by Google as part of its Google Workspace suite.

This Google Sheets `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_sheets_pipeline.py)
loads data using the “Google Sheets API” to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name               | Description                                                |
| ------------------ | ---------------------------------------------------------- |
| google_spreadsheet | Retrieves data from a Google Spreadsheet                   |
| range_names        | Processes the range and yields data from each range        |
| spreadsheet_info   | Information about the spreadsheet and the ranges processed |

## Setup guide

### Grab credentials

There are two methods to get authenticated for using this verified source:

- OAuth credentials
- Service account credentials

Here we'll discuss how to set up both OAuth tokens and service account credentials. In general,
OAuth tokens are preferred when user consent is required, while service account credentials are
better suited for server-to-server interactions. Here we recommend using service account
credentials. You can choose the method of authentication as per your requirement.

#### Google service account credentials

You need to create a GCP service account to get API credentials if you don't have one. To create
one, follow these steps:

1. Sign in to [console.cloud.google.com](http://console.cloud.google.com/).

1. [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create#creating) if
   needed.

1. Enable "Google Sheets API", refer
   [Google documentation](https://developers.google.com/sheets/api/guides/concepts) for
   comprehensive instructions on this process.

1. Generate credentials:

   1. Navigate to IAM & Admin in the console's left panel, and then select Service Accounts.
   1. Identify the service account you intend to use, and click on the three-dot menu under the
      "Actions" column next to it.
   1. Create a new JSON key by selecting "Manage Keys" > "ADD KEY" > "CREATE".
   1. You can download the ".json" file containing the necessary credentials for future use.

#### Google OAuth credentials

You need to create a GCP account to get OAuth credentials if you don't have one. To create one,
follow these steps:

1. Open a GCP project in your GCP account.

1. Enable the Sheets API in the project.

1. Search for credentials in the search bar and go to Credentials.

1. Go to Credentials -> OAuth client ID -> Select Desktop App from the Application type and give an
   appropriate name.

1. Download the credentials and fill `client_id`, `client_secret`, and `project_id` in
   `secrets.toml`.

1. Go back to credentials and select the OAuth consent screen on the left.

1. Fill in the App name, user support email (your email), authorized domain (localhost.com), and dev
   contact info (your email again).

1. Add the following scope:

   ```text
   "https://www.googleapis.com/auth/spreadsheets.readonly"
   ```

1. Add your email as a test user.

1. Generate `refresh_token`:

   After configuring "client_id", "client_secret", and "project_id" in "secrets.toml". To generate
   the refresh token, run the following script from the root folder:

   ```sh
   python google_sheets/setup_script_gcp_oauth.py
   ```

   Once you have executed the script and completed the authentication, you will receive a "refresh
   token" that can be used to set up the ".dlt/secrets.toml".

### Prepare your data



#### Share Google Sheet with the email

> Note: For service account authentication, use the client_email. For OAuth authentication, use the
> email associated with the app creation and refresh token generation.

To allow the API to access the Google Sheet, open the sheet that you wish to use and do the
following:

1. Select the share button in the top left corner.

   ![Share_Button](docs_images/Share_button.png)

1. In *Add people and groups*, add the "client_email" or "user_email" with at least viewer
   privileges.

   ![Add people](docs_images/Add_people.png)

#### Provide the spreadsheet ID/URL and explicit range names

When setting up the pipeline, you can use either the browser-copied URL of your spreadsheet:

```sh
https://docs.google.com/spreadsheets/d/1VTtCiYgxjAwcIw7UM1_BSaxC3rzIpr0HwXZwd2OlPD4/edit?usp=sharing
```

or the spreadsheet ID (which is a part of the URL)

```sh
1VTtCiYgxjAwcIw7UM1_BSaxC3rzIpr0HwXZwd2OlPD4
```

Typically, you pass it directly to the [google_spreadsheet function](#create-your-own-pipeline) or in [config.toml](#add-credentials) as defined here.

You can provide specific ranges to the `google_spreadsheet` pipeline, as detailed in the following.

#### Guidelines about headers

Make sure your data has headers and is in the form of a well-structured table.

The first row of any extracted range should contain headers. Please make sure:

1. The header names are strings and are unique.
1. All the columns that you intend to extract have a header.
1. The data starts exactly at the origin of the range - otherwise, a source will remove padding, but it
   is a waste of resources.
   > When a source detects any problems with headers or table layout, it will issue a WARNING in the
   > log. Hence, we advise running your pipeline script manually/locally and fixing all the problems.
1. Columns without headers will be removed and not extracted.
1. Columns with headers that do not contain any data will be removed.
1. If there are any problems with reading headers (i.e., the header is not a string or is empty or not
   unique): the headers row will be extracted as data and automatic header names will be used.
1. Empty rows are ignored.
1. `dlt` will normalize range names and headers into table and column names - so they may be
   different in the database than in Google Sheets. Prefer small cap names without special
   characters.

#### Guidelines about named ranges

We recommend using
[Named Ranges](https://support.google.com/docs/answer/63175?hl=en&co=GENIE.Platform%3DDesktop) to
indicate which data should be extracted from a particular spreadsheet, and this is how this source
will work by default - when called without setting any other options. All the named ranges will be
converted into tables, named after them, and stored in the destination.

1. You can let the spreadsheet users add and remove tables by just adding or removing the ranges;
   you do not need to configure the pipeline again.

1. You can indicate exactly the fragments of interest, and only this data will be retrieved, so it is
   the fastest.

1. You can name database tables by changing the range names.

1. In range_names, you can enter as follows:

   ```text
   range_names = ["Range_1","Range_2","Sheet1!A1:D10"]
   ```

1. You can pass explicit ranges to the Google Spreadsheet "ranged_names" as:

   | Name         | Example                                   |
   | ------------ | ----------------------------------------- |
   | Sheet names  | \["Sheet1","Sheet2","custom_sheet_name"\] |
   | Named ranges | \["range_name1","range_name2"\]           |
   | Any range    | \["Sheet1!A1:B7","Sheet2!B3:E15"\]        |

If you are not happy with the workflow above, you can:

1. Disable it by setting the `get_named_ranges` option to `False`.

1. Enable retrieving all sheets/tabs with the get_sheets option set to `True`.

1. Pass a list of ranges as supported by Google Sheets in range_names.

   > Note: To retrieve all named ranges with "get_named_ranges" or all sheets with "get_sheets"
   > methods, pass an empty `range_names` list as `range_names = []`. Even when you use a set
   > "get_named_ranges" to false, pass the range_names as an empty list to get all the sheets with
   > the "get_sheets" method.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init google_sheets duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_sheets_pipeline.py)
   with Google Sheets as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   [sources.google_sheets.credentials] ##CHECK IT
   project_id = "project_id" # please set me up!
   client_email = "client_email" # please set me up!
   private_key = "private_key" # please set me up!
   ```

1. From the ".json" that you
   [downloaded earlier](google_sheets.md#grab-google-service-account-credentials), copy
   `project_id`, `private_key`, and `client_email` under `[sources.google_sheets.credentials]`.

1. Alternatively, if you're using OAuth credentials, replace the fields and values with those
   you [grabbed for OAuth credentials](google_sheets.md#grab-google-oauth-credentials).

1. The secrets.toml for OAuth authentication looks like:

   ```toml
   [sources.google_sheets.credentials] ##CHECK IT
   client_id = "client_id" # please set me up!
   client_secret = "client_secret" # please set me up!
   refresh_token = "refresh_token" # please set me up!
   project_id = "project_id" # please set me up!
   ```

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

1. Next, you need to configure ".dlt/config.toml", which looks like:

   ```toml
   [sources.google_sheets]
   spreadsheet_url_or_id = "Please set me up!"
   range_names = ["Please set me up!"]
   ```

1. In range_names, you can enter values as discussed in
   [Guidelines about named ranges](google_sheets.md#guidelines-about-named-ranges).

1. Provide the spreadsheet URL or just its ID as the identifier. E.g., use either the full link:

   ```toml
   spreadsheet_identifier = "https://docs.google.com/spreadsheets/d/1VTtCiYgxjAwcIw7UM1_BSaxC3rzIpr0HwXZwd2OlPD4/edit?usp=sharing"
   ```

   or the spreadsheet ID (which is a part of the URL)

   ```toml
   spreadsheet_identifier="1VTtCiYgxjAwcIw7UM1_BSaxC3rzIpr0HwXZwd2OlPD4"
   ```

> Note: You have the option to pass "range_names" and "spreadsheet_identifier" directly to the
> google_spreadsheet function or in ".dlt/config.toml"

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```sh
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python google_sheets_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `google_sheets_pipeline`, you
   may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Data types

The `dlt` normalizer uses the first row of data to infer types and attempts to coerce subsequent rows, creating variant columns if unsuccessful. This is standard behavior.
If `dlt` did not correctly determine the data type in the column, or you want to change the data type for other reasons,
then you can provide a type hint for the affected column in the resource.
Also, since recently, `dlt` no longer recognizes date and time types, so you have to designate it yourself as `timestamp`.

Use the `apply_hints` method on the resource to achieve this.
Here's how you can do it:

```py
for resource in resources:
    resource.apply_hints(columns={
        "total_amount": {"data_type": "double"},
        "date": {"data_type": "timestamp"},
    })
```
In this example, the `total_amount` column is enforced to be of type double, and `date` is enforced to be of type timestamp.
This will ensure that all values in the `total_amount` column are treated as `double`, regardless of whether they are integers or decimals in the original Google Sheets data.
And the `date` column will be represented as dates, not integers.

For a single resource (e.g., `Sheet1`), you can simply use:
```py
source.Sheet1.apply_hints(columns={
    "total_amount": {"data_type": "double"},
    "date": {"data_type": "timestamp"},
})
```

To get the name of resources, you can use:
```py
print(source.resources.keys())
```

To read more about tables, columns, and datatypes, please refer to [our documentation here.](../../general-usage/schema#tables-and-columns)

:::caution
`dlt` will **not modify** tables after they are created.
So if you changed data types with hints,
then you need to **delete the dataset**
or set `dev_mode=True`.
:::

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `google_spreadsheet`

This function loads data from a Google Spreadsheet. It retrieves data from all specified ranges,
whether explicitly defined or named, and obtains metadata for the first two rows within each range.

```py
def google_spreadsheet(
      spreadsheet_url_or_id: str = dlt.config.value,
      range_names: Sequence[str] = dlt.config.value,
      credentials: Union[
          GcpOAuthCredentials, GcpServiceAccountCredentials
      ] = dlt.secrets.value,
      get_sheets: bool = False,
      get_named_ranges: bool = True,
) -> Iterable[DltResource]:
   ...
```

`spreadsheet_url_or_id`: ID or URL of the Google Spreadsheet.

`range_names`: List of ranges (in Google Sheets format) to be transformed into tables.

`credentials`: GCP credentials with Google Sheets API access.

`get_sheets`: If true, imports all spreadsheet sheets into the database.

`get_named_ranges`: If true, imports either all named ranges or those
[specified](google_sheets.md#guidelines-about-named-ranges) into the database.

### Resource `range_names`

This function processes each range name provided by the source function, loading its data into
separate tables in the destination.

```py
dlt.resource(
     process_range(rows_data, headers=headers, data_types=data_types),
     name=name,
     write_disposition="replace",
)
```

`process_range`: Function handles rows from a specified Google Spreadsheet range, taking data rows,
headers, and data types as arguments.

`name`: Specifies the table's name, derived from the spreadsheet range.

`write_disposition`: Dictates how data is loaded to the destination.

> Please note:
>
> 1. Empty rows are ignored.
> 1. Empty cells are converted to None (and then to NULL by dlt).
> 1. Data in columns without headers will be dropped.

### Resource `spreadsheet_info`

This resource loads the information about the sheets and range names into the destination as a table.
This table refreshes after each load, storing information on loaded ranges:

- Spreadsheet ID and title.
- Range name as given to the source.
- String and parsed representation of the loaded range.

```py
dlt.resource(
     metadata_table,
     write_disposition="merge",
     name="spreadsheet_info",
     merge_key="spreadsheet_id",
)
```

`metadata_table`: Contains metadata about the spreadsheet and the ranges processed.

`name`: Denotes the table name, set here as "spreadsheet_info".

`write_disposition`: Dictates how data is loaded into the destination.
[Read more](../../general-usage/incremental-loading#the-3-write-dispositions).

`merge_key`: This parameter is used to specify the column used to identify records for merging. In this
case, "spreadsheet_id" means that the records will be merged based on the values in this column.
[Read more](../../general-usage/incremental-loading#merge-incremental_loading).

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
        pipeline_name="google_sheets",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="google_spreadsheet_data"  # Use a custom name if desired
   )
   ```

1. To load data from explicit range names:

   ```py
   load_data = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580/edit#gid=0", # Spreadsheet URL
        range_names=["range_name1", "range_name2"], # Range names
        get_sheets=False,
        get_named_ranges=False,
   )
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   > Note: You can pass the URL or spreadsheet ID and range names explicitly or in ".dlt/config.toml".

1. To load all the range_names from the spreadsheet:

   ```py
   load_data = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580/edit#gid=0", # Spreadsheet URL
        get_sheets=False,
        get_named_ranges=True,
   )
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   > Pass an empty list to range_names in ".dlt/config.toml" to retrieve all range names.

1. To load all the sheets from the spreadsheet:

   ```py
   load_data = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580/edit#gid=0", # Spreadsheet URL
        get_sheets=True,
        get_named_ranges=False,
   )
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   > Pass an empty list to range_names in ".dlt/config.toml" to retrieve all sheets.

1. To load all the sheets and range_names:

   ```py
   load_data = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580/edit#gid=0", # Spreadsheet URL
        get_sheets=True,
        get_named_ranges=True,
   )
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   > Pass an empty list to range_names in ".dlt/config.toml" to retrieve all sheets and range names.

1. To load data from multiple spreadsheets:

   ```py
   load_data1 = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/43lkHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580/edit#gid=0", # Spreadsheet URL
        range_names=["Sheet 1!A1:B10"],
        get_named_ranges=False,
   )

   load_data2 = google_spreadsheet(
        "https://docs.google.com/spreadsheets/d/3jo4HjqouQnnCIZAFa2rL6vT91YRN8aIhts22SKKO390/edit#gid=0", # Spreadsheet URL
        range_names=["Sheet 1!B1:C10"],
        get_named_ranges=True,
   )
   load_info = pipeline.run([load_data1, load_data2])
   print(load_info)
   ```

1. To load with table rename:

   ```py
   load_data = google_spreadsheet(
    "https://docs.google.com/spreadsheets/d/43lkHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580/edit#gid=0", # Spreadsheet URL
     range_names=["Sheet 1!A1:B10"],
     get_named_ranges=False,
   )

   data.resources["Sheet 1!A1:B10"].apply_hints(table_name="loaded_data_1")

   load_info = pipeline.run(load_data)
   print(load_info)
   ```

### Using Airflow with Google spreadsheets

Consider the following when using Google spreadsheets with Airflow:

`Efficient data retrieval`

- Our source fetches all required data with just two API calls, regardless of the number of specified data ranges. This allows for swift data loading from google_spreadsheet before executing the pipeline.

`Airflow specificity`

- With Airflow, data source creation and execution are distinct processes.
- If your execution environment (runner) is on a different machine, this might cause the data to be loaded twice, leading to inefficiencies.

`Airflow helper caution`
- Avoid using `scc decomposition` because it unnecessarily creates a new source instance for every specified data range. This is not efficient and can cause redundant tasks.

#### Recommended Airflow deployment

Below is the correct way to set up an Airflow DAG for this purpose:

- Define a DAG to run daily, starting from February 1, 2023. It avoids catching up for missed runs and ensures only one instance runs at a time.

- Data is imported from Google spreadsheets and directed to BigQuery.

- When adding the Google Spreadsheet task to the pipeline, avoid decomposing it; run it as a single task for efficiency.

```py
@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args
)
def get_named_ranges():
    tasks = PipelineTasksGroup("get_named_ranges", use_data_folder=False, wipe_local_data=True)

    # Import your source from pipeline script
    from google_sheets import google_spreadsheet

    pipeline = dlt.pipeline(
        pipeline_name="get_named_ranges",
        dataset_name="named_ranges_data",
        destination='bigquery',
    )

    # Do not use decompose to run `google_spreadsheet` in single task
    tasks.add_run(pipeline, google_spreadsheet("1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580"), decompose="none", trigger_rule="all_done", retries=0, provide_context=True)
```

<!--@@@DLT_TUBA google_sheets-->

