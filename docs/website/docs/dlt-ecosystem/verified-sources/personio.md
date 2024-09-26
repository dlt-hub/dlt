---
title: Personio
description: dlt verified source for Personio API
keywords: [personio api, personio verified source, personio]
---
import Header from './_source-info-header.md';

# Personio

<Header/>

Personio is a human resources management software that helps businesses streamline HR processes,
including recruitment, employee data management, and payroll, in one platform.

Our [Personio verified](https://github.com/dlt-hub/verified-sources/blob/master/sources/personio) source loads data using the Personio API to your preferred
[destination](../destinations).

:::tip
You can check out our pipeline example [here](https://github.com/dlt-hub/verified-sources/blob/master/sources/personio_pipeline.py).
:::

Resources that can be loaded using this verified source are:

| Name                       | Description                                                                       | Endpoint                                          |
|----------------------------|-----------------------------------------------------------------------------------|---------------------------------------------------|
| employees                  | Retrieves company employees' details                                              | /company/employees                                |
| absences                   | Retrieves absence periods for absences tracked in days                            | /company/time-offs                                |
| absences_types             | Retrieves a list of various types of employee absences                            | /company/time-off-types                           |
| attendances                | Retrieves attendance records for each employee                                    | /company/attendances                              |
| projects                   | Retrieves a list of all company projects                                          | /company/attendances/projects                     |
| document_categories        | Retrieves all document categories of the company                                  | /company/document-categories                      |
| employees_absences_balance | The transformer retrieves the absence balance for a specific employee             | /company/employees/{employee_id}/absences/balance |
| custom_reports_list        | Retrieves metadata about existing custom reports (name, report type, report date) | /company/custom-reports/reports                   |
| custom_reports             | The transformer for custom reports                                                | /company/custom-reports/reports/{report_id}       |

## Setup guide

### Grab credentials

To load data from Personio, you need to obtain API credentials, `client_id` and `client_secret`:

1. Sign in to your Personio account, and ensure that your user account has API access rights.
1. Navigate to Settings > Integrations > API credentials.
1. Click on "Generate new credentials."
1. Assign necessary permissions to credentials, i.e., read access.

:::info
The Personio UI, which is described here, might change. The full guide is available at this [link.](https://developer.personio.de/docs#21-employee-attendance-and-absence-endpoints)
:::

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init personio duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/personio_pipeline.py)
   with Personio as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read [Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   # Put your secret values and credentials here
   # Note: Do not share this file and do not push it to GitHub!
   [sources.personio]
   client_id = "papi-*****" # please set me up!
   client_secret = "papi-*****" # please set me up!
   ```

1. Replace the value of `client_id` and `client_secret` with the one that
   [you copied above](#grab-credentials). This will ensure that your data-verified source can access
   your Personio API resources securely.

1. Next, follow the instructions in [Destinations](../destinations/duckdb) to add credentials for
   your chosen destination. This will ensure that your data is properly routed to its final
   destination.

For more information, read [Credentials](../../general-usage/credentials).

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python personio_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `personio`, you may also use
   any custom name instead.

For more information, read [Run a pipeline.](../../walkthroughs/run-a-pipeline)

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `personio_source`

This `dlt` source returns data resources like `employees`, `absences`, `absence_types`, etc.
```py
@dlt.source(name="personio")
def personio_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
) -> Iterable[DltResource]:
    ...
    return (
    employees,
    absence_types,
    absences,
    attendances,
    projects,
    document_categories,
    employees_absences_balance,
    custom_reports_list,
    custom_reports,
)
```

`client_id`: Generated ID for API access.

`client_secret`: Generated secret for API access.

`items_per_page`: Maximum number of items per page, defaults to 200.

### Resource `employees`

This resource retrieves data on all the employees in a company.

```py
@dlt.resource(primary_key="id", write_disposition="merge")
def employees(
    updated_at: dlt.sources.incremental[
        pendulum.DateTime
    ] = dlt.sources.incremental(
        "last_modified_at", initial_value=None, allow_external_schedulers=True
    ),
    items_per_page: int = items_per_page,
) -> Iterable[TDataItem]:
    ...
```

`updated_at`: The saved state of the last 'last_modified_at' value. It is used for
[incremental loading](../../general-usage/incremental-loading).

`items_per_page`: Maximum number of items per page, defaults to 200.

`allow_external_schedulers`: A boolean that, if true, permits [external schedulers](../../general-usage/incremental-loading#using-airflow-schedule-for-backfill-and-incremental-loading) to manage incremental loading.

Like the `employees` resource discussed above, other resources `absences` and `attendances` load
data incrementally from the Personio API to your preferred destination.

### Resource `absence_types`

Simple resource, which retrieves a list of various types of employee absences.
```py
@dlt.resource(primary_key="id", write_disposition="replace")
def absence_types(items_per_page: int = items_per_page) -> Iterable[TDataItem]:
   ...
...
```

`items_per_page`: Maximum number of items per page, defaults to 200.

It is important to note that the data is loaded in `replace` mode where the existing data is
completely replaced.

In addition to the mentioned resource,
there are three more resources `projects`, `custom_reports_list`, and `document_categories`
with similar behavior.

### Resource-transformer `employees_absences_balance`

Besides these source and resource functions, there are two transformer functions
for endpoints like `/company/employees/{employee_id}/absences/balance` and `/company/custom-reports/reports/{report_id}`.
The transformer functions transform or process data from resources.

The transformer function `employees_absences_balance` processes data from the `employees` resource.
It fetches and returns a list of the absence balances for each employee.

```py
@dlt.transformer(
    data_from=employees,
    write_disposition="merge",
    primary_key=["employee_id", "id"],
)
@dlt.defer
def employees_absences_balance(employees_item: TDataItem) -> Iterable[TDataItem]:
    ...
```
`employees_item`: The data item from the 'employees' resource.

It uses the `@dlt.defer` decorator to enable parallel run in thread pool.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the [pipeline](../../general-usage/pipeline) by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
      pipeline_name="personio",  # Use a custom name if desired
      destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
      dataset_name="personio_data"  # Use a custom name if desired
   )
   ```

1. To load employee data:

   ```py
   load_data = personio_source().with_resources("employees")
   print(pipeline.run(load_data))
   ```

1. To load data from all supported endpoints:

   ```py
   load_data = personio_source()
   print(pipeline.run(load_data))
   ```

