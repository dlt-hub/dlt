# Personio

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

Personio is a human resources management software that helps businesses streamline HR processes,
including recruitment, employee data management, and payroll, in one platform.

Our Personio verified source loads data using Perosnio API to your preferred
[destination](../destinations).

:::tip You can check out our pipeline example
[here](https://github.com/dlt-hub/verified-sources/blob/master/sources/personio_pipeline.py). 
:::

Resources that can be loaded using this verified source are:

| Name        | Description                                                                              |
|-------------|------------------------------------------------------------------------------------------|
| employees   | Retrieves company employees details. (Employees list, absense_entitlement, cost_centers) |
| absences    | Retrieves list of various types of employee absences                                     |
| attendances | Retrieves attendance records for each employee                                           |

## Setup Guide

### Grab credentials

To load data from Personio, you need to API credentials, `client_id` and `client_secret`:

1. Sign in to your Personio account, and ensure that your user account has API access rights.
1. Navigate to Settings > Integrations > API credentials.
1. Click on "Generate new credentials."
1. Assign necessary permissions to credentials, i.e. read access.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```bash
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

For more information, read the
[Walkthrough: Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   # Put your secret values and credentials here
   # Note: Do not share this file and do not push it to GitHub!
   [sources.personio]
   client_id = "papi-********-****-****-****-************" # please set me up!
   client_secret = "papi-************************************************" # please set me up!
   ```

1. Replace the value of `client_id` and `client_secret` with the one that
   [you copied above](#grab-credentials). This will ensure that your data-verified source can access
   your Personio API resources securely.

1. Next, follow the instructions in [Destinations](../destinations/duckdb) to add credentials for
   your chosen destination. This will ensure that your data is properly routed to its final
   destination.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```bash
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```bash
   python personio_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```bash
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `personio`, you may also use
   any custom name instead.

For more information, read the [Walkthrough: Run a pipeline.](../../walkthroughs/run-a-pipeline)

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `personio_source`

This function initializes class `PersonioAPI` in "personio/helpers.py" and returns data resources
like "employees", "absences", and "attendances".

```python
@dlt.source(name="personio")
def personio_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
) -> Iterable[DltResource]:
```

`client_id`: Generated ID for API access.

`client_secret`: Generated secret for API access.

`items_per_page`: Maximum number of items per page, defaults to 200.

### Resource `employees`

This resource retrieves data on all the employees in a company.

```python
 @dlt.resource(primary_key="id", write_disposition="merge")
    def employees(
        updated_at: dlt.sources.incremental[
            pendulum.DateTime
        ] = dlt.sources.incremental(
            "last_modified_at", initial_value=None, allow_external_schedulers=True
        ),
        items_per_page: int = items_per_page,
    ) -> Iterable[TDataItem]:
```

`updated_at`: The saved state of the last 'last_modified_at' value. It is used for
[incremental loading](../../general-usage/incremental-loading).

`items_per_page`: Maximum number of items per page, defaults to 200.

Like the `employees` resource discussed above, other resources `absences` and `attendances` load
data from the Personio API to your preferred destination.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
      pipeline_name="personio",  # Use a custom name if desired
      destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
      dataset_name="personio_data"  # Use a custom name if desired
   )
   ```

   :::note To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline). :::

1. To load employee data:

   ```python
   load_data = personio_source().with_resources("employees")
   print(pipeline.run(load_data))
   ```

1. To load data from all supported endpoints:

   ```python
   load_data = personio_source().with_resources("employees", "absences", "attendances")
   print(pipeline.run(load_data))
   ```
