# Notion

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://dlthub.com/community)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

[Notion](https://www.notion.so/) is a flexible workspace tool for organizing personal and
professional tasks, offering customizable notes, documents, databases, and more.

This Notion `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/notion_pipeline.py)
loads data using “Notion API” to the destination of your choice.

Sources that can be loaded using this verified source are:

| Name             | Description                           |
|------------------|---------------------------------------|
| notion_databases | Retrieves data from Notion databases. |

## Setup Guide

### Grab credentials

1. If you don't already have a Notion account, please create one.
1. Access your Notion account and navigate to
   [My Integrations](https://www.notion.so/my-integrations).
1. Click "New Integration" on the left and name it appropriately.
1. Finally, click on "Submit" located at the bottom of the page.


### Add a connection to the database

1. Open the database that you want to load to the destination.

1. Click on the three dots located in the top right corner and choose "Add connections".

   ![Notion Database](./docs_images/Notion_Database_2.jpeg)

1. From the list of options, select the integration you previously created and click on "Confirm".

> Note: The Notion UI, which is described here, might change.
The full guide is available at [this link.](https://developers.notion.com/docs/authorization)


### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```bash
   dlt init notion duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/notion_pipeline.py)
   with Notion as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   # Put your secret values and credentials here
   # Note: Do not share this file and do not push it to GitHub!
   [source.notion]
   api_key = "set me up!" # Notion API token (e.g. secret_XXX...)
   ```

1. Replace the value of `api_key` with the one that [you copied above](notion.md#grab-credentials).
   This will ensure that your data-verified source can access your Notion resources securely.

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
   python notion_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```bash
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `notion`, you may also use any
   custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `notion_databases`

This function loads notion databases from notion into the destination.

```python
@dlt.source
def notion_databases(
    database_ids: Optional[List[Dict[str, str]]] = None,
    api_key: str = dlt.secrets.value,
) -> Iterator[DltResource]:
```

`database_ids`: A list of dictionaries each containing a database id and a name.

`api_key`: The Notion API secret key.

> If "database_ids" is None, the source fetches data from all integrated databases in your Notion
> account.

It is important to note that the data is loaded in “replace” mode where the existing data is
completely replaced.


## Customization
### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
      pipeline_name="notion",  # Use a custom name if desired
      destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
      dataset_name="notion_database"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline).

1. To load all the integrated databases:

   ```python
   load_data = notion_databases()
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

1. To load the custom databases:

   ```python
   selected_database_ids = [{"id": "0517dae9409845cba7d","use_name":"db_one"}, {"id": "d8ee2d159ac34cfc"}]
   load_data = notion_databases(database_ids=selected_database_ids)
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   The Database ID can be retrieved from the URL. For example if the URL is:

   ```shell
   https://www.notion.so/d8ee2d159ac34cfc85827ba5a0a8ae71?v=c714dec3742440cc91a8c38914f83b6b
   ```

   > The database ID in the given Notion URL is: "d8ee2d159ac34cfc85827ba5a0a8ae71".

The database ID in a Notion URL is the string right after notion.so/, before any question marks. It
uniquely identifies a specific page or database.

The database name ("use_name") is optional; if skipped, the pipeline will fetch it from Notion
automatically.

<!--@@@DLT_SNIPPET_START tuba::notion-->
## Additional Setup guides

- [Load data from Notion to BigQuery in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-bigquery)
- [Load data from Notion to AWS Athena in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-athena)
- [Load data from Notion to Redshift in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-redshift)
- [Load data from Notion to Azure Synapse in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-synapse)
- [Load data from Notion to PostgreSQL in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-postgres)
- [Load data from Notion to Databricks in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-databricks)
- [Load data from Notion to DuckDB in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-duckdb)
- [Load data from Notion to Snowflake in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-snowflake)
- [Load data from Notion to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-mssql)
<!--@@@DLT_SNIPPET_END tuba::notion-->