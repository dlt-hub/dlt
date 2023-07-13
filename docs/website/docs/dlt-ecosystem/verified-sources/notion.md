# Notion

:::info
Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::


Notion is a tool that allows users to organize and manage their personal and professional lives.
It provides a flexible workspace where you can create and customize various types of digital content,
such as notes, documents, databases, task lists, and more.

Using this Notion `dlt` verified source and pipeline example, you can load the ***databases*** from Notion to a [destination](../destinations/duckdb) of your choice.
In Notion, [databases](https://www.notion.so/help/intro-to-databases) are a powerful feature that allows you to create structured collections of information.
They are similar to spreadsheets or tables but with added flexibility and functionality.

## Grab API credentials

1. If you don't already have a Notion account, please create one.
2. Access your Notion account and navigate to [My Integrations](https://www.notion.so/my-integrations).
3. On the left-hand side, click on "New Integration" and provide a suitable name for the integration.
4. Finally, click on "Submit" located at the bottom of the page.

## Add a connection to the database

1. Open the database that you want to load to the destination.
2. Click on the three dots located at the top right corner and choose "Add connections".

    ![Notion Database](./docs_images/Notion_Database_2.jpeg)


3. From the list of options, select the integration you previously created and click on "Confirm".

## Initialize the verified source and pipeline example

To get started with your verified source and pipeline example follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:

    ```bash
    dlt init notion duckdb
    ```

    This command will initialize your verified source with Notion and creates a pipeline with duckdb as the destination.
    If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred destination.
    You can find supported destinations and their configuration options in our [documentation](../destinations/duckdb)

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

    ```
    notion_source
    ├── .dlt
    │   ├── config.toml
    │   └── secrets.toml
    ├── notion
    │   ├── helpers
    │   │  ├── __init__.py
    │   │  ├── client.py
    │   │  └── database.py
    │   ├── __init__.py
    │   ├── README.md
    │   └── settings.py
    ├── .gitignore
    ├── requirements.txt
    └── notion_pipeline.py
    ```


## Add credentials

1. Inside the `.dlt` folder, you'll find a file called “*secrets.toml*”, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

Here's what the file looks like:

```toml
# Put your secret values and credentials here
# Note: Do not share this file and do not push it to GitHub!
[source.notion]
api_key = "set me up!" # Notion API token (e.g. secret_XXX...)
```

2. Replace the value of `api_key` with the one that [you copied above](notion.md#grab-api-credentials). This will ensure that your data-verified source can access your notion resources securely.
3. Next, follow the instructions in [Destinations](../destinations/duckdb) to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:

    ```bash
    pip install -r requirements.txt
    ```

2. Now the pipeline can be run by using the command:

    ```bash
    python3 notion_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:

    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline example is `notion`, you may also use any custom name instead.


## Customizations

To load data to the destination using `dlt`, you have the option to write your own methods.

### Source and resource methods

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source)
and [resources](https://dlthub.com/docs/general-usage/resource) that for this verified
source are found in the `__init__.py` file within the *notion* directory.
This verified source has one default method:

```python
@dlt.source
def notion_databases(
    database_ids: Optional[List[Dict[str, str]]] = None,
    api_key: str = dlt.secrets.value,
) -> Iterator[DltResource]:

```

- **`database_ids`**: A list of dictionaries each containing a database id and a name.
                      If `database_ids` is None, then the source retrieves data from all existed databases in your Notion account.
- **`api_key`**: The Notion API secret key.

The above function yields data resources from the Notion databases.
It is important to note that the data is loaded in “replace” mode where the existing data is completely replaced.

That’s it! Enjoy running your Notion DLT verified source!
