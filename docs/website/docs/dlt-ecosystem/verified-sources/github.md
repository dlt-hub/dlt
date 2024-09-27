---
title: GitHub
description: dlt verified source for GitHub API
keywords: [github api, github verified source, github]
---
import Header from './_source-info-header.md';

# GitHub

<Header/>

This verified source can be used to load data on issues or pull requests from any GitHub repository onto a [destination](../../dlt-ecosystem/destinations) of your choice using the [GitHub API](https://docs.github.com/en/rest?apiVersion=2022-11-28).

Resources that can be loaded using this verified source are:

| Name             | Description                                                                      |
| ---------------- |----------------------------------------------------------------------------------|
| github_reactions | Retrieves all issues, pull requests, comments, and reactions associated with them |
| github_repo_events      | Gets all the repo events associated with the repository                   |

## Setup guide

### Grab credentials

To get the API token, sign in to your GitHub account and follow these steps:

1. Click on your profile picture in the top right corner.

1. Choose "Settings".

1. Select "Developer settings" on the left panel.

1. Under "Personal access tokens", click on "Generate a personal access token (preferably under Tokens(classic))".

1. Grant at least the following scopes to the token by checking them.

   | Scope           | Description                                                                             |
   | --------------- | --------------------------------------------------------------------------------------- |
   | public_repo     | Limits access to public repositories                                                    |
   | read:repo_hook  | Grants read and ping access to hooks in public or private repositories                  |
   | read:org        | Read-only access to organization membership, organization projects, and team membership |
   | read:user       | Grants access to read a user's profile data                                             |
   | read:project    | Grants read-only access to user and organization projects                               |
   | read:discussion | Allows read access for team discussions                                                 |

1. Finally, click "Generate token".

1. Copy the token and save it. This is to be added later in the `dlt` configuration.

> You can optionally add API access tokens to avoid making requests as an unauthorized user.
> If you wish to load data using the github_reaction source, the access token is mandatory.

For more information, see the
[GitHub authentication](https://docs.github.com/en/rest/overview/authenticating-to-the-rest-api?apiVersion=2022-11-28#basic-authentication)
and
[GitHub API token scopes](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/scopes-for-oauth-apps)
documentation.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init github duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/github_pipeline.py)
   with GitHub as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. In `.dlt/secrets.toml`, you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe. Here's what the file looks like:

   ```toml
   # Put your secret values and credentials here
   # GitHub access token (must be classic for reactions source)
   [sources.github]
   access_token="please set me up!" # use GitHub access token here
   ```

1. Replace the API token value with the [previously copied one](#grab-credentials) to ensure secure access to your GitHub resources.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to add credentials for your chosen destination, ensuring proper routing of your data to the final destination.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python github_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `github_reactions`; you may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Source `github_reactions`

This `dlt.source` function uses GraphQL to fetch DltResource objects: issues and pull requests along with associated reactions, comments, and reactions to comments.

```py
@dlt.source
def github_reactions(
    owner: str,
    name: str,
    access_token: str = dlt.secrets.value,
    items_per_page: int = 100,
    max_items: int = None,
    max_item_age_seconds: float = None,
) -> Sequence[DltResource]:
   ...
```

`owner`: Refers to the owner of the repository.

`name`: Refers to the name of the repository.

`access_token`: A classic access token should be utilized and is stored in the `.dlt/secrets.toml` file.

`items_per_page`: The number of issues/pull requests to retrieve in a single page. Defaults to 100.

`max_items`: The maximum number of issues/pull requests to retrieve in total. If set to None, it means all items will be retrieved. Defaults to None.

`max_item_age_seconds`: The feature to restrict retrieval of items older than a specific duration is yet to be implemented. Defaults to None.

### Resource `_get_reactions_data` ("issues")

The `dlt.resource` function employs the `_get_reactions_data` method to retrieve data about issues, their associated comments, and subsequent reactions.

```py
dlt.resource(
    _get_reactions_data(
        "issues",
        owner,
        name,
        access_token,
        items_per_page,
        max_items,
        max_item_age_seconds,
    ),
    name="issues",
    write_disposition="replace",
),
```

### Source `github_repo_events`

This `dlt.source` fetches repository events incrementally, dispatching them to separate tables based on event type. It loads new events only and appends them to tables.

> Note: GitHub allows retrieving up to 300 events for public repositories, so frequent updates are recommended for active repos.

```py
@dlt.source(max_table_nesting=2)
def github_repo_events(
    owner: str, name: str, access_token: str = None
) -> DltResource:
   ...
```

`owner`: Refers to the owner of the repository.

`name`: Denotes the name of the repository.

`access_token`: Optional classic or fine-grained access token. If not provided, calls are made anonymously.

`max_table_nesting=2` sets the maximum nesting level to 2.

Read more about [nesting levels](../../general-usage/source#reduce-the-nesting-level-of-generated-tables).

### Resource `repo_events`

This `dlt.resource` function serves as the resource for the `github_repo_events` source. It yields repository events as data items.

```py
dlt.resource(primary_key="id", table_name=lambda i: i["type"])  # type: ignore
def repo_events(
    last_created_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "created_at", initial_value="1970-01-01T00:00:00Z", last_value_func=max
    )
) -> Iterator[TDataItems]:
   ...
```

`primary_key`: Serves as the primary key, instrumental in preventing data duplication.

`table_name`: Routes data to appropriate tables based on the data type.

`last_created_at`: This parameter determines the initial value for "last_created_at" in dlt.sources.incremental. If no value is given, the default "initial_value" is used. The function "last_value_func" determines the most recent 'created_at' value.

Read more about [incremental loading](../../general-usage/incremental-loading#incremental_loading-with-last-value).

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="github_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="github_reaction_data"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our [documentation](../../general-usage/pipeline).

1. To load all the data from the repo on issues, pull requests, their comments, and reactions, you can do the following:

   ```py
   load_data = github_reactions("duckdb", "duckdb")
   load_info = pipeline.run(load_data)
   print(load_info)
   ```
   Here, "duckdb" is the owner of the repository and the name of the repository.

1. To load only the first 100 issues, you can do the following:

   ```py
   load_data = github_reactions("duckdb", "duckdb", max_items=100)
   load_info = pipeline.run(load_data.with_resources("issues"))
   print(load_info)
   ```

1. You can fetch and process repo events data incrementally. It loads all data during the first run and incrementally in subsequent runs.

   ```py
   load_data = github_repo_events(
       "duckdb", "duckdb", access_token=os.getenv(ACCESS_TOKEN)
   )
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   It is optional to use `access_token` or make anonymous API calls.

<!--@@@DLT_TUBA github-->

