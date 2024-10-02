---
title: Jira
description: dlt verified source for Atlassian Jira
keywords: [jira api, jira verified source, jira]
---
import Header from './_source-info-header.md';

# Jira

<Header/>

[Jira](https://www.atlassian.com/software/jira) by Atlassian helps teams manage projects and tasks
efficiently, prioritize work, and collaborate.

This Jira `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira_pipeline.py)
loads data using the Jira API to the destination of your choice.

The endpoints that this verified source supports are:

| Name      | Description                                                                              |
| --------- | ---------------------------------------------------------------------------------------- |
| issues    | Individual pieces of work to be completed                                                |
| users     | Administrators of a given project                                                        |
| workflows | The key aspect of managing and tracking the progress of issues or tasks within a project |
| projects  | A collection of tasks that need to be completed to achieve a certain outcome             |

To get a complete list of sub-endpoints that can be loaded, see
[jira/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira/settings.py)

## Setup guide

### Grab credentials

1. Log in to your Jira account.

1. Navigate to the Jira project you have created.

1. Click on your profile picture in the top right corner, and choose "Manage Account."

1. Go to the "Security" tab and select "Create and manage API tokens."

1. Click "Create API Token," provide a descriptive name, and click the "Create" button.

1. Safely copy the newly generated access token.

> Note: The Jira UI, which is described here, might change.
The full guide is available at [this link.](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/)


### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init jira duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira_pipeline.py)
   with Jira as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as
   the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

   Here's what the file looks like:

   ```toml
   # put your secret values and credentials here. Please do not share this file, and do not push it to GitHub
   [sources.jira]
   subdomain = "set me up!" # please set me up!
   email = "set me up!" # please set me up!
   api_token = "set me up!" # please set me up!
   ```

1. A subdomain in a URL identifies your Jira account. For example, in "https://example.atlassian.net", "example" is the subdomain.

1. Use the email address associated with your Jira account.

1. Replace the "api_token" value with the [previously copied one](jira.md#grab-credentials) to ensure secure access to your Jira account.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to add credentials for your chosen destination, ensuring proper routing of your data to the final destination.

For more information, read [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python jira_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `jira_pipeline`. You may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Default endpoints

You can write your own pipelines to load data to a destination using this verified source. However, it is important to note the complete list of the default endpoints given in [jira/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/jira/settings.py)

### Source `jira`

This source function creates a list of resources to load data into the destination.

```py
@dlt.source
def jira(
     subdomain: str = dlt.secrets.value,
     email: str = dlt.secrets.value,
     api_token: str = dlt.secrets.value,
) -> Iterable[DltResource]:
   ...
```

- `subdomain`: The subdomain of the Jira account. Configured in ".dlt/secrets.toml".
- `email`: The email associated with the Jira account. Configured in ".dlt/secrets.toml".
- `api_token`: The API token for accessing the Jira account. Configured in ".dlt/secrets.toml".

### Source `jira_search`

This function returns a resource for querying issues using JQL [(Jira Query Language)](https://support.atlassian.com/jira-service-management-cloud/docs/use-advanced-search-with-jira-query-language-jql/).

```py
@dlt.source
def jira_search(
     subdomain: str = dlt.secrets.value,
     email: str = dlt.secrets.value,
     api_token: str = dlt.secrets.value,
) -> Iterable[DltResource]:
   ...
```

The above function uses the same arguments `subdomain`, `email`, and `api_token` as described above for the [jira source](jira.md#source-jira).

### Resource `issues`

The resource function searches issues using JQL queries and then loads them to the destination.

```py
@dlt.resource(write_disposition="replace")
def issues(jql_queries: List[str]) -> Iterable[TDataItem]:
   api_path = "rest/api/3/search"
   return {}  # return the retrieved values here
```

`jql_queries`: Accepts a list of JQL queries.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods as discussed above.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our documentation [here](../../general-usage/pipeline):

    ```py
    pipeline = dlt.pipeline(
        pipeline_name="jira_pipeline",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="jira"  # Use a custom name if desired
    )
    ```

2. To load custom endpoints such as "issues" and "users" using the jira source function:

    ```py
    # Run the pipeline
    load_info = pipeline.run(jira().with_resources("issues", "users"))
    print(f"Load Information: {load_info}")
    ```

3. To load custom issues using JQL queries, you can use custom queries. Here is an example below:

    ```py
    # Define the JQL queries as follows
    queries = [
              "created >= -30d order by created DESC",
              'created >= -30d AND project = DEV AND issuetype = Epic AND status = "In Progress" order by created DESC',
              ]
    # Run the pipeline
    load_info = pipeline.run(jira_search().issues(jql_queries=queries))
    # Print Load information
    print(f"Load Information: {load_info}")
    ```

<!--@@@DLT_TUBA jira-->

