# Jira

Jira, developed by Atlassian, is project management and issue-tracking software designed to assist teams in effectively organizing and tracking their projects and tasks. It provides a centralized platform for collaboration, prioritization, and organization of work.

With the help of this `dlt` Jira Verified Source and pipeline example, you can load data from various endpoints of the Jira API to your desired destination. This enables you to efficiently transfer and utilize Jira data in a way that suits your specific needs.

This verified source has the following default endpoint:

| Endpoint | Description |
| --- | --- |
| issues | individual pieces of work that must be completed |
| users | administrator of a given project |
| workflows | the key aspect of managing and tracking the progress of issues or tasks within a project |
| projects | a collection of tasks that need to be completed to achieve a certain outcome |

## Grab Credentials

1. Log in to your Jira account.
2. Navigate to the Jira project you have created.
3. Click on your profile picture located in the top right corner, and choose "Manage Account."
4. Go to the "Security" tab and select "Create and manage API tokens."
5. Click on "Create API Token," provide a descriptive name, and click the "Create" button.
6. Safely copy the newly generated access token.

## Initialize the Jira verified source and the pipeline example

To get started with this verified source, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:

    ```bash
    dlt init jira duckdb
    ```

    This command will initialize your verified source with Jira and create a pipeline example with duckdb as the destination. If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](https://dlthub.com/docs/dlt-ecosystem/destinations).

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

    ```toml
    jira_source
    ├── .dlt
    │   ├── config.toml
    │   └── secrets.toml
    ├── jira
    │   └── __init__.py
    │   └── settings.py
    ├── .gitignore
    ├── requirements.txt
    └── jira_pipeline.py
    ```


## **Add credentials**

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

Here's what the file looks like:

```toml
# put your secret values and credentials here. do not share this file and do not push it to github
[sources.jira]
subdomain = "set me up!" # please set me up!
email = "set me up!" # please set me up!
api_token = "set me up!" # please set me up!
```

2. A subdomain refers to the unique identifier that is part of the URL used to access your Jira account. For example, if your Jira account URL is "https://example.atlassian.net", then "example" is the subdomain.
3. The email will be the email address associated with your Jira account used for login purposes.
4. Replace the API token with the ones that you [copied above](jira.md#grab-credentials). This will ensure that this source can access your Jira resources securely.
5. Finally, follow the instructions in **[Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:

```bash
pip install -r requirements.txt
```

2. In “jira_pipeline.py”, if you do not have any custom queries, use the method "*load(endpoints=None)*" exclusively. However, if you have custom queries, modify the queries in the main method and utilize "*load_query_data(queries=queries)*". To execute the verified source, use the following command:

```bash
python3 jira_pipeline.py
```

3. To make sure that everything is loaded as expected, use the command:

```bash
dlt pipeline <pipeline_name> show
```

For example, the pipeline_name for the above pipeline example is `jira_pipeline`, you may also use any custom name instead.

## Customizations

To load data to the destination using this verified source, you have the option to write your own methods.

### Source and resource methods

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source) and [resources](https://dlthub.com/docs/general-usage/resource) that for this verified source are found in the `__init__.py` file within the *jira* directory. This verified source has three default methods that form the basis of loading. The methods are:

Source <u>jira</u>:

```python
@dlt.source
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
) -> Iterable[DltResource]:
```

- **`subdomain`**: is a string parameter that represents the subdomain of the Jira account. It is defined in "*dlt.secrets.value*".
- **`email`:** is a string parameter that represents the email associated with the Jira account. It is defined in “*dlt.secrets.value*”
- **`api_token`:** is a string parameter that represents the API token for accessing the Jira account. It is defined in “*dlt.secrets.value*”

The source function creates a list of resources and iterates over each resource, calling the "get_paginated_data()" function to retrieve paginated data from the corresponding Jira API endpoint.

Source <u>jira_search</u>:

```python
@dlt.source
def jira_search(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
) -> Iterable[DltResource]:
```

This source function uses the “**subdomain**”, “**email**” and “**api_token**” for authentication. This source function generates a resource function for searching issues by using [JQL(Jira Query Language)](https://support.atlassian.com/jira-service-management-cloud/docs/use-advanced-search-with-jira-query-language-jql/) queries. This is the resource function used for searching issues.

Resource <u>issues</u>:

```python
@dlt.resource(write_disposition="replace")
def issues(jql_queries: List[str]) -> Iterable[TDataItem]:
    api_path = "rest/api/3/search"
```

The above resource function searches the issues using JQL queries. And returns issues to be loaded to the destination.

## Create Your Data Loading Pipeline

If you wish to create your own pipelines you can leverage source and resource methods as discussed above.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our documentation [here](https://dlthub.com/docs/general-usage/pipeline).

```python
pipeline = dlt.pipeline(
    pipeline_name="jira_pipeline",  # Use a custom name if desired
    destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
    dataset_name="jira"  # Use a custom name if desired
)
```

2. To load custom endpoints such as “issues” and “users” using the `jira` source function:

```python
#Run the pipeline
load_info = pipeline.run(jira().with_resources("issues","users"))

# Print Load information
print(f"Load Information: {load_info}")
```

3. To load the custom issues using JQL queries, you can use your custom queries, here is an example below, you can use the following `jira_search` source function:

```python
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


That’s it! Enjoy running your `dlt` Jira pipeline!
