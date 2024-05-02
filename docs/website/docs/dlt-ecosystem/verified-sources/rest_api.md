---
title: REST API generic source
description: dlt verified source for REST APIs
keywords: [rest api, restful api]
---
import Header from './_source-info-header.md';

# REST API Generic Source

<Header/>

This is a generic dlt source you can use to extract data from any REST API. It uses declarative configuration to define the API endpoints, their relationships, parameters, pagination, and authentication.

## Setup Guide

### Initialize the verified source

Enter the following command:

   ```sh
   dlt init rest_api duckdb
   ```

[dlt init](../../reference/command-line-interface) will initialize the pipeline example with REST API as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

## Add credentials

In the `.dlt` folder, you'll find a file called `secrets.toml`, where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

The GitHub API requires an access token to be set in the `secrets.toml` file.
Here is an example of how to set the token in the `secrets.toml` file:

```toml
[sources.rest_api.github]
github_token = "your_github_token"
```

## Run the pipeline

1. Install the required dependencies by running the following command:

   ```sh
   pip install -r requirements.txt
   ```

2. Run the pipeline:

   ```sh
    python rest_api_pipeline.py
    ```

3. Verify that everything loaded correctly by using the following command:

    ```sh
    dlt pipeline rest_api show
    ```

## Source Configuration

Let's take a look at the GitHub example in `rest_api_pipeline.py` file:

```python
def load_github() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    github_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
            "auth": {
                "token": dlt.secrets["github_token"],
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "per_page": 100,
                },
            },
        },
        "resources": [
            {
                "name": "issues",
                "endpoint": {
                    "path": "issues",
                    "params": {
                        "sort": "updated",
                        "direction": "desc",
                        "state": "open",
                        "since": {
                            "type": "incremental",
                            "cursor_path": "updated_at",
                            "initial_value": "2024-01-25T11:21:28Z",
                        },
                    },
                },
            },
            {
                "name": "issue_comments",
                "endpoint": {
                    "path": "issues/{issue_number}/comments",
                    "params": {
                        "issue_number": {
                            "type": "resolve",
                            "resource": "issues",
                            "field": "number",
                        }
                    },
                },
                "include_from_parent": ["id"],
            },
        ],
    }

    github_source = rest_api_source(github_config)

    load_info = pipeline.run(github_source)
    print(load_info)
```

The declarative configuration is defined in the `github_config` dictionary. It contains the following key components:

1. `client`: Defines the base URL and authentication method for the API. In this case it uses token-based authentication. The token is stored in the `secrets.toml` file.

2. `resource_defaults`: Contains default settings for all resources.

3. `resources`: A list of resources to be loaded. In this example, we have two resources: `issues` and `issue_comments`. Which correspond to the GitHub API endpoints for issues and issue comments.

Each resource has a name and an endpoint configuration. The endpoint configuration includes:

- `path`: The path to the API endpoint.
- `method`: The HTTP method to be used. Default is `GET`.
- `params`: Query parameters to be sent with each request. For example, `sort` to order the results.
- `json`: The JSON payload to be sent with the request (for POST and PUT requests).
- `paginator`: Configuration for paginating the results.
- `data_selector`: A JSON path to select the data from the response.
- `response_actions`: A list of actions that define how to process the response data.
- `incremental`: Configuration for incremental loading.

When you pass this configuration to the `rest_api_source` function, it creates a dlt source object that can be used with the pipeline.

`rest_api_source` function takes the following arguments:

- `config`: The REST API configuration dictionary.
- `name`: An optional name for the source.
- `section`: An optional section name in the configuration file.
- `max_table_nesting`: Sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.
- `root_key` (bool): Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge. Defaults to False.
- `schema_contract`: Schema contract settings that will be applied to this resource.
- `spec`: A specification of configuration and secret values required by the source.

## Define Resource Relationships

When you have a resource that depends on another resource, you can define the relationship using the resolve field type.

In the GitHub example, the `issue_comments` resource depends on the `issues` resource. The `issue_number` parameter in the `issue_comments` endpoint configuration is resolved from the `number` field of the `issues` resource.

```python
{
    "name": "issue_comments",
    "endpoint": {
        "path": "issues/{issue_number}/comments",
        "params": {
            "issue_number": {
                "type": "resolve",
                "resource": "issues",
                "field": "number",
            }
        },
    },
},
```

This configuration tells the source to get issue numbers from the `issues` resource and use them to fetch comments for each issue.

## Incremental Loading

To set up incremental loading for a resource, you can use two options:

1. Defining a special parameter in the `params` section of the endpoint configuration:

    ```python
    "<parameter_name>": {
        "type": "incremental",
        "cursor_path": "<path_to_cursor_field>",
        "initial_value": "<initial_value>",
    },
    ```

    For example, in the `issues` resource configuration in the GitHub example, we have:

    ```python
    "since": {
        "type": "incremental",
        "cursor_path": "updated_at",
        "initial_value": "2024-01-25T11:21:28Z",
    },
    ```

    This configuration tells the source to create an incremental object that will keep track of the `updated_at` field in the response and use it as a value for the `since` parameter in subsequent requests.

2. Specifying the `incremental` field in the endpoint configuration:

    ```python
    "incremental": {
        "start_param": "<parameter_name>",
        "end_param": "<parameter_name>",
        "cursor_path": "<path_to_cursor_field>",
        "initial_value": "<initial_value>",
        "end_value": "<end_value>",
    },
    ```

    This configuration is more flexible and allows you to specify the start and end conditions for the incremental loading.

