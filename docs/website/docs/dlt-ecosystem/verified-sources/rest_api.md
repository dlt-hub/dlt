---
title: REST API generic source
description: dlt verified source for REST APIs
keywords: [rest api, restful api]
---
import Header from './_source-info-header.md';

<Header/>

This is a generic dlt source you can use to extract data from any REST API. It uses [declarative configuration](#source-configuration) to define the API endpoints, their [relationships](#define-resource-relationships), how to handle [pagination](#pagination), and [authentication](#authentication).

## Setup guide

### Initialize the verified source

Enter the following command in your terminal:

```sh
dlt init rest_api duckdb
```

[dlt init](../../reference/command-line-interface) will initialize the pipeline examples for REST API as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

Running `dlt init` creates the following in the current folder:
- `rest_api_pipeline.py` file with a sample pipelines definition:
    - GitHub API example
    - Pokemon API example
- `.dlt` folder with:
     - `secrets.toml` file to store your access tokens and other sensitive information
     - `config.toml` file to store the configuration settings
- `requirements.txt` file with the required dependencies

Change the REST API source to your needs by modifying the `rest_api_pipeline.py` file. See the detailed [source configuration](#source-configuration) section below.

:::note
For the rest of the guide, we will use the [GitHub API](https://docs.github.com/en/rest?apiVersion=2022-11-28) and [Pokemon API](https://pokeapi.co/) as example sources.
:::

This source is based on the [RESTClient class](../../general-usage/http/rest-client.md).

### Add credentials

In the `.dlt` folder, you'll find a file called `secrets.toml`, where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

The GitHub API [requires an access token](https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api?apiVersion=2022-11-28) to access some of its endpoints and to increase the rate limit for the API calls. To get a GitHub token, follow the GitHub documentation on [managing your personal access tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).

After you get the token, add it to the `secrets.toml` file:

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

## Source configuration

### Quick example

Let's take a look at the GitHub example in `rest_api_pipeline.py` file:

```py
from rest_api import RESTAPIConfig, rest_api_resources

@dlt.source
def github_source(github_token=dlt.secrets.value):
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
            "auth": {
                "token": github_token,
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

    yield from rest_api_resources(config)

def load_github() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    load_info = pipeline.run(github_source())
    print(load_info)
```

The declarative resource configuration is defined in the `config` dictionary. It contains the following key components:

1. `client`: Defines the base URL and authentication method for the API. In this case it uses token-based authentication. The token is stored in the `secrets.toml` file.

2. `resource_defaults`: Contains default settings for all [resources](#resource-configuration). In this example, we define that all resources:
    - Have `id` as the [primary key](../../general-usage/resource#define-schema)
    - Use the `merge` [write disposition](../../general-usage/incremental-loading#choosing-a-write-disposition) to merge the data with the existing data in the destination.
    - Send a `per_page` query parameter with each request to 100 to get more results per page.

3. `resources`: A list of [resources](#resource-configuration) to be loaded. Here, we have two resources: `issues` and `issue_comments`, which correspond to the GitHub API endpoints for [repository issues](https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues) and [issue comments](https://docs.github.com/en/rest/issues/comments?apiVersion=2022-11-28#list-issue-comments). Note that we need a in issue number to fetch comments for each issue. This number is taken from the `issues` resource. More on this in the [resource relationships](#define-resource-relationships) section.

Let's break down the configuration in more detail.

### Configuration structure

:::tip
Import the `RESTAPIConfig` type from the `rest_api` module to have convenient hints in your editor/IDE and use it to define the configuration object.

```py
from rest_api import RESTAPIConfig
```
:::


The configuration object passed to the REST API Generic Source has three main elements:

```py
config: RESTAPIConfig = {
    "client": {
        ...
    },
    "resource_defaults": {
        ...
    },
    "resources": [
        ...
    ],
}
```

#### `client`

`client` contains the configuration to connect to the API's endpoints. It includes the following fields:

- `base_url` (str): The base URL of the API. This string is prepended to all endpoint paths. For example, if the base URL is `https://api.example.com/v1/`, and the endpoint path is `users`, the full URL will be `https://api.example.com/v1/users`.
- `headers` (dict, optional): Additional headers to be sent with each request.
- `auth` (optional): Authentication configuration. It can be a simple token, a `AuthConfigBase` object, or a more complex authentication method.
- `paginator` (optional): Configuration for the default pagination to be used for resources that support pagination. See the [pagination](#pagination) section for more details.

#### `resource_defaults` (optional)

`resource_defaults` contains the default values to [configure the dlt resources](#resource-configuration). This configuration is applied to all resources unless overridden by the resource-specific configuration.

For example, you can set the primary key, write disposition, and other default settings here:

```py
config = {
    "client": {
        ...
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
        "resource1",
        "resource2": {
            "name": "resource2_name",
            "write_disposition": "append",
            "endpoint": {
                "params": {
                    "param1": "value1",
                },
            },
        },
    ],
}
```

Above, all resources will have `primary_key` set to `id`, `resource1` will have `write_disposition` set to `merge`, and `resource2` will override the default `write_disposition` with `append`.
Both `resource1` and `resource2` will have the `per_page` parameter set to 100.

#### `resources`

This is a list of resource configurations that define the API endpoints to be loaded. Each resource configuration can be:
- a dictionary with the [resource configuration](#resource-configuration).
- a string. In this case, the string is used as the both as the endpoint path and the resource name, and the resource configuration is taken from the `resource_defaults` configuration if it exists.

### Resource configuration

A resource configuration is used to define a [dlt resource](../../general-usage/resource.md) for the data to be loaded from an API endpoint. It contains the following key fields:

- `endpoint`: The endpoint configuration for the resource. It can be a string or a dict representing the endpoint settings. See the [endpoint configuration](#endpoint-configuration) section for more details.
- `write_disposition`: The write disposition for the resource.
- `primary_key`: The primary key for the resource.
- `include_from_parent`: A list of fields from the parent resource to be included in the resource output. See the [resource relationships](#include-fields-from-the-parent-resource) section for more details.
- `selected`: A flag to indicate if the resource is selected for loading. This could be useful when you want to load data only from child resources and not from the parent resource.

You can also pass additional resource parameters that will be used to configure the dlt resource. See [dlt resource API reference](../../api_reference/extract/decorators.md#resource) for more details.

### Endpoint configuration

The endpoint configuration defines how to query the API endpoint. Quick example:

```py
{
    "path": "issues",
    "method": "GET",
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
    "data_selector": "results",
}
```

The fields in the endpoint configuration are:

- `path`: The path to the API endpoint.
- `method`: The HTTP method to be used. Default is `GET`.
- `params`: Query parameters to be sent with each request. For example, `sort` to order the results or `since` to specify [incremental loading](#incremental-loading). This is also used to define [resource relationships](#define-resource-relationships).
- `json`: The JSON payload to be sent with the request (for POST and PUT requests).
- `paginator`: Pagination configuration for the endpoint. See the [pagination](#pagination) section for more details.
- `data_selector`: A JSONPath to select the data from the response. See the [data selection](#data-selection) section for more details.
- `response_actions`: A list of actions that define how to process the response data.
- `incremental`: Configuration for [incremental loading](#incremental-loading).

### Pagination

The REST API source will try to automatically handle pagination for you. This works by detecting the pagination details from the first API response.

In some special cases, you may need to specify the pagination configuration explicitly.

:::note
Currently pagination is supported only for GET requests. To handle POST requests with pagination, you need to implement a [custom paginator](../../general-usage/http/rest-client.md#custom-paginator).
:::

These are the available paginators:

| Paginator class | String Alias (`type`) | Description |
| -------------- | ------------ | ----------- |
| [JSONResponsePaginator](../../general-usage/http/rest-client.md#jsonresponsepaginator) | `json_response` | The links to the next page are in the body (JSON) of the response. |
| [HeaderLinkPaginator](../../general-usage/http/rest-client.md#headerlinkpaginator) | `header_link` | The links to the next page are in the response headers. |
| [OffsetPaginator](../../general-usage/http/rest-client.md#offsetpaginator) | `offset` | The pagination is based on an offset parameter. With total items count either in the response body or explicitly provided. |
| [PageNumberPaginator](../../general-usage/http/rest-client.md#pagenumberpaginator) | `page_number` | The pagination is based on a page number parameter. With total pages count either in the response body or explicitly provided. |
| [JSONCursorPaginator](../../general-usage/http/rest-client.md#jsonresponsecursorpaginator) | `cursor` | The pagination is based on a cursor parameter. The value of the cursor is in the response body (JSON). |
| SinglePagePaginator | `single_page` | The response will be interpreted as a single-page response, ignoring possible pagination metadata. |
| `None` | `auto` | Explicitly specify that the source should automatically detect the pagination method. |

To specify the pagination configuration, use the `paginator` field in the [client](#client) or [endpoint](#endpoint-configuration) configurations. You may either use a dictionary with a string alias in the `type` field along with the required parameters, or use the paginator instance directly:

```py
{
    ...
    "paginator": {
        "type": "json_links",
        "next_url_path": "paging.next",
    }
}
```

Or using the paginator instance:

```py
{
    ...
    "paginator": JSONResponsePaginator(
        next_url_path="paging.next"
    ),
}
```

This is useful when you're [implementing and using a custom paginator](../../general-usage/http/rest-client.md#custom-paginator).

### Data selection

The `data_selector` field in the endpoint configuration allows you to specify a JSONPath to select the data from the response. By default, the source will try to detect locations of the data automatically.

Use this field when you need to specify the location of the data in the response explicitly.

For example, if the API response looks like this:

```json
{
    "posts": [
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"}
    ]
}
```

You can use the following endpoint configuration:

```py
{
    "path": "posts",
    "data_selector": "posts",
}
```

For a nested structure like this:

```json
{
    "results": {
        "posts": [
            {"id": 1, "title": "Post 1"},
            {"id": 2, "title": "Post 2"},
            {"id": 3, "title": "Post 3"}
        ]
    }
}
```

You can use the following endpoint configuration:

```py
{
    "path": "posts",
    "data_selector": "results.posts",
}
```

Read more about [JSONPath syntax](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) to learn how to write selectors.


### Authentication

Many APIs require authentication to access their endpoints. The REST API source supports various authentication methods, such as token-based, query parameters, basic auth, etc.

#### Quick example

One of the most common method is token-based authentication. To authenticate with a token, you can use the `token` field in the `auth` configuration:

```py
{
    "client": {
        ...
        "auth": {
            "token": dlt.secrets["your_api_token"],
        },
        ...
    },
}
```

:::warning
Make sure to store your access tokens and other sensitive information in the `secrets.toml` file and never commit it to the version control system.
:::

Available authentication types:

| Authentication class | String Alias (`type`) | Description |
| ------------------- | ----------- | ----------- |
| [BearTokenAuth](../../general-usage/http/rest-client.md#bearer-token-authentication) | `bearer` | Bearer token authentication. |
| [HTTPBasicAuth](../../general-usage/http/rest-client.md#http-basic-authentication) | `api_key` | Basic HTTP authentication. |
| [APIKeyAuth](../../general-usage/http/rest-client.md#api-key-authentication) | `http_basic` | API key authentication with key defined in the query parameters or in the headers. |

To specify the authentication configuration, use the `auth` field in the [client](#client) configuration:

```py
{
    "client": {
        "auth": {
            "type": "bearer",
            "token": dlt.secrets["your_api_token"],
        },
        ...
    },
}
```

Alternatively, you can use the authentication class directly:

```py
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

config = {
    "client": {
        "auth": BearTokenAuth(dlt.secrets["your_api_token"]),
    },
    ...
}
```

### Define resource relationships

When you have a resource that depends on another resource, you can define the relationship using the `resolve` configuration. With it you link a path parameter in the child resource to a field in the parent resource's data.

In the GitHub example, the `issue_comments` resource depends on the `issues` resource. The `issue_number` parameter in the `issue_comments` endpoint configuration is resolved from the `number` field of the `issues` resource:

```py
{
    "resources": [
        {
            "name": "issues",
            "endpoint": {
                "path": "issues",
                ...
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
```

This configuration tells the source to get issue numbers from the `issues` resource and use them to fetch comments for each issue. So if the `issues` resource yields the following data:

```json
[
    {"id": 1, "number": 123},
    {"id": 2, "number": 124},
    {"id": 3, "number": 125}
]
```

The `issue_comments` resource will make requests to the following endpoints:

- `issues/123/comments`
- `issues/124/comments`
- `issues/125/comments`

The syntax for the `resolve` field in parameter configuration is:

```py
"<parameter_name>": {
    "type": "resolve",
    "resource": "<parent_resource_name>",
    "field": "<parent_resource_field_name>",
}
```

Under the hood, dlt handles this by using a [transformer resource](../../general-usage/resource.md#process-resources-with-dlttransformer).

#### Include fields from the parent resource

You can include data from the parent resource in the child resource by using the `include_from_parent` field in the resource configuration. For example:

```py
{
    "name": "issue_comments",
    "endpoint": {
        ...
    },
    "include_from_parent": ["id", "title", "created_at"],
}
```

This will include the `id`, `title`, and `created_at` fields from the `issues` resource in the `issue_comments` resource data. The name of the included fields will be prefixed with the parent resource name and an underscore (`_`) like so: `_issues_id`, `_issues_title`, `_issues_created_at`.

## Incremental loading

Some APIs provide a way to fetch only new or changed data (most often by using a timestamp field like `updated_at`, `created_at`, or incremental IDs).
This is called [incremental loading](../../general-usage/incremental-loading.md) and is very useful as it allows you to reduce the load time and the amount of data transferred.

When the API endpoint supports incremental loading, you can configure the source to load only the new or changed data using these two methods:

1. Defining a special parameter in the `params` section of the [endpoint configuration](#endpoint-configuration):

    ```py
    "<parameter_name>": {
        "type": "incremental",
        "cursor_path": "<path_to_cursor_field>",
        "initial_value": "<initial_value>",
    },
    ```

    For example, in the `issues` resource configuration in the GitHub example, we have:

    ```py
    "since": {
        "type": "incremental",
        "cursor_path": "updated_at",
        "initial_value": "2024-01-25T11:21:28Z",
    },
    ```

    This configuration tells the source to create an incremental object that will keep track of the `updated_at` field in the response and use it as a value for the `since` parameter in subsequent requests.

2. Specifying the `incremental` field in the [endpoint configuration](#endpoint-configuration):

    ```py
    "incremental": {
        "start_param": "<parameter_name>",
        "end_param": "<parameter_name>",
        "cursor_path": "<path_to_cursor_field>",
        "initial_value": "<initial_value>",
        "end_value": "<end_value>",
    },
    ```

    This configuration is more flexible and allows you to specify the start and end conditions for the incremental loading.

See the [incremental loading](../../general-usage/incremental-loading.md#incremental-loading-with-a-cursor-field) guide for more details.

## Advanced configuration

`rest_api_source()` function creates the [dlt source](../../general-usage/source.md) and lets you configure the following parameters:

- `config`: The REST API configuration dictionary.
- `name`: An optional name for the source.
- `section`: An optional section name in the configuration file.
- `max_table_nesting`: Sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.
- `root_key` (bool): Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge. Defaults to False.
- `schema_contract`: Schema contract settings that will be applied to this resource.
- `spec`: A specification of configuration and secret values required by the source.

## Troubleshooting

If you encounter any issues while setting up or running the pipeline, check the following:

- Make sure you have the correct access token and other credentials in the `secrets.toml` file.
- Check the API documentation for the correct endpoint paths and query parameters.
- Verify the configuration settings in the `rest_api_pipeline.py` file.
- Enable [logging](../../running-in-production/running.md#set-the-log-level-and-format) to see detailed information about the HTTP requests:

```bash
RUNTIME__LOG_LEVEL=INFO python my_script.py
```

`rest_api` source uses the [RESTClient](../../general-usage/http/rest-client.md) class to make HTTP requests. You can refer to the RESTClient [troubleshooting guide](../../general-usage/http/rest-client.md#troubleshooting) for guidance on debugging HTTP requests.
