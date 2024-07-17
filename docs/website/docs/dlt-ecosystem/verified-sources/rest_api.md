---
title: REST API generic source
description: dlt verified source for REST APIs
keywords: [rest api, restful api]
---
import Header from './_source-info-header.md';

<Header/>

This is a generic dlt source you can use to extract data from any REST API. It uses [declarative configuration](#source-configuration) to define the API endpoints, their [relationships](#define-resource-relationships), how to handle [pagination](#pagination), and [authentication](#authentication).

### Quick example

Here's an example of how to configure the REST API source to load posts and related comments from a hypothetical blog API:

```py
import dlt
from rest_api import rest_api_source

source = rest_api_source({
    "client": {
        "base_url": "https://api.example.com/",
        "auth": {
            "token": dlt.secrets["your_api_token"],
        },
        "paginator": {
            "type": "json_response",
            "next_url_path": "paging.next",
        },
    },
    "resources": [
        # "posts" will be used as the endpoint path, the resource name,
        # and the table name in the destination. The HTTP client will send
        # a request to "https://api.example.com/posts".
        "posts",

        # The explicit configuration allows you to link resources
        # and define parameters.
        {
            "name": "comments",
            "endpoint": {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {
                        "type": "resolve",
                        "resource": "posts",
                        "field": "id",
                    },
                    "sort": "created_at",
                },
            },
        },
    ],
})

pipeline = dlt.pipeline(
    pipeline_name="rest_api_example",
    destination="duckdb",
    dataset_name="rest_api_data",
)

load_info = pipeline.run(source)
```

Running this pipeline will create two tables in the DuckDB: `posts` and `comments` with the data from the respective API endpoints. The `comments` resource will fetch comments for each post by using the `id` field from the `posts` resource.

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

The `client` configuration is used to connect to the API's endpoints. It includes the following fields:

- `base_url` (str): The base URL of the API. This string is prepended to all endpoint paths. For example, if the base URL is `https://api.example.com/v1/`, and the endpoint path is `users`, the full URL will be `https://api.example.com/v1/users`.
- `headers` (dict, optional): Additional headers that are sent with each request.
- `auth` (optional): Authentication configuration. This can be a simple token, an `AuthConfigBase` object, or a more complex authentication method.
- `paginator` (optional): Configuration for the default pagination used for resources that support pagination. Refer to the [pagination](#pagination) section for more details.

#### `resource_defaults` (optional)

`resource_defaults` contains the default values to [configure the dlt resources](#resource-configuration). This configuration is applied to all resources unless overridden by the resource-specific configuration.

For example, you can set the primary key, write disposition, and other default settings here:

```py
config = {
    "client": {
        # ...
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
        {
            "resource2": {
                "name": "resource2_name",
                "write_disposition": "append",
                "endpoint": {
                    "params": {
                        "param1": "value1",
                    },
                },
            }
        }
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
- `response_actions`: A list of actions that define how to process the response data. See the [response actions](#response-actions) section for more details.
- `incremental`: Configuration for [incremental loading](#incremental-loading).

### Pagination

The REST API source will try to automatically handle pagination for you. This works by detecting the pagination details from the first API response.

In some special cases, you may need to specify the pagination configuration explicitly.

To specify the pagination configuration, use the `paginator` field in the [client](#client) or [endpoint](#endpoint-configuration) configurations. You may either use a dictionary with a string alias in the `type` field along with the required parameters, or use a [paginator class instance](../../general-usage/http/rest-client.md#paginators).

#### Example

Suppose the API response for `https://api.example.com/posts` contains a `next` field with the URL to the next page:

```json
{
    "data": [
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"}
    ],
    "pagination": {
        "next": "https://api.example.com/posts?page=2"
    }
}
```

You can configure the pagination for the `posts` resource like this:

```py
{
    "path": "posts",
    "paginator": {
        "type": "json_response",
        "next_url_path": "pagination.next",
    }
}
```

Alternatively, you can use the paginator instance directly:

```py
from dlt.sources.helpers.rest_client.paginators import JSONResponsePaginator

# ...

{
    "path": "posts",
    "paginator": JSONResponsePaginator(
        next_url_path="pagination.next"
    ),
}
```

:::note
Currently pagination is supported only for GET requests. To handle POST requests with pagination, you need to implement a [custom paginator](../../general-usage/http/rest-client.md#custom-paginator).
:::

These are the available paginators:

| `type` | Paginator class | Description |
| ------------ | -------------- | ----------- |
| `json_response` | [JSONResponsePaginator](../../general-usage/http/rest-client.md#jsonresponsepaginator) | The link to the next page is in the body (JSON) of the response.<br/>*Parameters:*<ul><li>`next_url_path` (str) - the JSONPath to the next page URL</li></ul> |
| `header_link` | [HeaderLinkPaginator](../../general-usage/http/rest-client.md#headerlinkpaginator) | The links to the next page are in the response headers.<br/>*Parameters:*<ul><li>`link_header` (str) - the name of the header containing the links. Default is "next".</li></ul> |
| `offset` | [OffsetPaginator](../../general-usage/http/rest-client.md#offsetpaginator) | The pagination is based on an offset parameter. With total items count either in the response body or explicitly provided.<br/>*Parameters:*<ul><li>`limit` (int) - the maximum number of items to retrieve in each request</li><li>`offset` (int) - the initial offset for the first request. Defaults to `0`</li><li>`offset_param` (str) - the name of the query parameter used to specify the offset. Defaults to "offset"</li><li>`limit_param` (str) - the name of the query parameter used to specify the limit. Defaults to "limit"</li><li>`total_path` (str) - a JSONPath expression for the total number of items. If not provided, pagination is controlled by `maximum_offset`</li><li>`maximum_offset` (int) - optional maximum offset value. Limits pagination even without total count</li></ul> |
| `page_number` | [PageNumberPaginator](../../general-usage/http/rest-client.md#pagenumberpaginator) | The pagination is based on a page number parameter. With total pages count either in the response body or explicitly provided.<br/>*Parameters:*<ul><li>`initial_page` (int) - the starting page number. Defaults to `0`</li><li>`page_param` (str) - the query parameter name for the page number. Defaults to "page"</li><li>`total_path` (str) - a JSONPath expression for the total number of pages. If not provided, pagination is controlled by `maximum_page`</li><li>`maximum_page` (int) - optional maximum page number. Stops pagination once this page is reached</li></ul> |
| `cursor` | [JSONResponseCursorPaginator](../../general-usage/http/rest-client.md#jsonresponsecursorpaginator) | The pagination is based on a cursor parameter. The value of the cursor is in the response body (JSON).<br/>*Parameters:*<ul><li>`cursor_path` (str) - the JSONPath to the cursor value. Defaults to "cursors.next"</li><li>`cursor_param` (str) - the query parameter name for the cursor. Defaults to "after"</li></ul> |
| `single_page` | SinglePagePaginator | The response will be interpreted as a single-page response, ignoring possible pagination metadata. |
| `auto` | `None` | Explicitly specify that the source should automatically detect the pagination method. |

For more complex pagination methods, you can implement a [custom paginator](../../general-usage/http/rest-client.md#implementing-a-custom-paginator), instantiate it, and use it in the configuration.

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

For APIs that require authentication to access their endpoints, the REST API source supports various authentication methods, including token-based authentication, query parameters, basic authentication, and custom authentication. The authentication configuration is specified in the `auth` field of the [client](#client) either as a dictionary or as an instance of the [authentication class](../../general-usage/http/rest-client.md#authentication).

#### Quick example

One of the most common methods is token-based authentication (also known as Bearer token authentication). To authenticate using this method, you can use the following shortcut:

```py
{
    "client": {
        # ...
        "auth": {
            "token": dlt.secrets["your_api_token"],
        },
        # ...
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
| [HTTPBasicAuth](../../general-usage/http/rest-client.md#http-basic-authentication) | `http_basic` | Basic HTTP authentication. |
| [APIKeyAuth](../../general-usage/http/rest-client.md#api-key-authentication) | `api_key` | API key authentication with key defined in the query parameters or in the headers. |
| [OAuth2ClientCredentials](../../general-usage/http/rest-client.md#oauth20-authorization) | N/A | OAuth 2.0 authorization with a temporary access token obtained from the authorization server. |

To specify the authentication configuration, use the `auth` field in the [client](#client) configuration:

```py
{
    "client": {
        # ...
        "auth": {
            "type": "bearer",
            "token": dlt.secrets["your_api_token"],
        },
        # ...
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
    # ...
}
```

:::warning
Make sure to store your access tokens and other sensitive information in the `secrets.toml` file and never commit it to the version control system.
:::

Available authentication types:

| `type` | Authentication class | Description |
| ----------- | ------------------- | ----------- |
| `bearer` | [BearTokenAuth](../../general-usage/http/rest-client.md#bearer-token-authentication) | Bearer token authentication.<br/>Parameters:<ul><li>`token` (str)</li></ul> |
| `http_basic` | [HTTPBasicAuth](../../general-usage/http/rest-client.md#http-basic-authentication) | Basic HTTP authentication.<br/>Parameters:<ul><li>`username` (str)</li><li>`password` (str)</li></ul> |
| `api_key` | [APIKeyAuth](../../general-usage/http/rest-client.md#api-key-authentication) | API key authentication with key defined in the query parameters or in the headers. <br/>Parameters:<ul><li>`name` (str) - the name of the query parameter or header</li><li>`api_key` (str) - the API key value</li><li>`location` (str, optional) - the location of the API key in the request. Can be `query` or `header`. Default is `header`</li></ul> |


For more complex authentication methods, you can implement a [custom authentication class](../../general-usage/http/rest-client.md#implementing-custom-authentication) and use it in the configuration.



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
                # ...
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
{
    "<parameter_name>": {
        "type": "resolve",
        "resource": "<parent_resource_name>",
        "field": "<parent_resource_field_name_or_jsonpath>",
    }
}
```

The `field` value can be specified as a [JSONPath](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) to select a nested field in the parent resource data. For example: `"field": "items[0].id"`.

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

When the API endpoint supports incremental loading, you can configure dlt to load only the new or changed data using these two methods:

1. Defining a special parameter in the `params` section of the [endpoint configuration](#endpoint-configuration).
2. Specifying the `incremental` field in the endpoint configuration.

Let's start with the first method.

### Incremental loading in `params`

Imagine we have the following endpoint `https://api.example.com/posts` and it:
1. Accepts a `created_since` query parameter to fetch posts created after a certain date.
2. Returns a list of posts with the `created_at` field for each post.

For example, if we query the endpoint with `https://api.example.com/posts?created_since=2024-01-25`, we get the following response:

```json
{
    "results": [
        {"id": 1, "title": "Post 1", "created_at": "2024-01-26"},
        {"id": 2, "title": "Post 2", "created_at": "2024-01-27"},
        {"id": 3, "title": "Post 3", "created_at": "2024-01-28"}
    ]
}
```

To enable the incremental loading for this endpoint, you can use the following endpoint configuration:

```py
{
    "path": "posts",
    "data_selector": "results",  # Optional JSONPath to select the list of posts
    "params": {
        "created_since": {
            "type": "incremental",
            "cursor_path": "created_at", # The JSONPath to the field we want to track in each post
            "initial_value": "2024-01-25",
        },
    },
}
```

After you run the pipeline, dlt will keep track of the last `created_at` from all the posts fetched and use it as the `created_since` parameter in the next request.
So in our case, the next request will be made to `https://api.example.com/posts?created_since=2024-01-28` to fetch only the new posts created after `2024-01-28`.

Let's break down the configuration.

1. We explicitly set `data_selector` to `"results"` to select the list of posts from the response. This is optional, if not set, dlt will try to auto-detect the data location.
2. We define the `created_since` parameter as an incremental parameter with the following fields:

```py
{
    "created_since": {
        "type": "incremental",
        "cursor_path": "created_at",
        "initial_value": "2024-01-25",
    },
}
```

- `type`: The type of the parameter definition. In this case, it must be set to `incremental`.
- `cursor_path`: The JSONPath to the field within each item in the list. The value of this field will be used in the next request. In the example above our items look like `{"id": 1, "title": "Post 1", "created_at": "2024-01-26"}` so to track the created time we set `cursor_path` to `"created_at"`. Note that the JSONPath starts from the root of the item (dict) and not from the root of the response.
- `initial_value`: The initial value for the cursor. This is the value that will initialize the state of incremental loading. In this case, it's `2024-01-25`. The value type should match the type of the field in the data item.

### Incremental loading using the `incremental` field

The alternative method is to use the `incremental` field in the [endpoint configuration](#endpoint-configuration). This method is more flexible and allows you to specify the start and end conditions for the incremental loading.

Let's take the same example as above and configure it using the `incremental` field:

```py
{
    "path": "posts",
    "data_selector": "results",
    "incremental": {
        "start_param": "created_since",
        "cursor_path": "created_at",
        "initial_value": "2024-01-25",
    },
}
```

Note that we specify the query parameter name `created_since` in the `start_param` field and not in the `params` section.

The full available configuration for the `incremental` field is:

```py
{
    "incremental": {
        "start_param": "<start_parameter_name>",
        "end_param": "<end_parameter_name>",
        "cursor_path": "<path_to_cursor_field>",
        "initial_value": "<initial_value>",
        "end_value": "<end_value>",
    }
}
```

The fields are:

- `start_param` (str): The name of the query parameter to be used as the start condition. If we use the example above, it would be `"created_since"`.
- `end_param` (str): The name of the query parameter to be used as the end condition. This is optional and can be omitted if you only need to track the start condition. This is useful when you need to fetch data within a specific range and the API supports end conditions (like `created_before` query parameter).
- `cursor_path` (str): The JSONPath to the field within each item in the list. This is the field that will be used to track the incremental loading. In the example above, it's `"created_at"`.
- `initial_value` (str): The initial value for the cursor. This is the value that will initialize the state of incremental loading.
- `end_value` (str): The end value for the cursor to stop the incremental loading. This is optional and can be omitted if you only need to track the start condition. If you set this field, `initial_value` needs to be set as well.

See the [incremental loading](../../general-usage/incremental-loading.md#incremental-loading-with-a-cursor-field) guide for more details.

If you encounter issues with incremental loading, see the [troubleshooting section](../../general-usage/incremental-loading.md#troubleshooting) in the incremental loading guide.

## Advanced configuration

`rest_api_source()` function creates the [dlt source](../../general-usage/source.md) and lets you configure the following parameters:

- `config`: The REST API configuration dictionary.
- `name`: An optional name for the source.
- `section`: An optional section name in the configuration file.
- `max_table_nesting`: Sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.
- `root_key` (bool): Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge. Defaults to False.
- `schema_contract`: Schema contract settings that will be applied to this resource.
- `spec`: A specification of configuration and secret values required by the source.

### Response actions

The `response_actions` field in the endpoint configuration allows you to specify how to handle specific responses or all responses from the API. For example, responses with specific status codes or content substrings can be ignored.
Additionally, all responses or only responses with specific status codes or content substrings can be transformed with a custom callable, such as a function. This callable is passed on to the requests library as a [response hook](https://requests.readthedocs.io/en/latest/user/advanced/#event-hooks). The callable can modify the response object and has to return it for the modifications to take effect.

:::caution Experimental Feature
This is an experimental feature and may change in future releases.
:::

**Fields:**

- `status_code` (int, optional): The HTTP status code to match.
- `content` (str, optional): A substring to search for in the response content.
- `action` (str or Callable or List[Callable], optional): The action to take when the condition is met. Currently supported actions:
  - `"ignore"`: Ignore the response.
  - a callable accepting and returning the response object.
  - a list of callables, each accepting and returning the response object.


#### Example A

```py
{
    "path": "issues",
    "response_actions": [
        {"status_code": 404, "action": "ignore"},
        {"content": "Not found", "action": "ignore"},
        {"status_code": 200, "content": "some text", "action": "ignore"},
    ],
}
```

In this example, the source will ignore responses with a status code of 404, responses with the content "Not found", and responses with a status code of 200 _and_ content "some text".

#### Example B

```py
def set_encoding(response, *args, **kwargs):
    # sets the encoding in case it's not correctly detected
    response.encoding = 'windows-1252'
    return response

def add_and_remove_fields(response: Response, *args, **kwargs) -> Response:
    payload = response.json()
    for record in payload["data"]:
        record["custom_field"] = "foobar"
        record.pop("email", None)
    modified_content: bytes = json.dumps(payload).encode("utf-8")
    response._content = modified_content
    return response

{
    "path": "issues",
    "response_actions": [
        set_encoding
        {"status_code": 200, "action": add_and_remove_fields},
    ],
}
```

In this example, the resource will set the correct encoding for all responses first. Thereafter, for all responses that have the status code 200, we will add a field `custom_field` and remove the field `email`.

#### Example C

```py
def set_encoding(response, *args, **kwargs):
    # sets the encoding in case it's not correctly detected
    response.encoding = 'windows-1252'
    return response

{
    "path": "issues",
    "response_actions": [
        set_encoding,
    ],
}
```

In this example, the resource will set the correct encoding for all responses. More callables can be added to the list of response_actions.


## Troubleshooting

If you encounter issues while running the pipeline, enable [logging](../../running-in-production/running.md#set-the-log-level-and-format) for detailed information about the execution:

```sh
RUNTIME__LOG_LEVEL=INFO python my_script.py
```

This also provides details on the HTTP requests.

### Configuration issues

#### Getting validation errors

When you running the pipeline and getting a `DictValidationException`, it means that the [source configuration](#source-configuration) is incorrect. The error message provides details on the issue including the path to the field and the expected type.

For example, if you have a source configuration like this:

```py
config: RESTAPIConfig = {
    "client": {
        # ...
    },
    "resources": [
        {
            "name": "issues",
            "params": {             # <- Wrong: this should be inside
                "sort": "updated",  #    the endpoint field below
            },
            "endpoint": {
                "path": "issues",
                # "params": {       # <- Correct configuration
                #     "sort": "updated",
                # },
            },
        },
        # ...
    ],
}
```

You will get an error like this:

```sh
dlt.common.exceptions.DictValidationException: In path .: field 'resources[0]'
expects the following types: str, EndpointResource. Provided value {'name': 'issues', 'params': {'sort': 'updated'},
'endpoint': {'path': 'issues', ... }} with type 'dict' is invalid with the following errors:
For EndpointResource: In path ./resources[0]: following fields are unexpected {'params'}
```

It means that in the first resource configuration (`resources[0]`), the `params` field should be inside the `endpoint` field.

:::tip
Import the `RESTAPIConfig` type from the `rest_api` module to have convenient hints in your editor/IDE and use it to define the configuration object.

```py
from rest_api import RESTAPIConfig
```
:::

#### Getting wrong data or no data

If incorrect data is received from an endpoint, check the `data_selector` field in the [endpoint configuration](#endpoint-configuration). Ensure the JSONPath is accurate and points to the correct data in the response body. `rest_api` attempts to auto-detect the data location, which may not always succeed. See the [data selection](#data-selection) section for more details.

#### Getting insufficient data or incorrect pagination

Check the `paginator` field in the configuration. When not explicitly specified, the source tries to auto-detect the pagination method. If auto-detection fails, or the system is unsure, a warning is logged. For production environments, we recommend to specify an explicit paginator in the configuration. See the [pagination](#pagination) section for more details. Some APIs may have non-standard pagination methods, and you may need to implement a [custom paginator](../../general-usage/http/rest-client.md#implementing-a-custom-paginator).

#### Incremental loading not working

See the [troubleshooting guide](../../general-usage/incremental-loading.md#troubleshooting) for incremental loading issues.

#### Getting HTTP 404 errors

Some API may return 404 errors for resources that do not exist or have no data. Manage these responses by configuring the `ignore` action in [response actions](#response-actions).

### Authentication issues

If experiencing 401 (Unauthorized) errors, this could indicate:

- Incorrect authorization credentials. Verify credentials in the `secrets.toml`. Refer to [Secret and configs](../../general-usage/credentials/configuration#understanding-the-exceptions) for more information.
- An incorrect authentication type. Consult the API documentation for the proper method. See the [authentication](#authentication) section for details. For some APIs, a [custom authentication method](../../general-usage/http/rest-client.md#custom-authentication) may be required.

### General guidelines

The `rest_api` source uses the [RESTClient](../../general-usage/http/rest-client.md) class for HTTP requests. Refer to the RESTClient [troubleshooting guide](../../general-usage/http/rest-client.md#troubleshooting) for debugging tips.

For further assistance, join our [Slack community](https://dlthub.com/community). We're here to help!
