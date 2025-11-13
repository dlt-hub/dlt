---
title: REST API source
description: Learn how to set up and configure
keywords: [rest api, restful api]
---
import Header from '../_source-info-header.md';

<Header/>

This is a dlt source you can use to extract data from any REST API. It uses [declarative configuration](#source-configuration) to define the API endpoints, their [relationships](#define-resource-relationships), how to handle [pagination](#pagination), and [authentication](#authentication).

### Quick example

Here's an example of how to configure the REST API source to load posts and related comments from a hypothetical blog API:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source({
    "client": {
        "base_url": "https://api.example.com/",
        "auth": {
            "token": dlt.secrets["your_api_token"],
        },
        "paginator": {
            "type": "json_link",
            "next_url_path": "paging.next",
        },
    },
    "resources": [
        # "posts" will be used as the endpoint path, the resource name,
        # and the table name in the destination. The HTTP client will send
        # a request to "https://api.example.com/posts".
        "posts",

        # The explicit configuration allows you to link resources
        # and define query string parameters.
        {
            "name": "comments",
            "endpoint": {
                "path": "posts/{resources.posts.id}/comments",
                "params": {
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

Running this pipeline will create two tables in DuckDB: `posts` and `comments` with the data from the respective API endpoints. The `comments` resource will fetch comments for each post by using the `id` field from the `posts` resource.

## Setup

### Prerequisites

Please make sure the `dlt` library is installed. Refer to the [installation guide](../../../intro).

### Initialize the REST API source

Enter the following command in your terminal:

```sh
dlt init rest_api duckdb
```

[dlt init](../../../reference/command-line-interface) will initialize the pipeline examples for REST API as the [source](../../../general-usage/source) and [duckdb](../../destinations/duckdb.md) as the [destination](../../destinations).

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

This source is based on the [RESTClient class](../../../general-usage/http/rest-client.md).

### Add credentials

In the `.dlt` folder, you'll find a file called `secrets.toml`, where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

The GitHub API [requires an access token](https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api?apiVersion=2022-11-28) to access some of its endpoints and to increase the rate limit for the API calls. To get a GitHub token, follow the GitHub documentation on [managing your personal access tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).

After you get the token, add it to the `secrets.toml` file:

```toml
[sources.rest_api_pipeline.github]
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

Let's take a look at the GitHub example in the `rest_api_pipeline.py` file:

```py
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

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
                    "path": "issues/{resources.issues.number}/comments",
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

1. `client`: Defines the base URL and authentication method for the API. In this case, it uses token-based authentication. The token is stored in the `secrets.toml` file.

2. `resource_defaults`: Contains default settings for all [resources](#resource-configuration). In this example, we define that all resources:
    - Have `id` as the [primary key](../../../general-usage/resource#define-schema)
    - Use the `merge` [write disposition](../../../general-usage/incremental-loading.md#choosing-a-write-disposition) to merge the data with the existing data in the destination.
    - Send a `per_page=100` query parameter with each request to get more results per page.

3. `resources`: A list of [resources](#resource-configuration) to be loaded. Here, we have two resources: `issues` and `issue_comments`, which correspond to the GitHub API endpoints for [repository issues](https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues) and [issue comments](https://docs.github.com/en/rest/issues/comments?apiVersion=2022-11-28#list-issue-comments). Note that we need an issue number to fetch comments for each issue. This number is taken from the `issues` resource. More on this in the [resource relationships](#define-resource-relationships) section.

Let's break down the configuration in more detail.

### Configuration structure

:::tip
Import the `RESTAPIConfig` type from the `rest_api` module to have convenient hints in your editor/IDE and use it to define the configuration object.

```py
from dlt.sources.rest_api import RESTAPIConfig
```
:::

The configuration object passed to the REST API Generic Source has three main elements:

```py
config: RESTAPIConfig = {
    "client": {
        # ...
    },
    "resource_defaults": {
        # ...
    },
    "resources": [
        # ...
    ],
}
```

#### `client`

The `client` configuration is used to connect to the API's endpoints. It includes the following fields:

- `base_url` (str): The base URL of the API. This string is prepended to all endpoint paths. For example, if the base URL is `https://api.example.com/v1/`, and the endpoint path is `users`, the full URL will be `https://api.example.com/v1/users`.
- `headers` (dict, optional): Additional headers that are sent with each request. See the [headers configuration](./advanced#headers-configuration) section for more details.
- `auth` (optional): Authentication configuration. This can be a simple token, an `AuthConfigBase` object, or a more complex authentication method.
- `session` (requests.Session, optional): A custom session object. When provided, this session will be used for all HTTP requests instead of the default session. Can be used, for example, with [requests-oauthlib](https://github.com/requests/requests-oauthlib) for OAuth authentication.
- `paginator` (optional): Configuration for the default pagination used for resources that support pagination. Refer to the [pagination](#pagination) section for more details.
- `session` (optional): Custom `requests` session to setup custom [timeouts and retry strategies.](advanced.md#setup-timeouts-and-retry-strategies)

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
            "name": "resource2_name",
            "write_disposition": "append",
            "endpoint": {
                "params": {
                    "param1": "value1",
                },
            },
        }
    ],
}
```

Above, all resources will have `primary_key` set to `id`, `resource1` will have `write_disposition` set to `merge`, and `resource2` will override the default `write_disposition` with `append`.
Both `resource1` and `resource2` will have the `per_page` parameter set to 100.

#### `resources`

This is a list of resource configurations that define the API endpoints to be loaded. Each resource configuration can be:
- a dictionary with the [resource configuration](#resource-configuration).
- a string. In this case, the string is used as both the endpoint path and the resource name, and the resource configuration is taken from the `resource_defaults` configuration if it exists.

### Resource configuration

A resource configuration is used to define a [dlt resource](../../../general-usage/resource.md) for the data to be loaded from an API endpoint. When defining the resource you may specify:
- dlt resource parameters, for for example:
    - `name`: The name of the resource. This is also used as the table name in the destination unless overridden by the `table_name` parameter.
    - `write_disposition`: The write disposition for the resource.
    - `primary_key`: The primary key for the resource.
    - `table_name`: Override the table name for this resource.
    - `max_table_nesting`: Sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON.
    - `selected`: A flag to indicate if the resource is selected for loading. This could be useful when you want to load data only from child resources and not from the parent resource.

    see [dlt resource API reference](../../../api_reference/dlt/extract/decorators#resource) for more details.

- `rest_api` specific parameters, such as:
    - `endpoint`: The endpoint configuration for the resource. It can be a string or a dict representing the endpoint settings. See the [endpoint configuration](#endpoint-configuration) section for more details.
    - `include_from_parent`: A list of fields from the parent resource to be included in the resource output. See the [resource relationships](#include-fields-from-the-parent-resource) section for more details.
    - `processing_steps`: A list of [processing steps](#processing-steps-filter-and-transform-data) to filter and transform your data.
    - `auth`: An optional `AuthConfig` instance. If passed, is used over the one defined in the [client](#client) definition.

Example:
```py
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth

config = {
    "client": {
        "auth": {
            "type": "bearer",
            "token": dlt.secrets["your_api_token"],
        }
    },
    "resources": [
        "resource-using-bearer-auth",
        {
            "name": "my-resource-with-special-auth",
            "write_disposition": "merge",
            "table_name": "my_custom_table",
            "endpoint": {
                # ...
                "auth": HttpBasicAuth("user", dlt.secrets["your_basic_auth_password"])
            },
            # ...
        }
    ]
    # ...
}
```
This would use `Bearer` auth as defined in the `client` for `resource-using-bearer-auth` and `Http Basic` auth for `my-resource-with-special-auth`.

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

- `path`: The path to the API endpoint. By default this path is appended to the given `base_url`. If this is a fully qualified URL starting with `http:` or `https:` it will be
used as-is and `base_url` will be ignored.
- `method`: The HTTP method to be used. The default is `GET`.
- `headers`: Additional headers specific to this endpoint. See the [headers configuration](./advanced#headers-configuration) section for more details.
- `params`: Query parameters to be sent with each request. For example, `sort` to order the results or `since` to specify [incremental loading](#incremental-loading). This is also may be used to define [resource relationships](#define-resource-relationships).
- `json`: The JSON payload to be sent with the request (for POST and PUT requests).
- `data`: The data payload to be sent with the request body. Can be a dictionary (form-encoded) or string. Mutually exclusive with `json` parameter. Use this for APIs that expect form-encoded data or raw payloads instead of JSON.
- `paginator`: Pagination configuration for the endpoint. See the [pagination](#pagination) section for more details.
- `data_selector`: A JSONPath to select the data from the response. See the [data selection](#data-selection) section for more details.
- `response_actions`: A list of actions that define how to process the response data. See the [response actions](./advanced#response-actions) section for more details.
- `incremental`: Configuration for [incremental loading](#incremental-loading).

### Pagination

The REST API source will try to automatically handle pagination for you. This works by detecting the pagination details from the first API response.

In some special cases, you may need to specify the pagination configuration explicitly.

To specify the pagination configuration, use the `paginator` field in the [client](#client), [resource_defaults](#resource_defaults-optional), or [endpoint](#endpoint-configuration) configurations (see [pagination configuration hierarchy](#pagination-configuration-hierarchy)). You may either use a dictionary with a string alias in the `type` field along with the required parameters, or use a [paginator class instance](../../../general-usage/http/rest-client.md#paginators).

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
        "type": "json_link",
        "next_url_path": "pagination.next",
    }
}
```

Alternatively, you can use the paginator instance directly:

```py
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

# ...

{
    "path": "posts",
    "paginator": JSONLinkPaginator(
        next_url_path="pagination.next"
    ),
}
```

:::tip JSONPath escaping for special characters
When working with APIs that use field names containing special characters (like dots, @ symbols, or other reserved JSONPath characters), you need to escape the field names using bracket notation.

For example, Microsoft Graph API uses `@odata.nextLink` for pagination. To access this field, use bracket notation with quotes:

```py
{
    "path": "users",
    "paginator": {
        "type": "json_link",
        "next_url_path": "['@odata.nextLink']",  # Escaped using bracket notation
    }
}
```

Refer to the [JSONPath syntax](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) for more details.

:::

:::note
Currently, pagination is supported only for GET requests for all paginators except [`JSONResponseCursorPaginator`](../../../general-usage/http/rest-client.md#jsonresponsecursorpaginator). To handle POST requests with pagination, you need to implement a [custom paginator](../../../general-usage/http/rest-client.md#implementing-a-custom-paginator).
:::

These are the available paginators:

| `type` | Paginator class | Description |
| ------------ | -------------- | ----------- |
| `json_link` | [JSONLinkPaginator](../../../general-usage/http/rest-client.md#jsonlinkpaginator) | The link to the next page is in the body (JSON) of the response.<br/>*Parameters:*<ul><li>`next_url_path` (str) - the JSONPath to the next page URL</li></ul> |
| `header_link` | [HeaderLinkPaginator](../../../general-usage/http/rest-client.md#headerlinkpaginator) | The links to the next page are in the response headers.<br/>*Parameters:*<ul><li>`links_next_key` (str) - the name of the header containing the links. Default is "next".</li></ul> |
| `header_cursor` | [HeaderCursorPaginator](../../../general-usage/http/rest-client.md#headercursorpaginator) | The cursor for the next page is in the response headers.<br/>*Parameters:*<ul><li>`cursor_key` (str) - the name of the header containing the cursor. Defaults to "next"</li><li>`cursor_param` (str) - the query parameter name for the cursor. Defaults to "cursor"</li></ul> |
| `offset` | [OffsetPaginator](../../../general-usage/http/rest-client.md#offsetpaginator) | The pagination is based on an offset parameter, with the total items count either in the response body or explicitly provided.<br/>*Parameters:*<ul><li>`limit` (int) - the maximum number of items to retrieve in each request</li><li>`offset` (int) - the initial offset for the first request. Defaults to `0`</li><li>`offset_param` (str) - the name of the query parameter used to specify the offset. Defaults to "offset"</li><li>`limit_param` (str) - the name of the query parameter used to specify the limit. Defaults to "limit"</li><li>`total_path` (str) - a JSONPath expression for the total number of items. If not provided, pagination is controlled by `maximum_offset` and `stop_after_empty_page`</li><li>`maximum_offset` (int) - optional maximum offset value. Limits pagination even without total count</li><li>`stop_after_empty_page` (bool) - Whether pagination should stop when a page contains no result items. Defaults to `True`</li><li>`has_more_path` (str) - a JSONPath expression for the boolean value indicating whether there are more items to fetch. Defaults to `None`.</li></ul> |
| `page_number` | [PageNumberPaginator](../../../general-usage/http/rest-client.md#pagenumberpaginator) | The pagination is based on a page number parameter, with the total pages count either in the response body or explicitly provided.<br/>*Parameters:*<ul><li>`base_page` (int) - the starting page number. Defaults to `0`</li><li>`page_param` (str) - the query parameter name for the page number. Defaults to "page"</li><li>`total_path` (str) - a JSONPath expression for the total number of pages. If not provided, pagination is controlled by `maximum_page` and `stop_after_empty_page`</li><li>`maximum_page` (int) - optional maximum page number. Stops pagination once this page is reached</li><li>`stop_after_empty_page` (bool) - Whether pagination should stop when a page contains no result items. Defaults to `True`</li><li>`has_more_path` (str) - a JSONPath expression for the boolean value indicating whether there are more items to fetch. Defaults to `None`.</li></ul> |
| `cursor` | [JSONResponseCursorPaginator](../../../general-usage/http/rest-client.md#jsonresponsecursorpaginator) | The pagination is based on a cursor parameter, with the value of the cursor in the response body (JSON).<br/>*Parameters:*<ul><li>`cursor_path` (str) - the JSONPath to the cursor value. Defaults to "cursors.next"</li><li>`cursor_param` (str) - the query parameter name for the cursor. Defaults to "cursor" if neither `cursor_param` nor `cursor_body_path` is provided.</li><li>`cursor_body_path` (str, optional) - the JSONPath to place the cursor in the request body.</li></ul>Note: You must provide either `cursor_param` or `cursor_body_path`, but not both. If neither is provided, `cursor_param` will default to "cursor". |
| `single_page` | SinglePagePaginator | The response will be interpreted as a single-page response, ignoring possible pagination metadata. |
| `auto` | `None` | Explicitly specify that the source should automatically detect the pagination method. |

#### Pagination configuration hierarchy

Paginators are applied in the following order of precedence:

1. Endpoint-level paginator: defined in individual [resource endpoint configurations](#endpoint-configuration).
2. Resource defaults paginator: defined in `resource_defaults.endpoint.paginator`
3. Client-level paginator: defined in the [client](#client) configuration. This is the lowest priority.

#### Single entity endpoint detection

The REST API source tries to automatically detect endpoints that return single entities (e.g. not paginated lists of items). It works by looking if the path contains any placeholders with references to other resources (for example `users/{resources.users.id}/` or `user/{id}`) and applies special handling:

- If no `paginator` is explicitly configured at endpoint or resource defaults level, these endpoints automatically use `SinglePagePaginator`.
- If no `data_selector` is explicitly configured, these endpoints automatically use `"$"` to select the entire response.

:::warning
Single entity detection only applies when no paginator is configured at endpoint or resource defaults level. So:

- Endpoint-level paginators always take precedence and are _never overridden_ by single entity detection.
- Resource defaults paginators are preserved and _not overridden_ by single entity detection.
- Client-level paginators are ignored and _get overridden_ by single entity detection.

To change this behavior for a specific endpoint, explicitly set the `paginator` in the endpoint configuration:

```py
{
    "name": "user_details",
    "endpoint": {
        "path": "user/{resources.users.id}/comments",
        "paginator": {"type": "json_link", "next_url_path": "next"}
    }
}
```
:::

#### Custom paginators

For more complex pagination methods, you can implement a [custom paginator](../../../general-usage/http/rest-client.md#implementing-a-custom-paginator), instantiate it, and use it in the configuration.

Alternatively, you can use the dictionary configuration syntax also for custom paginators. For this, you need to register your custom paginator:

```py
from dlt.sources.rest_api.config_setup import register_paginator

class CustomPaginator(SinglePagePaginator):
    # custom implementation of SinglePagePaginator
    pass

register_paginator("custom_paginator", CustomPaginator)

{
    # ...
    "paginator": {
        "type": "custom_paginator",
        "next_url_path": "paging.nextLink",
    }
}
```

### Data selection

The `data_selector` field in the endpoint configuration allows you to specify a JSONPath to select the data from the response. By default, the source will try to detect the locations of the data automatically.

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

:::tip
For field names with special characters (dots, @ symbols, etc.), use the bracket notation escaping technique described in the [pagination section](#pagination).
:::

### Authentication

For APIs that require authentication to access their endpoints, the REST API source supports various authentication methods, including token-based authentication, query parameters, basic authentication, and custom authentication. The authentication configuration is specified in the `auth` field of the [client](#client) either as a dictionary or as an instance of the [authentication class](../../../general-usage/http/rest-client.md#authentication).

#### Quick example

Here's how to configure authentication using a bearer token:

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
        "auth": BearerTokenAuth(dlt.secrets["your_api_token"]),
    },
    "resources": [
    ]
    # ...
}
```

Since token-based authentication is one of the most common methods, you can use the following shortcut:

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

| `type` | Authentication class | Description |
| ----------- | ------------------- | ----------- |
| `bearer` | [BearerTokenAuth](../../../general-usage/http/rest-client.md#bearer-token-authentication) | Bearer token authentication.<br/>Parameters:<ul><li>`token` (str)</li></ul> |
| `http_basic` | [HTTPBasicAuth](../../../general-usage/http/rest-client.md#http-basic-authentication) | Basic HTTP authentication.<br/>Parameters:<ul><li>`username` (str)</li><li>`password` (str)</li></ul> |
| `api_key` | [APIKeyAuth](../../../general-usage/http/rest-client.md#api-key-authentication) | API key authentication with key defined in the query parameters or in the headers. <br/>Parameters:<ul><li>`name` (str) - the name of the query parameter or header</li><li>`api_key` (str) - the API key value</li><li>`location` (str, optional) - the location of the API key in the request. Can be `query` or `header`. Default is `header`</li></ul> |
| `oauth2_client_credentials` | [OAuth2ClientCredentials](../../../general-usage/http/rest-client.md#oauth-20-authorization) | OAuth 2.0 Client Credentials authorization for server-to-server communication without user consent. <br/>Parameters:<ul><li>`access_token` (str, optional) - the temporary token. Usually not provided here because it is automatically obtained from the server by exchanging `client_id` and `client_secret`. Default is `None`</li><li>`access_token_url` (str) - the URL to request the `access_token` from</li><li>`client_id` (str) - identifier for your app. Usually issued via a developer portal</li><li>`client_secret` (str) - client credential to obtain authorization. Usually issued via a developer portal</li><li>`access_token_request_data` (dict, optional) - A dictionary with data required by the authorization server apart from the `client_id`, `client_secret`, and `"grant_type": "client_credentials"`. Defaults to `None`</li><li>`default_token_expiration` (int, optional) - The time in seconds after which the temporary access token expires. Defaults to 3600.</li><li>`session` (requests.Session, optional) - a custom session object. Mostly used for testing</li></ul> |


For more complex authentication methods, you can implement a [custom authentication class](../../../general-usage/http/rest-client.md#implementing-custom-authentication) and use it in the configuration.

You can use the dictionary configuration syntax also for custom authentication classes after registering them as follows:

```py
from dlt.common.configuration import configspec
from dlt.sources.rest_api.config_setup import register_auth

@configspec
class CustomAuth(AuthConfigBase):
    pass

register_auth("custom_auth", CustomAuth)

{
    # ...
    "auth": {
        "type": "custom_auth",
        "api_key": dlt.secrets["sources.my_source.my_api_key"],
    }
}
```

### Define resource relationships

When you have a resource that depends on another resource (for example, you must fetch a parent resource to get an ID needed to fetch the child), you can reference fields in the parent resource using special placeholders.
This allows you to link one or more [path](#via-request-path), [query string](#via-query-string-parameters) or [JSON body](#via-json-body) parameters in the child resource to fields in the parent resource's data.

#### Via request path

In the GitHub example, the `issue_comments` resource depends on the `issues` resource. The `resources.issues.number` placeholder links the `number` field in the `issues` resource data to the current request's path parameter.

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
                "path": "issues/{resources.issues.number}/comments",
            },
            "include_from_parent": ["id"],
        },
    ],
}
```

This configuration tells the source to get issue numbers from the `issues` resource data and use them to fetch comments for each issue number. So for each issue item, `"{resources.issues.number}"` is replaced by the issue number in the request path.
For example, if the `issues` resource yields the following data:

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

The syntax for the placeholder is `resources.<parent_resource_name>.<field_name>`.

#### Via query string parameters

The placeholder syntax can also be used in the query string parameters. For example, in an API which lets you fetch a blog posts (via `/posts`) and their comments (via `/comments?post_id=<post_id>`), you can define a resource `posts` and a resource `post_comments` which depends on the `posts` resource. You can then reference the `id` field from the `posts` resource in the `post_comments` resource:

```py
{
    "resources": [
        "posts",
        {
            "name": "post_comments",
            "endpoint": {
                "path": "comments",
                "params": {
                    "post_id": "{resources.posts.id}",
                },
            },
        },
    ],
}
```

Similar to the GitHub example above, if the `posts` resource yields the following data:

```json
[
    {"id": 1, "title": "Post 1"},
    {"id": 2, "title": "Post 2"},
    {"id": 3, "title": "Post 3"}
]
```

The `post_comments` resource will make requests to the following endpoints:

- `comments?post_id=1`
- `comments?post_id=2`
- `comments?post_id=3`

#### Via JSON body

In many APIs, you can send a complex query or configuration through a POST request's JSON body rather than in the request path or query parameters. For example, consider an imaginary `/search` endpoint that supports multiple filters and settings. You might have a parent resource `posts` with each post's `id` and a second resource, `post_details`, that uses `id` to perform a custom search.

In the example below we reference the `posts` resource's `id` field in the JSON body via placeholders:

```py
{
    "resources": [
        "posts",
        {
            "name": "post_details",
            "endpoint": {
                "path": "search",
                "method": "POST",
                "json": {
                    "filters": {
                        "id": "{resources.posts.id}",
                    },
                    "order": "desc",
                    "limit": 5,
                }
            },
        },
    ],
}
```

:::tip Escaping curly braces
When your API requires literal curly braces in parameters (e.g., for JSON filters or GraphQL queries), escape them by doubling: `{{` and `}}`.

Example with GraphQL query:
```py
{
    "json": {
        "query": """
            query Artist {{
                artist(id: "{resources.artist_list.id}") {{
                    id
                    name
                }}
            }}
        """
    }
}
```

Use the same technique for GET request query string parameters:

```py
{
    "params": {
        "search": "{{'filters': [{{'id': 42}}]}}"
    }
}
```

:::

#### Legacy syntax: `resolve` field in parameter configuration

:::warning
`resolve` works only for path parameters. The new placeholder syntax is more flexible and recommended for new configurations.
:::

An alternative, legacy way to define resource relationships is to use the `resolve` field in the parameter configuration.
Here's the same example as above that uses the `resolve` field:

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


#### Resolving multiple path parameters from a parent resource

When a child resource depends on multiple fields from a single parent resource, you can define multiple `resolve` parameters in the endpoint configuration. For example:

```py
{
    "resources": [
        "groups",
        {
            "name": "users",
            "endpoint": {
                "path": "groups/{group_id}/users",
                "params": {
                    "group_id": {
                        "type": "resolve",
                        "resource": "groups",
                        "field": "id",
                    },
                },
            },
        },
        {
            "name": "user_details",
            "endpoint": {
                "path": "groups/{group_id}/users/{user_id}/details",
                "params": {
                    "group_id": {
                        "type": "resolve",
                        "resource": "users",
                        "field": "group_id",
                    },
                    "user_id": {
                        "type": "resolve",
                        "resource": "users",
                        "field": "id",
                    },
                },
            },
        },
    ],
}
```

In the configuration above:

- The `users` resource depends on the `groups` resource, resolving the `group_id` parameter from the `id` field in `groups`.
- The `user_details` resource depends on the `users` resource, resolving both `group_id` and `user_id` parameters from fields in `users`.

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

This will include the `id`, `title`, and `created_at` fields from the `issues` resource in the `issue_comments` resource data. The names of the included fields will be prefixed with the parent resource name and an underscore (`_`) like so: `_issues_id`, `_issues_title`, `_issues_created_at`.

### Define a resource which is not a REST endpoint

Sometimes, we want to request endpoints with specific values that are not returned by another endpoint.
Thus, you can also include arbitrary dlt resources in your `RESTAPIConfig` instead of defining a resource for every path!

In the following example, we want to load the issues belonging to three repositories.
Instead of defining three different issues resources, one for each of the paths `dlt-hub/dlt/issues/`, `dlt-hub/verified-sources/issues/`, `dlt-hub/dlthub-education/issues/`, we have a resource `repositories` which yields a list of repository names that will be fetched by the dependent resource `issues`.

```py
from dlt.sources.rest_api import RESTAPIConfig

@dlt.resource()
def repositories() -> Generator[List[Dict[str, Any]], Any, Any]:
    """A seed list of repositories to fetch"""
    yield [{"name": "dlt"}, {"name": "verified-sources"}, {"name": "dlthub-education"}]


config: RESTAPIConfig = {
    "client": {"base_url": "https://github.com/api/v2"},
    "resources": [
        {
            "name": "issues",
            "endpoint": {
                "path": "dlt-hub/{repository}/issues/",
                "params": {
                    "repository": {
                        "type": "resolve",
                        "resource": "repositories",
                        "field": "name",
                    },
                },
            },
        },
        repositories(),
    ],
}
```

Be careful that the parent resource needs to return `Generator[List[Dict[str, Any]]]`. Thus, the following will NOT work:

```py
@dlt.resource
def repositories() -> Generator[Dict[str, Any], Any, Any]:
    """Not working seed list of repositories to fetch"""
    yield from [{"name": "dlt"}, {"name": "verified-sources"}, {"name": "dlthub-education"}]
```

### Processing steps: filter and transform data

The `processing_steps` field in the resource configuration allows you to apply transformations to the data fetched from the API before it is loaded into your destination. This is useful when you need to filter out certain records, modify the data structure, or anonymize sensitive information.

Each processing step is a dictionary specifying the type of operation (`filter`, `map` or `yield_map`) and the function to apply. Steps apply in the order they are listed.

#### Quick example

```py
def lower_title(record):
    record["title"] = record["title"].lower()
    return record

def flatten_reactions(post):
    post_without_reactions = copy.deepcopy(post)
    post_without_reactions.pop("reactions")
    for reaction in post["reactions"]:
        yield {"reaction": reaction, **post_without_reactions}

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com",
    },
    "resources": [
        {
            "name": "posts",
            "processing_steps": [
                {"filter": lambda x: x["id"] < 10},
                {"map": lower_title},
                {"yield_map": flatten_reactions},
            ],
        },
    ],
}
```

In the example above:

- First, the `filter` step uses a lambda function to include only records where `id` is less than 10.
- Then, the `map` step applies the `lower_title` function to each remaining record.
- Finally, the `yield_map` step applies the `flatten_reactions` function to each transformed record, 
yielding a set of records, one for each reaction for the given post.

#### Using `filter`

The `filter` step allows you to exclude records that do not meet certain criteria. The provided function should return `True` to keep the record or `False` to exclude it:

```py
{
    "name": "posts",
    "endpoint": "posts",
    "processing_steps": [
        {"filter": lambda x: x["id"] in [10, 20, 30]},
    ],
}
```

In this example, only records with `id` equal to 10, 20, or 30 will be included.

#### Using `map`

The `map` step allows you to modify the records fetched from the API. The provided function should take a record as an argument and return the modified record. For example, to anonymize the `email` field:

```py
def anonymize_email(record):
    record["email"] = "REDACTED"
    return record

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com",
    },
    "resources": [
        {
            "name": "users",
            "processing_steps": [
                {"map": anonymize_email},
            ],
        },
    ],
}
```

#### Using `yield_map`

The `yield_map` step allows you to transform a record into multiple records. The provided function should take a record as an argument and return an iterator of records. For example, to flatten the `reactions` field:

```py
def flatten_reactions(post):
    post_without_reactions = copy.deepcopy(post)
    post_without_reactions.pop("reactions")
    for reaction in post["reactions"]:
        yield {"reaction": reaction, **post_without_reactions}

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com",
    },
    "resources": [
        {
            "name": "posts",
            "processing_steps": [
                {"yield_map": flatten_reactions},
            ],
        },
    ],
}
```
#### Combining `filter` and `map`

You can combine multiple processing steps to achieve complex transformations:

```py
{
    "name": "posts",
    "endpoint": "posts",
    "processing_steps": [
        {"filter": lambda x: x["id"] < 10},
        {"map": lower_title},
        {"filter": lambda x: "important" in x["title"]},
    ],
}
```

:::tip
#### Best practices
1. Order matters: Processing steps are applied in the order they are listed. Be mindful of the sequence, especially when combining `map` and `filter`.
2. Function definition: Define your filter and map functions separately for clarity and reuse.
3. Use `filter` to exclude records early in the process to reduce the amount of data that needs to be processed.
4. Combine consecutive `map` steps into a single function for faster execution.
:::

## Incremental loading

Some APIs provide a way to fetch only new or changed data (most often by using a timestamp field like `updated_at`, `created_at`, or incremental IDs).
This is called [incremental loading](../../../general-usage/incremental-loading.md) and is very useful as it allows you to reduce the load time and the amount of data transferred.

Let's continue with our imaginary blog API example to understand incremental loading with query parameters.

Imagine we have the following endpoint `https://api.example.com/posts` and it:
1. Accepts a `created_since` query parameter to fetch blog posts created after a certain date.
2. Returns a list of posts with the `created_at` field for each post.

For example, if we query the endpoint with GET request `https://api.example.com/posts?created_since=2024-01-25`, we get the following response:

```json
{
    "results": [
        {"id": 1, "title": "Post 1", "created_at": "2024-01-26"},
        {"id": 2, "title": "Post 2", "created_at": "2024-01-27"},
        {"id": 3, "title": "Post 3", "created_at": "2024-01-28"}
    ]
}
```

When the API endpoint supports incremental loading, you can configure dlt to load only the new or changed data using these three methods:

1. Using [placeholders for incremental loading](#using-placeholders-for-incremental-loading)
2. Defining a special parameter in the `params` section of the [endpoint configuration](#endpoint-configuration) (DEPRECATED)
3. Using the `incremental` field in the [endpoint configuration](#endpoint-configuration) with the `start_param` field (DEPRECATED)

:::warning
The last two methods are deprecated and will be removed in a future dlt version.
:::

### Using placeholders for incremental loading

The most flexible way to configure incremental loading is to use placeholders in the request configuration along with the `incremental` section.
Here's how it works:

1. Define the `incremental` section in the [endpoint configuration](#endpoint-configuration) to specify the cursor path (where to find the incremental value in the response) and initial value (the value to start the incremental loading from).
2. Use the placeholder `{incremental.start_value}` in the request configuration to reference the incremental value.

Let's take the example from the previous section and configure it using placeholders:

```py
{
    "path": "posts",
    "data_selector": "results",
    "params": {
        "created_since": "{incremental.start_value}",  # Uses cursor value in query parameter
    },
    "incremental": {
        "cursor_path": "created_at",
        "initial_value": "2024-01-25T00:00:00Z",
    },
}
```

When you first run this pipeline, dlt will:
1. Replace `{incremental.start_value}` with `2024-01-25T00:00:00Z` (the initial value)
2. Make a GET request to `https://api.example.com/posts?created_since=2024-01-25T00:00:00Z`
3. Parse the response (e.g., posts with created_at values like "2024-01-26", "2024-01-27", "2024-01-28")
4. Track the maximum value found in the "created_at" field (in this case, "2024-01-28")

On the next pipeline run, dlt will:
1. Replace `{incremental.start_value}` with "2024-01-28" (the last seen maximum value)
2. Make a GET request to `https://api.example.com/posts?created_since=2024-01-28`
3. The API will only return posts created on or after January 28th

Let's break down the configuration:
1. We explicitly set `data_selector` to `"results"` to select the list of posts from the response. This is optional; if not set, dlt will try to auto-detect the data location.
2. We define the `created_since` parameter in `params` section and use the placeholder `{incremental.start_value}` to reference the incremental value.

Placeholders are versatile and can be used in various request components. Here are some examples:

#### In JSON body (for POST requests)

If the API lets you filter the data by a range of dates (e.g. `fromDate` and `toDate`), you can use the placeholder in the JSON body:

```py
{
    "path": "posts/search",
    "method": "POST",
    "json": {
        "filters": {
            "fromDate": "{incremental.start_value}",  # In JSON body
            "toDate": "2024-03-25"
        },
        "limit": 1000
    },
    "incremental": {
        "cursor_path": "created_at",
        "initial_value": "2024-01-25T00:00:00Z",
    },
}
```

#### In path parameters

Some APIs use path parameters to filter the data:

```py
{
    "path": "posts/since/{incremental.start_value}/list",  # In URL path
    "incremental": {
        "cursor_path": "created_at",
        "initial_value": "2024-01-25",
    },
}
```

#### In request headers

You can also use placeholders in request headers:

```py
{
    "path": "posts",
    "headers": {
        "X-Since-Timestamp": "{incremental.start_value}"  # In custom header
    },
    "incremental": {
        "cursor_path": "created_at",
        "initial_value": "2024-01-25T00:00:00Z",
    },
}
```

For more details on headers configuration and dynamic placeholders, see the [headers configuration](./advanced#headers-configuration) section.

You can also use different placeholder variants depending on your needs:

| Placeholder | Description |
| ----------- | ----------- |
| `{incremental.start_value}` | The value to use as the starting point for this request (either the initial value or the last tracked maximum value) |
| `{incremental.initial_value}` | Always uses the initial value specified in the configuration |
| `{incremental.last_value}` | The last seen value (same as start_value in most cases, see the [incremental loading](../../../general-usage/incremental/cursor.md) guide for more details) |
| `{incremental.end_value}` | The end value if specified in the configuration |


### Legacy method: Incremental loading in `params` (DEPRECATED)

:::warning
DEPRECATED: This method is deprecated and will be removed in a future version. Use the [placeholder method](#using-placeholders-for-incremental-loading) instead.
:::

:::note
This method only works for query string parameters. For other request parts (path, JSON body, headers), use the [placeholder method](#using-placeholders-for-incremental-loading). For more details on headers, see the [headers configuration](./advanced#headers-configuration) section.
:::

For query string parameters, you can also specify incremental loading directly in the `params` section:

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

Above we define the `created_since` parameter as an incremental parameter as:

```py
{
    "created_since": {
        "type": "incremental",
        "cursor_path": "created_at",
        "initial_value": "2024-01-25",
    },
}
```

The fields are:

- `type`: The type of the parameter definition. In this case, it must be set to `incremental`.
- `cursor_path`: The JSONPath to the field within each item in the list. The value of this field will be used in the next request. In the example above, our items look like `{"id": 1, "title": "Post 1", "created_at": "2024-01-26"}` so to track the created time, we set `cursor_path` to `"created_at"`. Note that the JSONPath starts from the root of the item (dict) and not from the root of the response.
- `initial_value`: The initial value for the cursor. This is the value that will initialize the state of incremental loading. In this case, it's `2024-01-25`. The value type should match the type of the field in the data item.

### Incremental loading using the `incremental` field (DEPRECATED)

:::warning
DEPRECATED: This method is deprecated and will be removed in a future dlt version. Use the [placeholder method](#using-placeholders-for-incremental-loading) instead.
:::

Another alternative method is to use the `incremental` field in the [endpoint configuration](#endpoint-configuration) while specifying names of the query string parameters to be used as start and end conditions.

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
The full available configuration for the `incremental` field is:

```py
{
    "incremental": {
        "start_param": "<start_parameter_name>",
        "end_param": "<end_parameter_name>",
        "cursor_path": "<path_to_cursor_field>",
        "initial_value": "<initial_value>",
        "end_value": "<end_value>",
        "convert": my_callable,
    }
}
```

The fields are:

- `start_param` (str): The name of the query parameter to be used as the start condition. If we use the example above, it would be `"created_since"`.
- `end_param` (str): The name of the query parameter to be used as the end condition. This is optional and can be omitted if you only need to track the start condition. This is useful when you need to fetch data within a specific range and the API supports end conditions (like the `created_before` query parameter).
- `cursor_path` (str): The JSONPath to the field within each item in the list. This is the field that will be used to track the incremental loading. In the example above, it's `"created_at"`.
- `initial_value` (str): The initial value for the cursor. This is the value that will initialize the state of incremental loading.
- `end_value` (str): The end value for the cursor to stop the incremental loading. This is optional and can be omitted if you only need to track the start condition. If you set this field, `initial_value` needs to be set as well.
- `convert` (callable): A callable that converts the cursor value into the format that the query parameter requires. For example, a UNIX timestamp can be converted into an ISO 8601 date or a date can be converted into `created_at+gt+{date}`.

See the [incremental loading](../../../general-usage/incremental/cursor.md) guide for more details.

If you encounter issues with incremental loading, see the [troubleshooting section](../../../general-usage/incremental/troubleshooting.md) in the incremental loading guide.

### Convert the incremental value before calling the API

If you need to transform the values in the cursor field before passing them to the API endpoint, you can specify a callable under the key `convert`. For example, the API might return UNIX epoch timestamps but expects to be queried with an ISO 8601 date. To achieve that, we can specify a function that converts from the date format returned by the API to the date format required for API requests.

In the following examples, `1704067200` is returned from the API in the field `updated_at`, but the API will be called with `?created_since=2024-01-01`.

Incremental loading using the `params` field:
```py
{
    "created_since": {
        "type": "incremental",
        "cursor_path": "updated_at",
        "initial_value": "1704067200",
        "convert": lambda epoch: pendulum.from_timestamp(int(epoch)).to_date_string(),
    }
}
```

Incremental loading using the `incremental` field:
```py
{
    "path": "posts",
    "data_selector": "results",
    "incremental": {
        "start_param": "created_since",
        "cursor_path": "updated_at",
        "initial_value": "1704067200",
        "convert": lambda epoch: pendulum.from_timestamp(int(epoch)).to_date_string(),
    },
}
```


## Troubleshooting

If you encounter issues while running the pipeline, enable [logging](../../../running-in-production/running.md#set-the-log-level-and-format) for detailed information about the execution:

```sh
RUNTIME__LOG_LEVEL=INFO python my_script.py
```

This also provides details on the HTTP requests. If you want to see even more details, you can enable the HTTP error response bodies.

### Viewing HTTP error response bodies

By default, HTTP error response bodies are not included in error messages to keep logs concise. However, during development or debugging, you may want to see the full error response from the API.

To enable error response bodies in logs and exceptions in your `config.toml` file:

```toml
[runtime]
http_show_error_body = true
```

Or set via environment variables:
```sh
export RUNTIME__HTTP_SHOW_ERROR_BODY=true
```

### Automatic secret redaction in logs

The REST API source automatically redacts sensitive query parameters in URLs when logging or raising errors. The following parameter names are automatically considered sensitive and will be replaced with `***`:

- `api_key`, `token`, `key`, `access_token`, `apikey`, `api-key`, `access-token`
- `secret`, `password`, `pwd`, `client_secret`
- `username`, `client_id`

### Configuration issues

#### Getting validation errors

When you are running the pipeline and getting a `DictValidationException`, it means that the [source configuration](#source-configuration) is incorrect. The error message provides details on the issue, including the path to the field and the expected type.

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
from dlt.sources.rest_api import RESTAPIConfig
```
:::

#### Getting wrong data or no data

If incorrect data is received from an endpoint, check the `data_selector` field in the [endpoint configuration](#endpoint-configuration). Ensure the JSONPath is accurate and points to the correct data in the response body. `rest_api` attempts to auto-detect the data location, which may not always succeed. See the [data selection](#data-selection) section for more details.

#### Getting insufficient data or incorrect pagination

Check the `paginator` field in the configuration. When not explicitly specified, the source tries to auto-detect the pagination method. If auto-detection fails, or the system is unsure, a warning is logged. For production environments, we recommend specifying an explicit paginator in the configuration. See the [pagination](#pagination) section for more details. Some APIs may have non-standard pagination methods, and you may need to implement a [custom paginator](../../../general-usage/http/rest-client.md#implementing-a-custom-paginator).

#### Incremental loading not working

See the [troubleshooting guide](../../../general-usage/incremental/troubleshooting.md) for incremental loading issues.

#### Getting HTTP 404 errors

Some APIs may return 404 errors for resources that do not exist or have no data. Manage these responses by configuring the `ignore` action in [response actions](./advanced#response-actions).

### Authentication issues

If you are experiencing 401 (Unauthorized) errors, this could indicate:

- Incorrect authorization credentials. Verify credentials in the `secrets.toml`. Refer to [Secret and configs](../../../general-usage/credentials/setup#troubleshoot-configuration-errors) for more information.
- An incorrect authentication type. Consult the API documentation for the proper method. See the [authentication](#authentication) section for details. For some APIs, a [custom authentication method](../../../general-usage/http/rest-client.md#implementing-custom-authentication) may be required.

### General guidelines

The `rest_api` source uses the [RESTClient](../../../general-usage/http/rest-client.md) class for HTTP requests. Refer to the RESTClient [troubleshooting guide](../../../general-usage/http/rest-client.md#troubleshooting) for debugging tips.

For further assistance, join our [Slack community](https://dlthub.com/community). We're here to help!

