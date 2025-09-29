---
title: Advanced configuration
description: Learn custom response processing, headers configuration and more
keywords: [rest api, restful api, headers, response actions, advanced configuration]
---

`rest_api_source()` function creates the [dlt source](../../../general-usage/source.md) and lets you configure the following parameters:

- `config`: The REST API configuration dictionary.
- `name`: An optional name for the source.
- `section`: An optional section name in the configuration file.
- `max_table_nesting`: Sets the maximum depth of nested tables above which the remaining nodes are loaded as structs or JSON.
- `root_key` (bool): Enables merging on all resources by propagating the root foreign key to nested tables. This option is most useful if you plan to change the write disposition of a resource to disable/enable merge. Defaults to False.
- `schema`: An explicit `dlt.Schema` instance to be associated with the source. If not present, `dlt` creates a new `Schema` object with the provided `name`. If such `dlt.Schema` already exists in the same folder as the module containing the decorated function, such schema will be loaded from file.
- `schema_contract`: Schema contract settings that will be applied to this resource.
- `parallelized`: If `True`, resource generators will be extracted in parallel with other resources.

### Headers configuration

The REST API source supports configuring custom headers at both the client level and the endpoint level. This allows you to send additional HTTP headers with your API requests, which is useful for some use cases.

Headers can be configured in two places:

1. [Client-level headers](#client-level-headers): Static headers applied to all requests for all resources.
2. [Endpoint-level headers](#endpoint-level-headers): Headers applied only to requests for a specific resource (support placeholder interpolation). These are also can be specified via `resource_defaults`.

When both client-level and endpoint-level headers are specified, endpoint-level headers override client-level headers for the same header names.

##### Client-level headers

Client-level headers are static and applied to all requests:

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com",
        "headers": {
            "User-Agent": "MyApp/1.0",
            "Accept": "application/json",
            "X-API-Version": "v2"
        }
    },
    "resources": ["posts", "comments"]
}
```

##### Endpoint-level headers

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com",
        "headers": {
            "User-Agent": "MyApp/1.0"
        }
    },
    "resources": [
        {
            "name": "posts",
            "endpoint": {
                "path": "posts",
                "headers": {
                    "Content-Type": "application/vnd.api+json",
                    "X-Custom-Header": "posts-specific-value"
                }
            }
        }
    ]
}
```

#### Dynamic headers with placeholders

[Endpoint-level headers](#endpoint-level-headers) support dynamic values using placeholder interpolation. This allows you to reference data from parent resources and incremental values.

:::note
Client-level headers do not support placeholder interpolation. If you need dynamic headers with placeholders, you must define them at the endpoint level.
:::

##### Example: headers with resource data

You can reference fields from parent resources in header values:

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com"
    },
    "resources": [
        "posts",
        {
            "name": "comments",
            "endpoint": {
                "path": "posts/{resources.posts.id}/comments",
                "headers": {
                    "X-Post-ID": "{resources.posts.id}",
                    "X-Post-Title": "{resources.posts.title}"
                }
            }
        }
    ]
}
```

In this example, for each post, the `comments` resource will include headers with the post's ID and title.

##### Example: headers with incremental values

You can also use incremental values in headers:

```py
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.example.com"
    },
    "resources": [
        {
            "name": "events",
            "endpoint": {
                "path": "events",
                "headers": {
                    "X-Since-Timestamp": "{incremental.start_value}"
                },
                "incremental": {
                    "cursor_path": "updated_at",
                    "initial_value": "2024-01-01T00:00:00Z"
                }
            }
        }
    ]
}
```

See the [incremental loading](./basic.md#incremental-loading) section for more details.

### Response actions

The `response_actions` field in the endpoint configuration allows you to specify how to handle specific responses or all responses from the API. For example, responses with specific status codes or content substrings can be ignored.
Additionally, all responses or only responses with specific status codes or content substrings can be transformed with a custom callable, such as a function. This callable is passed on to the requests library as a [response hook](https://requests.readthedocs.io/en/latest/user/advanced/#event-hooks). The callable can modify the response object and must return it for the modifications to take effect.

:::warning Experimental Feature
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
from requests.models import Response
from dlt.common import json

def set_encoding(response, *args, **kwargs):
    # Sets the encoding in case it's not correctly detected
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


source_config = {
    "client": {
        # ...
    },
    "resources": [
        {
            "name": "issues",
            "endpoint": {
                "path": "issues",
                "response_actions": [
                    set_encoding,
                    {
                        "status_code": 200,
                        "content": "some text",
                        "action": add_and_remove_fields,
                    },
                ],
            },
        },
    ],
}
```

In this example, the resource will set the correct encoding for all responses first. Thereafter, for all responses that have the status code 200, we will add a field `custom_field` and remove the field `email`.

#### Example C

```py
def set_encoding(response, *args, **kwargs):
    # Sets the encoding in case it's not correctly detected
    response.encoding = 'windows-1252'
    return response

source_config = {
    "client": {
        # ...
    },
    "resources": [
        {
            "name": "issues",
            "endpoint": {
                "path": "issues",
                "response_actions": [
                    set_encoding,
                ],
            },
        },
    ],
}
```

In this example, the resource will set the correct encoding for all responses. More callables can be added to the list of response_actions.


### Setup timeouts and retry strategies
`rest_api` uses `dlt` [custom sessions](../../../general-usage/http/requests.md) and [`RESTClient`](../../../general-usage/http/rest-client.md) to access http(s) endpoints. You can use them to configure timeout, retries and other aspects. For example:
```py
from dlt.sources.helpers import requests

source_config: RESTAPIConfig = {
    "client": {
        "session": requests.Client(request_timeout=(1.0, 1.0), request_max_attempts=0)
    },
}
```
will set-up all endpoints to use a short connect and read timeouts with no retries.
Most settings [can be configured](../../../general-usage/http/requests.md#customizing-retry-settings) using `toml` files or environment variables.

:::note
By default, we set connection timeout and read timeout to 60 seconds, with
5 retry attempts without backoff.
:::