---
title: Advanced configuration
description: Learn custom response processing
keywords: [rest api, restful api]
---

`rest_api_source()` function creates the [dlt source](../../../general-usage/source.md) and lets you configure the following parameters:

- `config`: The REST API configuration dictionary.
- `name`: An optional name for the source.
- `section`: An optional section name in the configuration file.
- `max_table_nesting`: Sets the maximum depth of nested tables above which the remaining nodes are loaded as structs or JSON.
- `root_key` (bool): Enables merging on all resources by propagating the root foreign key to nested tables. This option is most useful if you plan to change the write disposition of a resource to disable/enable merge. Defaults to False.
- `schema_contract`: Schema contract settings that will be applied to this resource.
- `spec`: A specification of configuration and secret values required by the source.

### Response actions

The `response_actions` field in the endpoint configuration allows you to specify how to handle specific responses or all responses from the API. For example, responses with specific status codes or content substrings can be ignored.
Additionally, all responses or only responses with specific status codes or content substrings can be transformed with a custom callable, such as a function. This callable is passed on to the requests library as a [response hook](https://requests.readthedocs.io/en/latest/user/advanced/#event-hooks). The callable can modify the response object and must return it for the modifications to take effect.

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

