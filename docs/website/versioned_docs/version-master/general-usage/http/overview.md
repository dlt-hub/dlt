---
title: REST API helpers
description: Use the dlt RESTClient to interact with RESTful APIs and paginate the results
keywords: [api, http, rest, restful, requests, restclient, paginate, pagination, json]
---

dlt has built-in support for fetching data from APIs:
- [RESTClient](./rest-client.md) for interacting with RESTful APIs and paginating the results
- [Requests wrapper](./requests.md) for making simple HTTP requests with automatic retries and timeouts

Additionally, dlt provides tools to simplify working with APIs:
- [REST API generic source](../../dlt-ecosystem/verified-sources/rest_api) integrates APIs using a [declarative configuration](../../dlt-ecosystem/verified-sources/rest_api#source-configuration) to minimize custom code.
- [OpenAPI source generator](../../dlt-ecosystem/verified-sources/openapi-generator) automatically creates declarative API configurations from [OpenAPI specifications](https://swagger.io/specification/).

## Quick example

Here's a simple pipeline that reads issues from the [dlt GitHub repository](https://github.com/dlt-hub/dlt/issues). The API endpoint is https://api.github.com/repos/dlt-hub/dlt/issues. The result is "paginated," meaning that the API returns a limited number of issues per page. The `paginate()` method iterates over all pages and yields the results which are then processed by the pipeline.

```py
import dlt
from dlt.sources.helpers.rest_client import RESTClient

github_client = RESTClient(base_url="https://api.github.com")  # (1)

@dlt.resource
def get_issues():
    for page in github_client.paginate(                        # (2)
        "/repos/dlt-hub/dlt/issues",                           # (3)
        params={                                               # (4)
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
        },
    ):
        yield page                                             # (5)


pipeline = dlt.pipeline(
    pipeline_name="github_issues",
    destination="duckdb",
    dataset_name="github_data",
)
load_info = pipeline.run(get_issues)
print(load_info)
```

Here's what the code does:
1. We create a `RESTClient` instance with the base URL of the API: in this case, the GitHub API (https://api.github.com).
2. The issues endpoint returns a list of issues. Since there could be hundreds of issues, the API "paginates" the results: it returns a limited number of issues in each response along with a link to the next batch of issues (or "page"). The `paginate()` method iterates over all pages and yields the batches of issues.
3. Here we specify the address of the endpoint we want to read from: `/repos/dlt-hub/dlt/issues`.
4. We pass the parameters to the actual API call to control the data we get back. In this case, we ask for 100 issues per page (`"per_page": 100`), sorted by the last update date (`"sort": "updated"`) in descending order (`"direction": "desc"`).
5. We yield the page from the resource function to the pipeline. The `page` is an instance of the [`PageData`](#pagedata) and contains the data from the current page of the API response and some metadata.

Note that we do not explicitly specify the pagination parameters in the example. The `paginate()` method handles pagination automatically: it detects the pagination mechanism used by the API from the response. What if you need to specify the pagination method and parameters explicitly? Let's see how to do that in a different example below.

## Explicitly specifying pagination parameters

```py
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

github_client = RESTClient(
    base_url="https://pokeapi.co/api/v2",
    paginator=JSONLinkPaginator(next_url_path="next"),   # (1)
    data_selector="results",                             # (2)
)

@dlt.resource
def get_pokemons():
    for page in github_client.paginate(
        "/pokemon",
        params={
            "limit": 100,                                    # (3)
        },
    ):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="get_pokemons",
    destination="duckdb",
    dataset_name="github_data",
)
load_info = pipeline.run(get_pokemons)
print(load_info)
```

In the example above:
1. We create a `RESTClient` instance with the base URL of the API: in this case, the [PokéAPI](https://pokeapi.co/). We also specify the paginator to use explicitly: `JSONLinkPaginator` with the `next_url_path` set to `"next"`. This tells the paginator to look for the next page URL in the `next` key of the JSON response.
2. In `data_selector`, we specify the JSON path to extract the data from the response. This is used to extract the data from the response JSON.
3. By default, the number of items per page is limited to 20. We override this by specifying the `limit` parameter in the API call.

