# /// script
# dependencies = [
#     "dlt[duckdb]",
#     "numpy",
#     "pandas",
#     "sqlalchemy",
# ]
# ///

import marimo

__generated_with = "0.17.4"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""# **Building custom sources with [dlt REST API source](https://dlthub.com/docs/devel/dlt-ecosystem/verified-sources/rest_api/basic) and [RESTClient](https://dlthub.com/docs/devel/general-usage/http/rest-client)** [![Open in molab](https://marimo.io/molab-shield.svg)](https://molab.marimo.io/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_1_custom_sources_restapi_source_and_restclient.py) [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_1_custom_sources_restapi_source_and_restclient.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_1_custom_sources_restapi_source_and_restclient.ipynb)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# New section""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # **Recap**

    In the **[dlt Fundamentals](https://github.com/dlt-hub/dlthub-education/tree/main/courses/dlt_fundamentals_dec_2024)** course, we learned two primary ways to build sources for REST APIs:

    1. **Using low-level dlt decorators** (`@dlt.source` and `@dlt.resource`) with [`RESTClient`](https://dlthub.com/docs/devel/general-usage/http/rest-client).
    2. **Using the built-in [`rest_api` source](https://dlthub.com/docs/devel/dlt-ecosystem/verified-sources/rest_api/basic)** with declarative configuration.

    ---
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **1. Building sources with low-level dlt decorators**

    We constructed a custom source for the **GitHub API** using the `RESTClient` class, decorators like `@dlt.resource` and `@dlt.source`, and manual pagination handling.


    #### **Example**

    > Don't forget to use your [GitHub API token](https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api?apiVersion=2022-11-28) below!
    """
    )
    return


@app.cell
def _():
    from typing import Iterator, Any, Iterable
    import os
    import dlt
    from dlt.common.typing import TDataItems, TDataItem
    from dlt.sources import DltResource
    from dlt.sources.helpers import requests
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

    os.environ["ACCESS_TOKEN"] = os.getenv("ACCESS_TOKEN")

    @dlt.source
    def github_source(access_token: str = dlt.secrets.value) -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource
        def github_events() -> Iterator[TDataItems]:
            for page in client.paginate("orgs/dlt-hub/events"):
                yield page

        @dlt.resource
        def github_stargazers() -> Iterator[TDataItems]:
            for page in client.paginate("repos/dlt-hub/dlt/stargazers"):
                yield page

        return (github_events, github_stargazers)

    pipeline = dlt.pipeline(
        pipeline_name="rest_client_github",
        destination="duckdb",
        dataset_name="rest_client_data",
        dev_mode=True,
    )
    _load_info = pipeline.run(github_source())
    print(_load_info)
    return (
        Any,
        BearerTokenAuth,
        DltResource,
        HeaderLinkPaginator,
        Iterable,
        Iterator,
        RESTClient,
        TDataItems,
        dlt,
        os,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---

    ### **2. Building sources with `rest_api` source**

    The **`rest_api` source** provides a higher-level, declarative approach to building sources for REST APIs. It's particularly suited for REST APIs with predictable structures and behaviors.


    #### **Example**
    """
    )
    return


@app.cell
def _(dlt):
    from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com",
            "auth": {"token": dlt.secrets["access_token"]},
            "paginator": "header_link",
        },
        "resources": [
            {
                "name": "issues",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/issues",
                    "params": {"state": "open"},
                },
            },
            {
                "name": "issue_comments",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/issues/{issue_number}/comments",
                    "params": {
                        "issue_number": {
                            "type": "resolve",
                            "resource": "issues",
                            "field": "number",
                        }
                    },
                },
            },
            {
                "name": "contributors",
                "endpoint": {"path": "repos/dlt-hub/dlt/contributors"},
            },
        ],
    }
    git_source = rest_api_source(config)
    rest_api_pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
        dev_mode=True,
    )
    _load_info = rest_api_pipeline.run(git_source)
    print(_load_info)
    return RESTAPIConfig, rest_api_source


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # **REST API Client by `dlt`**

    `dlt`’s REST API Client is the low level abstraction that powers the REST API Source. You can use it in your imperative code for more automation and brevity, if you do not wish to use the higher level declarative interface.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    If you don't like black boxes and prefer lower-level building blocks, then our `RESTClient` is perfect for you!

    The `RESTClient` class offers a Pythonic interface for interacting with RESTful APIs, including features like:

    - automatic pagination,
    - various authentication mechanisms,
    - customizable request/response handling.

    ### What you’ll learn

    - How to authenticate with your API key
    - How to fetch paginated results using `RESTClient`
    - How to build a custom `@dlt.source`
    - How to run the pipeline and inspect the data

    For more information, read `dlt`'s official documentation for the [REST API Client](https://dlthub.com/devel/general-usage/http/rest-client).
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## **1. Creating a RESTClient instance**""")
    return


@app.cell
def _(BearerTokenAuth, HeaderLinkPaginator, RESTClient, dlt, os):
    os.environ["ACCESS_TOKEN"] = os.getenv("ACCESS_TOKEN")
    client = RESTClient(
        base_url="https://api.github.com",
        headers={"User-Agent": "MyApp/1.0"},
        auth=BearerTokenAuth(dlt.secrets["access_token"]),
        paginator=HeaderLinkPaginator(),
        data_selector="data",
    )
    client.get("repos/dlt-hub/dlt/issues").json()  # session=MyCustomSession()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    The `RESTClient` class is initialized with the following parameters:

    - `base_url`: The root URL of the API. All requests will be made relative to this URL.
    - `headers`: Default headers to include in every request. This can be used to set common headers like `User-Agent` or other custom headers.
    - `auth`: The authentication configuration. See the [Authentication](https://dlthub.com/docs/general-usage/http/rest-client#authentication) section for more details.
    - `paginator`: A paginator instance for handling paginated responses. See the [Paginators](https://dlthub.com/docs/general-usage/http/rest-client#paginators) section below.
    - `data_selector`: A [JSONPath selector](https://github.com/h2non/jsonpath-ng?tab=readme-ov-file#jsonpath-syntax) for extracting data from the responses. This defines a way to extract the data from the response JSON. Only used when paginating.
    - `session`: An optional session for making requests. This should be a [Requests session](https://requests.readthedocs.io/en/latest/api/#requests.Session) instance that can be used to set up custom request behavior for the client.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **2. Add authentication**

    The RESTClient supports various authentication strategies, such as bearer tokens, API keys, and HTTP basic auth, configured through the `auth` parameter of both the `RESTClient` and the `paginate()` method.

    The **available authentication methods** are defined in the `dlt.sources.helpers.rest_client.auth` module:

    - [BearerTokenAuth](https://dlthub.com/docs/devel/general-usage/http/rest-client#bearer-token-authentication)
    - [APIKeyAuth](https://dlthub.com/docs/devel/general-usage/http/rest-client#api-key-authentication)
    - [HttpBasicAuth](https://dlthub.com/docs/devel/general-usage/http/rest-client#http-basic-authentication)
    - [OAuth2ClientCredentials](https://dlthub.com/docs/devel/general-usage/http/rest-client#oauth-20-authorization)

    For specific use cases, you can [implement custom authentication](https://dlthub.com/docs/devel/general-usage/http/rest-client#implementing-custom-authentication) by subclassing the `AuthConfigBase` class from the [`dlt.sources.helpers.rest_client.auth`](https://github.com/dlt-hub/dlt/blob/devel/dlt/sources/helpers/rest_client/auth.py) module.
    For specific flavors of OAuth 2.0, you can [implement custom OAuth 2.0](https://dlthub.com/docs/devel/general-usage/http/rest-client#oauth-20-authorization) by subclassing `OAuth2ClientCredentials`.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_1_Custom_sources_RestAPI_source_and_RESTClient_img1](https://storage.googleapis.com/dlt-blog-images/dlt-advanced-course/Lesson_1_Custom_sources_RestAPI_source_and_RESTClient_img1.png)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### 📰 **NewsAPI overview**

    - **Base URL:** `https://newsapi.org/v2/`
    - **Authentication:** API key passed in query string as `apiKey`
    - **Documentation:** [NewsAPI Docs](https://newsapi.org/docs)

    | Endpoint          | Description                              | Auth Required | Response     |
    |-------------------|------------------------------------------|---------------|--------------|
    | `/everything`     | Search for news articles by query string | ✅ Yes        | JSON object with `articles[]` |
    | `/top-headlines`  | Latest headlines filtered by region/topic| ✅ Yes        | JSON object with `articles[]` |
    | `/sources`        | List of available news sources           | ✅ Yes        | JSON object with `sources[]`  |


    #### **Authentication Details:**

    To use the NewsAPI, you must register for a **free account** and obtain an API key. This key is required for all endpoints and must be included as a query parameter in your request:

    ```http
    GET /v2/everything?q=python&page=1&apiKey=YOUR_API_KEY
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Prerequisites:**

    To securely access the NewsAPI in your dlt project:

    1. **Sign up** at [https://newsapi.org/register](https://newsapi.org/register)
    2. Copy your **API key** from your dashboard
    3. Save your **API key** in Colab (or Molab) Secrets (side-bar on the right) as NEWS_API_KEY


    ### **How we chose the right authenticator for NewsAPI**

    NewsAPI uses a **simple API key-based scheme**. You sign up, get a key, and send it with every request.

    There are two supported ways to send this key:

    - In a **query string**, like `?apiKey=...`
    - Or in the **Authorization header**, as a Bearer token

    We are using the **query string method**, because:

    - It's supported on **all plans**, including the free tier
    - It's more transparent — you can inspect the request URL and see the key
    - It's easier to test manually in a browser or terminal


    **Using `APIKeyAuth` simplifies request setup**

    Instead of manually appending the key to every URL, we use dlt’s built-in `APIKeyAuth`:

    ```python
    APIKeyAuth(name="apiKey", api_key=api_key, location="query")
    ```

    This means:

    - `name="apiKey"` tells it what the key is called (NewsAPI expects `apiKey`)
    - `location="query"` means the key will be added to the URL as a query parameter:

      ```
      https://newsapi.org/v2/everything?q=python&apiKey=your_key
      ```
    """
    )
    return


@app.cell
def _(RESTClient, os):
    from dlt.sources.helpers.rest_client.auth import APIKeyAuth

    api_key = os.getenv("NEWS_API_KEY")
    news_api_client = RESTClient(
        base_url="https://newsapi.org/v2/",
        auth=APIKeyAuth(name="apiKey", api_key=api_key, location="query"),
    )
    response = news_api_client.get("everything", params={"q": "python", "page": 1})
    print(response.json())
    return APIKeyAuth, news_api_client


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""This authenticates every request by adding `?apiKey=your_key` to the URL."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **3. Add pagination**

    The `RESTClient` supports automatic pagination of API responses via the `paginate()` method, which can be customized using a built-in or custom paginator.

    You specify the paginator using the `paginator` parameter of the `RESTClient` or directly in the `paginate()` method.

    The **available pagination strategies** are defined in the `dlt.sources.helpers.rest_client.paginators` module and cover the most common pagination patterns used in REST APIs:

    - [`PageNumberPaginator`](https://dlthub.com/docs/general-usage/http/rest-client#pagenumberpaginator) – uses `page=N`, optionally with `pageSize` or `limit`
    - [`OffsetPaginator`](https://dlthub.com/docs/general-usage/http/rest-client#offsetpaginator) – uses `offset` and `limit`
    - [`JSONLinkPaginator`](https://dlthub.com/docs/general-usage/http/rest-client#jsonresponsepaginator) – follows a `next` URL in the response body
    - [`HeaderLinkPaginator`](https://dlthub.com/docs/general-usage/http/rest-client#headerlinkpaginator) – follows a `Link` header (used by GitHub and others)
    - [`JSONResponseCursorPaginator`](https://dlthub.com/docs/general-usage/http/rest-client#jsonresponsecursorpaginator) – uses a cursor from the response body

    Each paginator knows how to update the request to get the next page of results, and will continue until:

    - no more pages are available,
    - a configurable `maximum_page` or `maximum_offset` is reached,
    - or the API response is empty (depending on paginator behavior).


    > If a `paginator` is not specified, the `paginate()` method will attempt to **automatically detect** the pagination mechanism used by the API. If the API uses a standard pagination mechanism like having a `next` link in the response's headers or JSON body, the `paginate()` method will handle this automatically. Otherwise, you can specify a paginator object explicitly or implement a custom paginator.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **PageData**

    When using `client.paginate(...)` in dlt, you don’t just get a stream of data — each **page** returned is a rich object called `PageData`, and it gives you full access to the internals of the request, response, and pagination state.

    This is especially useful for **debugging**, **tracing**, or building custom logic.


    The `PageData` is a list-like object that contains the following attributes:

    - `request`: The original request object.
    - `response`: The response object.
    - `paginator`: The paginator object used to paginate the response.
    - `auth`: The authentication object used for the request.

    Let’s walk through an example.
    """
    )
    return


@app.cell
def _(news_api_client):
    page_iterator = news_api_client.paginate(
        "everything", params={"q": "python", "page": 1}
    )
    # prints the original request object
    print(next(page_iterator).request)
    page_iterator = news_api_client.paginate(
        "everything", params={"q": "python", "page": 1}
    )
    # prints the raw HTTP response
    print(next(page_iterator).response)
    page_iterator = news_api_client.paginate(
        "everything", params={"q": "python", "page": 1}
    )
    # prints the paginator that was used
    print(next(page_iterator).paginator)
    page_iterator = news_api_client.paginate(
        "everything", params={"q": "python", "page": 1}
    )
    # prints the authentication class used
    print(next(page_iterator).auth)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Log Warning explained**

    ```
    [WARNING] Fallback paginator used: SinglePagePaginator...
    ```

    This warning means:

    - dlt tried to guess the pagination method but failed
    - It will make only **one request**
    - You won’t get multiple pages of data unless you configure a paginator explicitly
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **Question 1:**


    Which paginator is used by `news_api_client.paginate()` by default in the example above?


    >Answer this question and select the correct option in the homework Google Form.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **How we chose the right paginator for NewsAPI**

    When using `RESTClient` to extract data from paginated APIs, one of the first decisions you must make is:
    **"What type of pagination does this API use?"**
    This determines which paginator to plug into the client.

    ---

    **Step 1: Read the API docs**

    From the [NewsAPI documentation](https://newsapi.org/docs/endpoints/everything), we learn:

    - Pagination is done via two query parameters:
      - `page` → page number (starts at 1)
      - `pageSize` → how many articles per page (max 100)
    - Example request:
      ```
      GET /v2/everything?q=bitcoin&page=2&pageSize=20&apiKey=...
      ```

    There is **no "next" URL**, no cursor, no `offset`.

    This is **classic page-number pagination.**

    ---

    **Step 2: Understand response behavior**

    Each response includes:

    ```json
    {
      "status": "ok",
      "totalResults": 1532,
      "articles": [ ... ]
    }
    ```

    But:
    - The API **does not tell us how many total pages exist**.
    - We only know how many total results there are.

    So we either:
    - Compute total pages: `ceil(totalResults / pageSize)`
      *(But that requires looking into the first page’s body)*
    - **Or we keep requesting pages until we get an empty list.**

    ---

    **Step 3: Choose `PageNumberPaginator`**

    This is exactly what `PageNumberPaginator` is made for:
    """
    )
    return


@app.cell
def _(APIKeyAuth, RESTClient, os):
    from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

    api_key_1 = os.getenv("NEWS_API_KEY")
    another_client = RESTClient(
        base_url="https://newsapi.org/v2/",
        auth=APIKeyAuth(name="apiKey", api_key=api_key_1, location="query"),
        paginator=PageNumberPaginator(
            base_page=1,
            page_param="page",
            total_path=None,
            stop_after_empty_page=True,
            maximum_page=4,
        ),
    )
    for page in another_client.paginate(
        "everything", params={"q": "python", "pageSize": 5, "language": "en"}
    ):
        for article in page:
            print(
                article["title"]
            )  # NewsAPI starts paging from 1  # Matches the API spec  # Set it to None explicitly  # Stop if no articles returned  # Optional limit for dev/testing
    return PageNumberPaginator, api_key_1


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **4. Wrap into a dlt Resource**

    Let’s turn this into a dlt pipeline resource:
    """
    )
    return


@app.cell
def _(
    APIKeyAuth,
    Iterator,
    PageNumberPaginator,
    RESTClient,
    TDataItems,
    dlt,
    os,
):
    dlt.secrets["NEWS_API_KEY"] = os.getenv("NEWS_API_KEY")

    @dlt.resource(write_disposition="replace", name="python_articles")
    def get_articles(news_api_key: str = dlt.secrets.value) -> Iterator[TDataItems]:
        client = RESTClient(
            base_url="https://newsapi.org/v2/",
            auth=APIKeyAuth(name="apiKey", api_key=news_api_key, location="query"),
            paginator=PageNumberPaginator(
                base_page=1,
                page_param="page",
                total_path=None,
                stop_after_empty_page=True,
                maximum_page=4,
            ),
        )
        for page in client.paginate(
            "everything", params={"q": "python", "pageSize": 5, "language": "en"}
        ):
            yield page

    return (get_articles,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## **5. Add `top-headlines` Resource**""")
    return


@app.cell
def _(
    APIKeyAuth,
    Iterator,
    PageNumberPaginator,
    RESTClient,
    TDataItems,
    api_key_1,
    dlt,
    os,
):
    dlt.secrets["NEWS_API_KEY"] = os.getenv("NEWS_API_KEY")

    @dlt.resource(write_disposition="replace", name="top_articles")
    def get_top_articles(news_api_key: str = dlt.secrets.value) -> Iterator[TDataItems]:
        client = RESTClient(
            base_url="https://newsapi.org/v2/",
            auth=APIKeyAuth(name="apiKey", api_key=api_key_1, location="query"),
            paginator=PageNumberPaginator(
                base_page=1,
                page_param="page",
                total_path=None,
                stop_after_empty_page=True,
                maximum_page=4,
            ),
        )
        for page in client.paginate(
            "top-headlines", params={"pageSize": 5, "language": "en"}
        ):
            yield page

    return (get_top_articles,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **6. Create a reusable Source**

    Now bundle both resources into a single `@dlt.source`:
    """
    )
    return


@app.cell
def _(DltResource, Iterable, dlt, get_articles, get_top_articles):
    @dlt.source
    def newsapi_source(news_api_key: str = dlt.secrets.value) -> Iterable[DltResource]:
        return [get_articles(news_api_key), get_top_articles(news_api_key)]

    return (newsapi_source,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## **7. Run the pipeline**""")
    return


@app.cell
def _(dlt, newsapi_source):
    pipeline_1 = dlt.pipeline(
        pipeline_name="newsapi_pipeline", destination="duckdb", dataset_name="news_data"
    )
    info = pipeline_1.run(newsapi_source())
    print(info)
    return (pipeline_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## **8. Explore data**""")
    return


@app.cell
def _(pipeline_1):
    pipeline_1.dataset().python_articles.df().head()
    return


@app.cell
def _(pipeline_1):
    pipeline_1.dataset().top_articles.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # **Create custom source using `dlt` and [`rest_api` source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic)**

    `rest_api` is a generic source that you can use to create a `dlt` source from a REST API using a declarative configuration. The majority of REST APIs behave in a similar way; this `dlt` source attempts to provide a declarative way to define a `dlt` source for those APIs.

    Using a [declarative configuration](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic#source-configuration), you can define:

    - the API endpoints to pull data from,
    - their relationships,
    - how to handle pagination,
    - authentication,
    - data transformation,
    - incremental loading.

    dlt will take care of the rest: **unnesting the data, inferring the schema**, etc., and **writing to the destination**

    In the previous section, you've already learned about the Rest API Client. `dlt`’s **[RESTClient](https://dlthub.com/docs/general-usage/http/rest-client)** is the **low level abstraction** that powers the REST API Source.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **What you’ll learn**

    This section will teach you how to create a reusable, authenticated, and paginated pipeline using the `rest_api_source` module in dlt. Our example will use the [NewsAPI](https://newsapi.org), which provides access to thousands of news articles via a REST API.

    We'll walk step-by-step through:
    - Setting up the source configuration
    - Authenticating with an API key
    - Configuring pagination
    - Building a working `dlt` pipeline
    - Inspecting and transforming the response
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Reminder: **About NewsAPI**

    - **Base URL:** `https://newsapi.org/v2/`
    - **Authentication:** API key passed in query string as `apiKey`
    - **Documentation:** [NewsAPI Docs](https://newsapi.org/docs)

    | Endpoint          | Description                              | Auth Required | Response     |
    |-------------------|------------------------------------------|---------------|--------------|
    | `/everything`     | Search for news articles by keyword      | ✅ Yes        | JSON with `articles[]` |
    | `/top-headlines`  | Latest headlines filtered by region/topic| ✅ Yes        | JSON with `articles[]` |
    | `/sources`        | List of available news sources           | ✅ Yes        | JSON with `sources[]`  |

    To access the API, register for a **free account** at [newsapi.org](https://newsapi.org/register) and copy your personal API key.

    Add this key to your Colab secrets.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **1. Define the source configuration**

    We'll now build the complete configuration step-by-step. This gives you control over authentication, pagination, filters, and even incremental loading.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **RESTAPIConfig**

    The central object when working with the `rest_api_source` is the `RESTAPIConfig`. This is a declarative Python dictionary that tells dlt everything it needs to know about the API you are connecting to.

    It defines:
    - how to connect to the API (base URL, authentication)
    - what endpoints to call (resources)
    - how to paginate
    - how to filter or sort the data
    - how to extract the actual data from responses

    ```python
    import dlt
    from dlt.sources.rest_api import rest_api_source

    # Define config
    news_config = {
        "client": {
            "base_url": ...,
            "auth": ...
        },
        "resources": [
                ...
        ]
    }

    # Create source
    news_source = rest_api_source(news_config)

    # Create pipeline
    pipeline = dlt.pipeline(
      pipeline_name="news_pipeline",
      destination="duckdb",
      dataset_name="news"
    )

    # Run it
    load_info = pipeline.run(news_source)
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    You can start with just these fields and then add pagination, schema hints, transformations, and more as needed.

    To extract data from a REST API using `dlt`, we define a configuration dictionary that follows the `RESTAPIConfig` structure.
    This configuration describes:

    - how to connect to the API (base URL, headers, auth)
    - what resources to extract (endpoints)
    - how to paginate, filter, and process responses

    At a high level, the configuration has two required keys:
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **`client`**
    This defines the shared connection details for all requests:
    - `base_url`: The root URL for the API
    - `auth`: (Optional) Authentication method to use — such as API key or token
    - `headers`: (Optional) Custom headers for requests
    - `paginator`: (Optional) Default paginator for all resources
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **`resources`**
    A list of resource definitions. Each resource becomes a table in your destination.
    A resource includes:
    - `name`: Table name for the resource
    - `endpoint`: Path to the endpoint, query parameters, pagination config
    - `write_disposition`: How to load the data (`append`, `merge`, `replace`)
    - `primary_key`: Optional key used when merging
    - `data_selector`: JSONPath to extract data from the response (e.g., "articles")
    - `processing_steps`: Optional filters and transformations
    - `response_actions`: Optional hooks to inspect or alter the HTTP response

    Let’s build a real-world configuration step-by-step using NewsAPI.
    """
    )
    return


@app.cell
def _(RESTAPIConfig, dlt, rest_api_source):
    _news_config: RESTAPIConfig = {
        "client": {"base_url": "https://newsapi.org/v2/"},
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {"path": "everything", "params": {"q": "python"}},
            }
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_2 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_2.run(_news_source)
    print(pipeline_2.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Question 2:

    What error was thrown in the example above?

    >Answer this question and select the correct option in the homework Google Form.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **2. Add authentication**

    NewsAPI requires an API key to be sent with every request. We use dlt's built-in `api_key` authentication method, which places the key into the query string automatically:

    ```python
    "auth": {
        "type": "api_key",
        "name": "apiKey",
        "api_key": "your_key",
        "location": "query",
    }
    ```

    This ensures every request has `?apiKey=...` added. It's simple and secure, especially when storing the key in ENVs or Colab or Molab's secret manager.


    The available authentication methods you can find in [dlt documentation](https://dlthub.com/docs/general-usage/http/rest-client#authentication).
    """
    )
    return


@app.cell
def _(dlt, os, rest_api_source):
    api_key_2 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_2,
                "location": "query",
            },
        },
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {"path": "everything", "params": {"q": "python"}},
            }
        ],
    }
    _news_source = rest_api_source(_news_config)
    another_pipeline = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    another_pipeline.run(_news_source)
    print(another_pipeline.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **3. Add pagination**

    The REST API source will try to automatically handle pagination for you. This works by detecting the pagination details from the first API response. Unfortunately, it doesn't work for NewsAPI.

    NewsAPI uses page-based pagination. We use the built-in `PageNumberPaginator` to automatically paginate through pages until results run out:


    ```python
    "paginator": {
        "type": "page_number",
        "page_param": "page",
        "stop_after_empty_page": True,
        "total_path": None,
        "maximum_page": 3,
    },
    ```

    This will fetch up to 3 pages of results, stopping early if a page is empty.
    """
    )
    return


@app.cell
def _(dlt, os, rest_api_source):
    api_key_3 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_3,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {"path": "everything", "params": {"q": "python"}},
            }
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_3 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_3.run(_news_source)
    print(pipeline_3.last_trace)
    return (pipeline_3,)


@app.cell
def _(pipeline_3):
    pipeline_3.dataset().news_articles.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **4. Add order, filtering via params**
    We can filter articles using query parameters supported by NewsAPI:

    ```python
    "params": {
        "q": "python",
        "language": "en",
        "pageSize": 20,
    },
    ```

    - `q`: search keyword (e.g. "python")
    - `language`: filter by article language
    - `pageSize`: number of articles per page (max 100)
    """
    )
    return


@app.cell
def _(dlt, os, rest_api_source):
    api_key_4 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_4,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {
                    "path": "everything",
                    "params": {"q": "python", "language": "en", "pageSize": 20},
                },
            }
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_4 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_4.run(_news_source)
    print(pipeline_4.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## **5. Incremental loading**

    Although NewsAPI does not support true incremental loading via cursors, you can simulate it using the `from` or `to` date filters and dlt's `incremental` loader:

    ```python
    "from": {
        "type": "incremental",
        "cursor_path": "publishedAt",
        "initial_value": "2024-01-01T00:00:00Z",
    },
    ```

    This setup means:
    - dlt will remember the last `publishedAt` seen
    - On the next run, it will only request articles newer than that

    This is optional and depends on your usage pattern.
    """
    )
    return


@app.cell
def _(dlt, os, rest_api_source):
    api_key_5 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_5,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {
                    "path": "everything",
                    "params": {
                        "q": "python",
                        "language": "en",
                        "pageSize": 20,
                        "from": {
                            "type": "incremental",
                            "cursor_path": "publishedAt",
                            "initial_value": "2025-04-15T00:00:00Z",
                        },
                    },
                },
            }
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_5 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_5.run(_news_source)
    print(pipeline_5.last_trace)
    pipeline_5.run(_news_source)
    print(pipeline_5.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## **6. Add more endpoints**""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Set defaults

    First, set some defaults for all endpoints:

    ```python
    "resource_defaults": {
        "primary_key": "id",
        "write_disposition": "merge",
        "endpoint": {
            "params": {
                "per_page": 100,
            },
        },
    },
    ```
    """
    )
    return


@app.cell
def _(dlt, os, rest_api_source):
    api_key_6 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_6,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {"params": {"language": "en", "pageSize": 20}},
        },
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {
                    "path": "everything",
                    "params": {
                        "q": "python",
                        "from": {
                            "type": "incremental",
                            "cursor_path": "publishedAt",
                            "initial_value": "2025-04-15T00:00:00Z",
                        },
                    },
                },
            }
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_6 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_6.run(_news_source)
    print(pipeline_6.last_trace)
    pipeline_6.run(_news_source)
    print(pipeline_6.last_trace)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    `resource_defaults` contains the default values to configure the dlt resources returned by this source.

    `resources` object contains the configuration for each resource.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Add same level endpoint

    To load additional endpoints like `/top-headlines` or `/sources`, you can simply add more entries to the `resources` list:
    ```python
    {
      "name": "top_headlines",
      "endpoint": {
        "path": "top-headlines",
        "params": {"country": "us", "pageSize": 10},
        "paginator": {"type": "page_number", "page_param": "page"}
      },
      "primary_key": "url",
      "write_disposition": "append",
      "data_selector": "articles"
    }
    ```
    """
    )
    return


@app.cell
def _(dlt, os, rest_api_source):
    api_key_7 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_7,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {"params": {"language": "en", "pageSize": 20}},
        },
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {
                    "path": "everything",
                    "params": {
                        "q": "python",
                        "from": {
                            "type": "incremental",
                            "cursor_path": "publishedAt",
                            "initial_value": "2025-04-15T00:00:00Z",
                        },
                    },
                },
            },
            {
                "name": "top_headlines",
                "endpoint": {"path": "top-headlines", "params": {"country": "us"}},
            },
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_7 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_7.run(_news_source)
    print(pipeline_7.last_trace)
    pipeline_7.dataset().top_headlines.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Advanced""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Response actions

    The `response_actions` field in the endpoint configuration allows you to specify how to **handle specific responses** or all responses from the API.

    For example:
    - Responses with specific status codes or content substrings can be ignored.
    - All responses or only responses with specific status codes or content substrings can be transformed with a custom callable, such as a function. This callable is passed on to the requests library as a response hook. The callable can modify the response object and has to return it for the modifications to take effect.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ```python
    "resources": [
        {
            "name": "news_articles",
            "endpoint": {
                "path": "everything",
                "response_actions": [
                    {
                        "status_code": 200,
                        "content": "some text",
                        "action": do_something,
                    },
                ],
            },
        },
    ```

    Fields:

    * `status_code` (int, optional): The HTTP status code to match.
    * `content` (str, optional): A substring to search for in the response content.
    * `action` (str or Callable or List[Callable], optional): The action to take when the condition is met. Currently supported actions:
    "ignore": Ignore the response.
    a callable accepting and returning the response object.
    a list of callables, each accepting and returning the response object.
    """
    )
    return


@app.cell
def _(Any):
    from dlt.sources.helpers.requests import Response

    def debug_response(response: Response, *args: Any, **kwargs: Any) -> Response:
        print("Intercepted:", response.status_code)
        return response

    return (debug_response,)


@app.cell
def _(debug_response, dlt, os, rest_api_source):
    api_key_8 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_8,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {"params": {"language": "en", "pageSize": 20}},
        },
        "resources": [
            {
                "name": "news_articles",
                "endpoint": {
                    "path": "everything",
                    "response_actions": [
                        {"status_code": 200, "action": debug_response}
                    ],
                    "params": {
                        "q": "python",
                        "from": {
                            "type": "incremental",
                            "cursor_path": "publishedAt",
                            "initial_value": "2025-04-15T00:00:00Z",
                        },
                    },
                },
            },
            {
                "name": "top_headlines",
                "endpoint": {"path": "top-headlines", "params": {"country": "us"}},
            },
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_8 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_8.run(_news_source)
    print(pipeline_8.last_trace)
    pipeline_8.dataset().news_articles.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Processing steps: filter and transform data
    The `processing_steps` field in the resource configuration allows you to **apply transformations** to the data fetched from the API before it is loaded into your destination.

    This is useful when you need to
    - **filter out** certain records,
    - **modify the data** structure,
    - **anonymize** sensitive information.

    Each processing step is a dictionary specifying the type of operation (filter or map) and the function to apply. Steps apply in the order they are listed.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ```python
     "resources": [
            {
                "name": "news_articles",
                "processing_steps": [
                    {"filter": lambda x: len(x["author"]) > 0},
                    {"map": lower_title},
                ],
            },
        ],
    ```
    """
    )
    return


@app.cell
def _(Any):
    def lower_title(record: dict[str, Any]) -> dict[str, Any]:
        record["title"] = str(record["title"]).lower()
        return record

    return (lower_title,)


@app.cell
def _(debug_response, dlt, lower_title, os, rest_api_source):
    api_key_9 = os.getenv("NEWS_API_KEY")
    _news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key_9,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {"params": {"language": "en", "pageSize": 20}},
        },
        "resources": [
            {
                "name": "news_articles",
                "processing_steps": [
                    {"filter": lambda x: len(x["author"]) > 0},
                    {"map": lower_title},
                ],
                "endpoint": {
                    "path": "everything",
                    "response_actions": [
                        {"status_code": 200, "action": debug_response}
                    ],
                    "params": {
                        "q": "python",
                        "from": {
                            "type": "incremental",
                            "cursor_path": "publishedAt",
                            "initial_value": "2025-04-15T00:00:00Z",
                        },
                    },
                },
            },
            {
                "name": "top_headlines",
                "endpoint": {"path": "top-headlines", "params": {"country": "us"}},
            },
        ],
    }
    _news_source = rest_api_source(_news_config)
    pipeline_9 = dlt.pipeline(
        pipeline_name="news_pipeline", destination="duckdb", dataset_name="news"
    )
    pipeline_9.run(_news_source)
    print(pipeline_9.last_trace)
    pipeline_9.dataset().news_articles.df().head()
    return (pipeline_9,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Links

    More Information about how to build efficient data pipelines you can find in our official documentation:
    - `dlt` [Getting Started](https://dlthub.com/docs/getting-started),
    - [REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api),
    - [REST API Client](https://dlthub.com/docs/general-usage/http/rest-client),
    - `dlt` [Sources](https://dlthub.com/docs/general-usage/source) and [Resources](https://dlthub.com/docs/general-usage/resource),
    - [Incremental loading](https://dlthub.com/docs/general-usage/incremental-loading),
    - Our pre-built [Verified Sources](https://dlthub.com/docs/dlt-ecosystem/verified-sources/),
    - Available [Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/).
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_1_Custom_sources_RestAPI_source_and_RESTClient_img2](https://storage.googleapis.com/dlt-blog-images/dlt-advanced-course/Lesson_1_Custom_sources_RestAPI_source_and_RESTClient_img2.jpeg)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Exercise 1

    Your task is to create a `rest_api_source` configuration for the public **Jaffle Shop API**. This exercise will help you apply what you’ve learned:

    ### API details:
    - **Base URL:** `https://jaffle-shop.scalevector.ai/api/v1`
    - **Docs:** [https://jaffle-shop.scalevector.ai/docs](https://jaffle-shop.scalevector.ai/docs)

    ### Endpoints to load:
    - `/orders`

    ### Requirements:
    1. Use `rest_api_source` to define your source config.
    2. This API uses **pagination**. Figure out what type it is.
    3. Add incremental loading to `orders`, starting from `2017-08-01` and using `ordered_at` as the cursor.
    4. Add `processing_steps` to `orders`:
      - Remove records from orders for which it is true that `order_total` > 500.



    ### Question:
    How many rows does the resulting table `orders` contain?
    """
    )
    return


@app.cell
def _(pipeline_9):
    pipeline_9.dataset().orders.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""✅ ▶ Well done! Go to [the next lesson.](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_2_custom_sources_sql_databases_.ipynb)"""
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
