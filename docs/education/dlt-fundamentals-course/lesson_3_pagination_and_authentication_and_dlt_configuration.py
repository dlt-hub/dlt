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
        r"""
    # **Recap of [Lesson 2](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_2_dlt_sources_and_resources_create_first_dlt_pipeline.ipynb) 👩‍💻🚀**

    1. Used `@dlt.resource` to load and query data such as lists, dataframes, and REST API responses into DuckDB.
    2. Grouped multiple resources into a single `@dlt.source` for better organization and efficiency.
    3. Used `@dlt.transformer` to process and enrich data between resources.

    Next: We'll dive deeper into building dlt pipelines using pagination, authentication, and dlt configuration! 🚀
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---

    # **Pagination & Authentication & dlt Configuration** 🤫🔩   [![Open in molab](https://marimo.io/molab-shield.svg)](https://molab.marimo.io/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_3_pagination_and_authentication_and_dlt_configuration.py) [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_3_pagination_and_authentication_and_dlt_configuration.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_3_pagination_and_authentication_and_dlt_configuration.ipynb)



    **In this lesson, you will learn how to:**
    - Use pagination for REST APIs.
    - Use environment variables to manage both secrets & configs.
    - Add values to `secrets.toml` or `config.toml`.

    To learn more about credentials, refer to the [dlt documentation](https://dlthub.com/docs/general-usage/credentials/).
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""In the previous lesson, we loaded data from the GitHub API to DuckDB,""")
    return


@app.cell
def _():
    import dlt
    from dlt.sources.helpers import requests
    from dlt.common.typing import TDataItems

    @dlt.resource
    # define dlt resources
    def github_events() -> TDataItems:
        url = "https://api.github.com/orgs/dlt-hub/events"
        _response = requests.get(url)
        yield _response.json()

    _pipeline = dlt.pipeline(destination="duckdb")
    _load_info = _pipeline.run(github_events)
    print(_load_info)
    # define dlt pipeline
    # run dlt pipeline
    # explore loaded data
    _pipeline.dataset().github_events.df()
    return TDataItems, dlt, requests


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    You may notice we received only one page — just 30 records — even though this endpoint has many more.

    To fetch everything, enable pagination: many APIs (like GitHub) return results in pages and limit how much you can retrieve per request, so paginating lets you iterate through all pages to collect the full dataset.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img1](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img1.webp)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---
    ## **Pagination**

    GitHub provides excellent documentation, making it easy to find the relevant section on [Pagination.](https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2022-11-28)

    It explains that:

    >You can use the `Link` header from the response to request additional pages of data.

    >The `Link` header contains URLs that let you fetch other pages of results — for example, the previous, next, first, and last pages.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **GitHub API Pagination Example**

    The GitHub API provides the `per_page` and `page` query parameters:

    * `per_page`: The number of records per page (up to 100).
    * `page`: The page number to retrieve.
    """
    )
    return


@app.cell
def _(requests):
    _response = requests.get(
        "https://api.github.com/orgs/dlt-hub/events?per_page=10&page=1"
    )
    _response.headers
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Got it! We can see the `Link` field in the response headers. Alternatively, you can access it directly using `response.links`:"""
    )
    return


@app.cell
def _(requests):
    _response = requests.get(
        "https://api.github.com/orgs/dlt-hub/events?per_page=10&page=1"
    )
    _response.links
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **dlt RESTClient**

    Now that we know how pagination works conceptually, let’s see how to implement it efficiently!

    When working with APIs, you could implement pagination using only Python and the `requests` library. While this approach works, it often requires writing boilerplate code for tasks like managing authentication, constructing URLs, and handling pagination logic.

    Learn more about building pagination with Python and `requests`:

    * [Link 1](https://farnamdata.com/api-pagination)

    * [Link 2](https://www.klamp.io/blog/python-requests-pagination-for-efficient-data-retrieval)

    **But!** In this lesson, we’re going to use dlt's **[RESTClient](https://dlthub.com/docs/general-usage/http/rest-client)** to handle pagination seamlessly when working with REST APIs like GitHub.


    **Why use RESTClient?**

    RESTClient is part of dlt's helpers, making it easier to interact with REST APIs by managing repetitive tasks such as:

    * Authentication
    * Query parameter handling
    * Pagination

    This reduces boilerplate code and lets you focus on your data pipeline logic.

    **Here’s how to fetch paginated data:**
    1. Import `RESTClient`
    2. Create a `RESTClient` instance
    3. Use the `paginate` method to iterate through all pages of data
    """
    )
    return


@app.cell
def _():
    from dlt.sources.helpers.rest_client import RESTClient

    client = RESTClient(base_url="https://api.github.com")
    for _page in client.paginate("orgs/dlt-hub/events"):
        print(_page)
    return (RESTClient,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""☝️ The pagination type was detected automatically, but you can also specify it explicitly:"""
    )
    return


@app.cell
def _(RESTClient):
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

    client_1 = RESTClient(
        base_url="https://api.github.com", paginator=HeaderLinkPaginator()
    )
    return (client_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""The full list of available paginators is in the official [dlt documentation](https://dlthub.com/docs/general-usage/http/rest-client#paginators)."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img2](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img2.png)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    The events endpoint doesn’t contain as much data, especially compared to the stargazers endpoint of the dlt repository.

    If you run the pipeline for the stargazers endpoint, there's a high chance that you'll face a **rate limit error**.
    """
    )
    return


@app.cell
def _(client_1):
    for _page in client_1.paginate("repos/dlt-hub/dlt/stargazers"):
        print(_page)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **Exercise 1: Pagination with RESTClient**
    Explore the cells above and answer the question below.
    #### Question
    What type of pagination should we use for the GitHub API?
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---
    ## **Authentication**

    To avoid the **rate limit error** you can use [GitHub API Authentication](https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api?apiVersion=2022-11-28):

    1. Login to your GitHub account.
    2. Generate an [API token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) (classic).
    2.  Use it as an access token for the GitHub API.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    > **! ATTENTION !**
    > Never share your credentials publicly and never hard-code them in your code. Use **environment variables, files** or dlt's **secrets.toml**.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Create an environment variable for your access token in Colab.

    ![Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img3](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img3.webp)
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""In Molab, simply click on the `Secrets` section in the left-side menu and add your access token."""
    )
    return


@app.cell
def _():
    import os

    access_token = os.getenv("SECRET_KEY")
    return access_token, os


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Use the `access_token` variable in the code below:""")
    return


@app.cell
def _(RESTClient, access_token):
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

    client_2 = RESTClient(
        base_url="https://api.github.com", auth=BearerTokenAuth(token=access_token)
    )
    for _page in client_2.paginate("repos/dlt-hub/dlt/stargazers"):
        print(_page)
        break
    return (BearerTokenAuth,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Let's rewrite our GitHub dlt pipeline using the RestAPI Client and the `access_token`."""
    )
    return


@app.cell
def _(BearerTokenAuth, RESTClient, TDataItems, access_token, dlt):
    @dlt.resource
    def github_stargazers() -> TDataItems:
        client = RESTClient(
            base_url="https://api.github.com", auth=BearerTokenAuth(token=access_token)
        )
        for _page in client.paginate("repos/dlt-hub/dlt/stargazers"):
            yield _page

    _pipeline = dlt.pipeline(destination="duckdb")
    _load_info = _pipeline.run(github_stargazers)
    print(_load_info)
    _pipeline.dataset().github_stargazers.df()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""You can see that all dlt [stargazers](https://github.com/dlt-hub/dlt/stargazers) were loaded into the DuckDB destination."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---
    ## **dlt configuration and secrets**

    In dlt, [configurations and secrets](https://dlthub.com/docs/general-usage/credentials/) are essential for setting up data pipelines.

    **Configurations** are **non-sensitive** settings that define the behavior of a data pipeline, including file paths, database hosts, timeouts, API URLs, and performance settings.

    On the other hand, **secrets** are **sensitive** data like passwords, API keys, and private keys, which should never be hard-coded to avoid security risks.

    Both can be set up in various ways:

    * As environment variables
    * Within code using `dlt.secrets` and `dlt.config`
    * Via configuration files (`secrets.toml` and `config.toml`)

    > **Note**: While you can store both configurations and credentials in `dlt.secrets` (or `secrets.toml`) if that’s more convenient, credentials cannot be placed in `dlt.config` (or `config.toml`) because dlt does not read them from there.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Let's create a dlt pipeline for both endpoints: `repos/dlt-hub/dlt/stargazers` and `orgs/dlt-hub/events`.

    We'll use `@dlt.source` to group both resources.
    """
    )
    return


@app.cell
def _(BearerTokenAuth, RESTClient, TDataItems, access_token, dlt):
    from typing import Iterable
    from dlt.extract import DltResource

    @dlt.source
    def github_source() -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com", auth=BearerTokenAuth(token=access_token)
        )

        @dlt.resource
        def github_events() -> TDataItems:
            for _page in client.paginate("orgs/dlt-hub/events"):
                yield _page

        @dlt.resource
        def github_stargazers() -> TDataItems:
            for _page in client.paginate("repos/dlt-hub/dlt/stargazers"):
                yield _page

        return (github_events, github_stargazers)

    return DltResource, Iterable


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Now, we'll use `dlt.secrets.value` in our source, enabling dlt's automatic secrets resolution. Note that we first reset all environment variables to demonstrate what happens if dlt tries to resolve a non-existing variable:"""
    )
    return


@app.cell
def _(os):
    os.environ.clear()
    return


@app.cell
def _(BearerTokenAuth, DltResource, Iterable, RESTClient, TDataItems, dlt):
    @dlt.source
    def github_source_1(access_token=dlt.secrets.value) -> Iterable[DltResource]:
        client = RESTClient(
            base_url="https://api.github.com", auth=BearerTokenAuth(token=access_token)
        )

        @dlt.resource
        def github_events() -> TDataItems:
            for _page in client.paginate("orgs/dlt-hub/events"):
                yield _page

        @dlt.resource
        def github_stargazers() -> TDataItems:
            for _page in client.paginate("repos/dlt-hub/dlt/stargazers"):
                yield _page

        return (github_events, github_stargazers)

    return (github_source_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""> Configs are defined in a similar way but are accessed using `dlt.config.value`. However, since configuration variables are internally managed by `dlt`, it is unlikely that you would need to explicitly use `dlt.config.value` in most cases."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""If you now run the pipeline, you will see the following error:""")
    return


@app.cell
def _(dlt, github_source_1):
    _pipeline = dlt.pipeline(destination="duckdb")
    _load_info = _pipeline.run(github_source_1())
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    That’s what happens when you use `dlt.secrets.value` for a variable in your pipeline but haven’t actually set the secret value.

    When this occurs, dlt searches for the missing secret across different possible locations and naming formats, as shown below:

    ```python
    ConfigFieldMissingException: Following fields are missing: ['access_token'] in configuration with spec GithubSourceConfiguration
    	for field "access_token" config providers and keys were tried in following order:
    		In Environment Variables key DLT_COLAB_KERNEL_LAUNCHER__SOURCES____MAIN____GITHUB_SOURCE__ACCESS_TOKEN was not found.
    		In Environment Variables key DLT_COLAB_KERNEL_LAUNCHER__SOURCES____MAIN____ACCESS_TOKEN was not found.
    		In Environment Variables key DLT_COLAB_KERNEL_LAUNCHER__SOURCES__ACCESS_TOKEN was not found.
    		In Environment Variables key DLT_COLAB_KERNEL_LAUNCHER__ACCESS_TOKEN was not found.
    		In Environment Variables key SOURCES____MAIN____GITHUB_SOURCE__ACCESS_TOKEN was not found.
    		In Environment Variables key SOURCES____MAIN____ACCESS_TOKEN was not found.
    		In Environment Variables key SOURCES__ACCESS_TOKEN was not found.
    		In Environment Variables key ACCESS_TOKEN was not found.
    WARNING: dlt looks for .dlt folder in your current working directory and your cwd (/content) is different from directory of your pipeline script (/usr/local/lib/python3.10/dist-packages).
    If you keep your secret files in the same folder as your pipeline script but run your script from some other folder, secrets/configs will not be found
    Please refer to https://dlthub.com/docs/general-usage/credentials for more information
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    To define the `access_token` secret value, we can use (as mentioned earlier):

    1. `dlt.secrets` in code (recommended for secret vaults or dynamic creds)
    2. Environment variables (recommended for prod)
    3. `secrets.toml` file (recommended for local dev)
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **Use `dlt.secrets` in code**

    You can easily set or update your secrets directly in Python code. This is especially convenient when retrieving credentials from third-party secret managers or when you need to update secrets and configurations dynamically.
    """
    )
    return


@app.cell
def _(dlt, github_source_1, os):
    dlt.secrets["access_token"] = os.getenv("SECRET_KEY")
    github_pipeline = dlt.pipeline(destination="duckdb")
    _load_info = github_pipeline.run(github_source_1())
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Alternatively, you can set:

    ```python
    dlt.secrets["sources.access_token"] = userdata.get('SECRET_KEY')
    dlt.secrets["sources.____main____.access_token"] = userdata.get('SECRET_KEY')
    dlt.secrets["sources.____main____.github_source.access_token"] = userdata.get('SECRET_KEY')
    ...
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    * `sources` is a special word;

    * `__main__` is a python module name;

    * `github_source` is the resource name;

    * `access_token` is the secret variable name.


    So dlt looks for secrets according to this hierarchy:
    ```
    pipeline_name
        |
        |-sources
            |
            |-<module name>
                |
                |-<source function 1 name>
                    |
                    |- secret variable 1
                    |- secret variable 2
    ```

    To keep the **naming convention** flexible, dlt looks for a lot of **possible combinations** of key names, starting from the most specific possible path. Then, if the value is not found, it removes the right-most section and tries again.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **Exercise 2: Run a pipeline with `dlt.secrets.value`**

    Explore the cells above and answer the question below using `sql_client`.

    #### Question

    Who has id=`17202864` in the `stargazers` table? Use `sql_client`.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ###  **Use environment variables**

    Let's explicitly set the environment variable for our access token in one of the formats dlt accepts: `ACCESS_TOKEN`.
    """
    )
    return


@app.cell
def _(dlt, github_source_1, os):
    os.environ["ACCESS_TOKEN"] = os.getenv("SECRET_KEY")
    _pipeline = dlt.pipeline(destination="duckdb")
    _load_info = _pipeline.run(github_source_1())
    print(_load_info)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Alternatively, you can set:

    > `userdata.get()` is Colab-specific.

    ```python
    os.environ["SOURCES__ACCESS_TOKEN"] = userdata.get('SECRET_KEY')
    os.environ["SOURCES____MAIN____ACCESS_TOKEN"] = userdata.get('SECRET_KEY')
    os.environ["SOURCES____MAIN____GITHUB_SOURCE__ACCESS_TOKEN"] = userdata.get('SECRET_KEY')
    ...
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **How does it work?**

    `dlt` **automatically extracts** configuration settings and secrets based on flexible naming conventions.

    It then **injects** these values where needed in functions decorated with `@dlt.source`, `@dlt.resource`, or `@dlt.destination`.


    >dlt uses a specific naming hierarchy to search for the secrets and config values. This makes configurations and secrets easy to manage.
    >
    > The naming convention for **environment variables** in dlt follows a specific pattern. All names are **capitalized** and sections are separated with **double underscores** __ , e.g.  `SOURCES____MAIN____GITHUB_SOURCE__SECRET_KEY`.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""###  **Use dlt `secrets.toml` or `config.toml`**""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""> Note that Colab is not well-suited for using `secrets.toml` or `config.toml` files. As a result, these sections will provide instructions rather than code cells, detailing how to use them in a local environment. You should test this functionality on your own machine. For Colab, it is recommended to use environment variables instead."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    The `secrets.toml` file - along with the `config.toml` file - should be stored in the `.dlt` directory where your pipeline code is located:

    ```
    /your_project_directory
    │
    ├── .dlt
    │   ├── secrets.toml
    │   └── config.toml
    │
    └── my_pipeline.py
    ```

    Read more about adding [credentials](https://dlthub.com/docs/walkthroughs/add_credentials).
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    To set credentials via the toml files, you would first add your access token to `secrets.toml`:

    ```toml
    # .dlt/secrets.toml

    [sources]
    secret_key = "your_access_token"
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Alternatively, you can set:

    ```
    [sources]
    secret_key = "your_access_token"
    ```
    which is equal to:

    ```
    secret_key = "your_access_token"
    ```

    and to:

    ```
    [sources.____main____]
    secret_key = "your_access_token"
    ```
    as well as:

    ```
    [sources.____main____.github_source]
    secret_key = "your_access_token"
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### **Configure secrets in Colab**

    You can configure secrets using the **Secrets** sidebar. Just create a variable with the name `secrets.toml` and paste the content of the toml file from your `.dlt` folder into it. We support `config.toml` variable as well.

    Open the **Secrets** sidebar, press `Add new secret`, create a variable with name `secrets.toml` and copy-paste secrets in the `Value` field and click `Enable`:

    ```
    [sources]
    secret_key = "your_access_token"
    ```


    >dlt will not reload the secrets automatically. **Restart your interpreter** in Colab options when you add/change the variables above.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img4](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration_img4.png)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""✅ ▶ Proceed to the [next lesson](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-fundamentals-course/lesson_4_using_pre_build_sources_and_destinations.ipynb)!"""
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
