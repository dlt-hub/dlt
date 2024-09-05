"""Default Pipeline template for loading each datatype to your destination"""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt

# This is a generic pipeline example and demonstrates
# how to use the dlt REST client for extracting data from APIs.
# It showcases the use of authentication via bearer tokens and pagination.


@dlt.source
def source(
    api_secret_key: str = dlt.secrets.value,
    org: str = "dlt-hub",
    repository: str = "dlt",
):
    """This source function aggregates data from two GitHub endpoints: issues and pull requests."""
    # Ensure that secret key is provided for GitHub
    # either via secrets.toml or via environment variables.
    # print(f"api_secret_key={api_secret_key}")

    api_url = f"https://api.github.com/repos/{org}/{repository}"
    return [
        resource_1(api_url, api_secret_key),
        resource_2(api_url, api_secret_key),
    ]


@dlt.resource
def resource_1(api_url: str, api_secret_key: str = dlt.secrets.value):
    """
    Fetches issues from a specified repository on GitHub using Bearer Token Authentication.
    """
    # paginate issues and yield every page
    pass


@dlt.resource
def resource_2(api_url: str, api_secret_key: str = dlt.secrets.value):
    pass


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name="generic",
        destination="duckdb",
        dataset_name="generic_data",
        full_refresh=False,
    )

    load_info = p.run(source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201
