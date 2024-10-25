"""The Github API templates provide a starting point to read data from REST APIs with REST Client helper"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Optional
import os
import dlt
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


@dlt.resource(write_disposition="replace")
def github_api_resource(api_secret_key: Optional[str] = None):
    # Fallback to environment variable if no API secret key is provided
    api_secret_key = api_secret_key or os.getenv("GITHUB_API_TOKEN")
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"

    # Use BearerTokenAuth for authenticated requests if the token is available
    auth = BearerTokenAuth(api_secret_key) if api_secret_key else None

    # Paginate through the GitHub API results
    for page in paginate(
        url, auth=auth, paginator=HeaderLinkPaginator(), params={"state": "open", "per_page": "100"}
    ):
        yield page


@dlt.source
def github_api_source(api_secret_key: Optional[str] = None):
    # Pass the token from the function argument or environment variable
    return github_api_resource(api_secret_key=api_secret_key)


def run_source() -> None:
    # Configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="github_api_pipeline", destination="duckdb", dataset_name="github_api_data"
    )

    # Run the resource to get data
    data = list(github_api_resource())

    # Print the data yielded from the resource (for debugging purposes)
    print(data)  # noqa: T201

    # Run the pipeline with your parameters
    load_info = pipeline.run(github_api_source())

    # Pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run_source()
