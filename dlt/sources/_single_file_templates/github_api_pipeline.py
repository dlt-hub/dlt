"""The Github API templates provides a starting point to read data from REST APIs with REST Client helper"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Optional

import dlt

from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


@dlt.resource(write_disposition="replace")
def github_api_resource(access_token: Optional[str] = dlt.secrets.value):
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"

    # Github allows both authenticated and non-authenticated requests (with low rate limits)
    auth = BearerTokenAuth(access_token) if access_token else None
    for page in paginate(
        url, auth=auth, paginator=HeaderLinkPaginator(), params={"state": "open", "per_page": "100"}
    ):
        yield page


@dlt.source
def github_api_source(access_token: Optional[str] = dlt.secrets.value):
    return github_api_resource(access_token=access_token)


def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="github_api_pipeline", destination="duckdb", dataset_name="github_api_data"
    )

    # print credentials by running the resource
    data = list(github_api_resource())

    # print the data yielded from resource
    print(data)  # noqa: T201

    # run the pipeline with your parameters
    load_info = pipeline.run(github_api_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run_source()
