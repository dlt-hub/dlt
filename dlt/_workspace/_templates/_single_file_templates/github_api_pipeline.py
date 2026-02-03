"""The Github API templates provides a starting point to read data from REST APIs with REST Client"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Optional
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


@dlt.source
def github_source(access_token: Optional[str] = dlt.secrets.value):
    # Optional auth: works with and without secrets
    auth = BearerTokenAuth(token=access_token) if access_token else None

    client = RESTClient(
        base_url="https://api.github.com",
        auth=auth,
        paginator=HeaderLinkPaginator(),
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )

    # Define a resource which fetches repos data
    @dlt.resource(name="repos", write_disposition="replace")
    def repos():
        for page in client.paginate("orgs/dlt-hub/repos"):
            yield page

    # Define a resource which fetches issues data (incremental by updated_at timestamp)
    @dlt.resource(name="issues", write_disposition="append")
    def issues(
        updated_at=dlt.sources.incremental(
            "updated_at",
            initial_value="2026-01-01T00:00:00Z",
        )
    ):
        for page in client.paginate(
            "repos/dlt-hub/dlt/issues",
            params={
                "state": "open",
                "sort": "updated",
                "direction": "desc",
                "since": updated_at.start_value,
                "per_page": "100",
            },
        ):
            yield page

    return [repos, issues]


if __name__ == "__main__":
    # Define pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_api_pipeline",
        destination="duckdb",
        dataset_name="github_api_pipeline",
        progress="log",
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(github_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201
