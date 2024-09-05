"""Debug Pipeline for loading each datatype to your destination"""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt

from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# This is a generic pipeline example and demonstrates
# how to use the dlt REST client for extracting data from APIs.
# It showcases the use of authentication via bearer tokens and pagination.


@dlt.source
def source(api_secret_key: str = dlt.secrets.value):
    # print(f"api_secret_key={api_secret_key}")
    return resource(api_secret_key)


@dlt.resource(write_disposition="append")
def resource():
    # this is the test data for loading validation, delete it once you yield actual data
    yield [
        {
            "id": 1,
            "node_id": "MDU6SXNzdWUx",
            "number": 1347,
            "state": "open",
            "title": "Found a bug",
            "body": "I'm having a problem with this.",
            "user": {"login": "octocat", "id": 1},
            "created_at": "2011-04-22T13:33:48Z",
            "updated_at": "2011-04-22T13:33:48Z",
            "repository": {
                "id": 1296269,
                "node_id": "MDEwOlJlcG9zaXRvcnkxMjk2MjY5",
                "name": "Hello-World",
                "full_name": "octocat/Hello-World",
            },
        }
    ]


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name="pipeline",
        destination="duckdb",
        dataset_name="pipeline_data",
    )

    data = list(resource())

    # print the data yielded from resource
    print(data)  # noqa: T201

    # run the pipeline with your parameters
    # load_info = pipeline.run(source())

    # pretty print the information on data that was loaded
    # print(load_info)
