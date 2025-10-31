"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

# if no argument is provided, `access_token` is read from `.dlt/secrets.toml`
@dlt.source
def rest_api_source(access_token: str = dlt.secrets.value):
    """Define dlt resources from REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            # TODO set base URL for the REST API
            "base_url": "https://example.com/v1/",
            # TODO configure the right authentication method or remove
            "auth": {"type": "bearer", "token": access_token},
        },
        "resources": [
            # TODO define resources per endpoint
        ],
        # set `resource_defaults` to apply configuration to all endpoints
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="rest_api_ingest",
    destination="duckdb",
    #  `dev_mode=True` creates a new namespace for each pipeline run.
    # set to `False` once you have a working pipeline you're satisfied with.
    dev_mode=True,
)


if __name__ == "__main__":
    load_info = pipeline.run(rest_api_source())
    print(load_info)
