"""A template that is a good start for vibe coding REST API Source. Works best with `dlt ai` command cursor rules"""

import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)


@dlt.source
def source(access_token=dlt.secrets.value):
    config: RESTAPIConfig = {
        "client": {
            # TODO: place valid base url here
            "base_url": "https://example.com/v1/",
            # TODO: configure the right auth or remove if api does not need authentication
            # NOTE: pass secrets and other configuration in source function signature
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
        },
        "resources": [
            # TODO: add resource definitions here
        ],
    }

    yield from rest_api_resources(config)


def get_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    # TODO: during debugging feel free to pass access token explicitly
    # NOTE: use `secrets.toml` or env variables to pass configuration in production
    access_token = "my_access_token"
    load_info = pipeline.run(source(access_token))
    print(load_info)  # noqa


if __name__ == "__main__":
    get_data()
