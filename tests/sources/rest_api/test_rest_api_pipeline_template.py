import dlt
import pytest
from dlt.common.typing import TSecretStrValue


# NOTE: needs github secrets to work
@pytest.mark.parametrize(
    "example_name",
    (
        "load_github",
        "load_pokemon",
    ),
)
def test_all_examples(example_name: str) -> None:
    from dlt.sources import rest_api_pipeline

    # reroute token location from secrets
    github_token: TSecretStrValue = dlt.secrets.get("sources.github.access_token")
    dlt.secrets["sources.rest_api_pipeline.github.access_token"] = github_token
    getattr(rest_api_pipeline, example_name)()
