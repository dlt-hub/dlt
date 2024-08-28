import pytest
@pytest.mark.parametrize(
    "example_name",
    (
        "load_github",
        "load_pokemon",
    ),
)
def test_all_examples(example_name: str) -> None:
    from dlt.sources import rest_api_pipeline

    getattr(rest_api_pipeline, example_name)()
