import pytest


@pytest.mark.parametrize(
    "example_name",
    ("load_all_datatypes",),
)
def test_debug_pipeline(example_name: str) -> None:
    from dlt.sources.pipeline_templates import debug_pipeline

    getattr(debug_pipeline, example_name)()


@pytest.mark.parametrize(
    "example_name",
    ("load_arrow_tables",),
)
def test_arrow_pipeline(example_name: str) -> None:
    from dlt.sources.pipeline_templates import arrow_pipeline

    getattr(arrow_pipeline, example_name)()


@pytest.mark.parametrize(
    "example_name",
    ("load_stuff",),
)
def test_default_pipeline(example_name: str) -> None:
    from dlt.sources.pipeline_templates import default_pipeline

    getattr(default_pipeline, example_name)()


@pytest.mark.parametrize(
    "example_name",
    ("load_chess_data",),
)
def test_requests_pipeline(example_name: str) -> None:
    from dlt.sources.pipeline_templates import requests_pipeline

    pytest.skip("TODO: unskip")
    getattr(requests_pipeline, example_name)()
