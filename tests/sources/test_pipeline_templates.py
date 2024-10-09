import pytest
import importlib


@pytest.mark.parametrize(
    "template_name,examples",
    [
        ("debug_pipeline", ("load_all_datatypes",)),
        ("default_pipeline", ("load_api_data", "load_sql_data", "load_pandas_data")),
        ("arrow_pipeline", ("load_arrow_tables",)),
        ("dataframe_pipeline", ("load_dataframe",)),
        ("requests_pipeline", ("load_chess_data",)),
        ("github_api_pipeline", ("run_source",)),
        ("fruitshop_pipeline", ("load_shop",)),
    ],
)
def test_debug_pipeline(template_name: str, examples: str) -> None:
    demo_module = importlib.import_module(f"dlt.sources.pipeline_templates.{template_name}")
    for example_name in examples:
        getattr(demo_module, example_name)()
