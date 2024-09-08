import pytest


# TODO: not all template functions are tested here
# we may be able to test more in tests/load/sources
@pytest.mark.parametrize(
    "example_name",
    (
        "load_select_tables_from_database",
        # "load_entire_database",
        "load_standalone_table_resource",
        "select_columns",
        "specify_columns_to_load",
        "test_pandas_backend_verbatim_decimals",
        "select_with_end_value_and_row_order",
        "my_sql_via_pyarrow",
    ),
)
def test_all_examples(example_name: str) -> None:
    from dlt.sources import sql_database_pipeline

    getattr(sql_database_pipeline, example_name)()
