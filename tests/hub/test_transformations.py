import dlt
from dlt.sources.rest_api import rest_api_resources


def test_transformation_decorator() -> None:
    assert dlt.hub.transformation is not None

    # current version blocks declarations with licenses
    @dlt.hub.transformation
    def get_even_rows(dataset: dlt.Dataset):
        return dataset.table("items").filter("id % 2 = 0")

    # get instance without license
    transformation = get_even_rows(dlt.dataset("duckdb", "mock_dataset"))
    assert transformation.name == "get_even_rows"


def test_missing_columns_bug() -> None:
    """Regression test: bare Identifier nodes were not properly type annotated in dlt.dataset.lineage.compute_columns_schema,
    causing success_count and success_rate to have UNKNOWN type and be excluded as incomplete columns by dlt.
    """
    import dlthub.data_quality as dq

    @dlt.source
    def jaffleshop():
        jaffle_rest_resources = rest_api_resources(
            {
                "client": {
                    "base_url": "https://jaffle-shop.dlthub.com/api/v1",
                    "paginator": {"type": "header_link"},
                },
                "resources": [
                    "customers",
                    "products",
                    "orders",
                ],
                "resource_defaults": {
                    "endpoint": {
                        "params": {
                            "start_date": "2017-01-01",
                            "end_date": "2017-01-15",
                        },
                    },
                },
            }
        )

        return jaffle_rest_resources

    @dlt.hub.transformation
    def jaffle_checks(dataset: dlt.Dataset) -> dlt.Relation:
        checks = {"orders": [dq.checks.is_unique("id"), dq.checks.case("subtotal > 0")]}
        return dq.prepare_checks(dataset, checks=checks)

    pipeline = dlt.pipeline("test_missing_columns", destination="duckdb")
    pipeline.run([jaffleshop()])
    pipeline.run(jaffle_checks(pipeline.dataset()))

    expected_column_names = [
        "table_name",
        "check_qualified_name",
        "row_count",
        "success_count",  # was missing due to unqualified column
        "success_rate",  # was missing due to unqualified column
    ]

    # direct query execution returns raw select output (no dlt columns)
    query = dq.prepare_checks(
        pipeline.dataset(),
        checks={
            "orders": [dq.checks.is_unique("id"), dq.checks.case("subtotal > 0")],
        },
    )
    assert query.arrow().column_names == expected_column_names

    # materialized table includes _dlt_load_id added by pipeline
    with pipeline.sql_client() as client:
        with client.execute_query(
            f"SELECT * FROM {pipeline.pipeline_name}.{pipeline.dataset_name}.jaffle_checks"
        ) as cursor:
            df = cursor.df()
            columns = list(df.columns)
    assert columns == expected_column_names + ["_dlt_load_id"]
