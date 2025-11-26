import pathlib

import dlt


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
    import dlthub.data_quality as dq

    data = [
        {"id": "foo", "subtotal": 10},
        {"id": "bar", "subtotal": 11},
        {"id": "baz", "subtotal": 5},
    ]

    pipeline = dlt.pipeline("test_missing_columns", destination="duckdb")
    pipeline.run(data, table_name="orders")
    dataset = pipeline.dataset()

    relation =  dq.prepare_checks(
        dataset,
        checks={"orders": [dq.checks.is_unique("id"), dq.checks.case("subtotal > 0")]}
    )

    @dlt.hub.transformation
    def transform(dataset: dlt.Dataset):
        yield relation

    assert relation is list(transform(dataset))[0]

    expected_column_names = [
        "table_name",
        "check_qualified_name",
        "row_count",
        "success_count",
        "success_rate",
    ]

    df_from_relation = relation.df()
    assert set(expected_column_names) == set(df_from_relation.columns)

    pipeline.extract(transform(dataset))
    normalize_info = pipeline.normalize()
    normalized_model_path = pathlib.Path(normalize_info[3][0][6]["new_jobs"][0].file_path)
    dialect, normalized_query = normalized_model_path.read_text().splitlines()
    
    df_from_pipeline = dataset(normalized_query).df()
    assert set(expected_column_names + ["_dlt_load_id"]) == set(df_from_pipeline.columns)
