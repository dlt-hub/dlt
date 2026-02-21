import dlt


def test_transformation_decorator() -> None:
    assert dlt.hub.transformation is not None

    @dlt.hub.transformation
    def get_even_rows(dataset: dlt.Dataset):
        return dataset.table("items").filter("id % 2 = 0")

    # get instance without license
    transformation = get_even_rows(dlt.dataset("duckdb", "mock_dataset"))
    assert transformation.name == "get_even_rows"
