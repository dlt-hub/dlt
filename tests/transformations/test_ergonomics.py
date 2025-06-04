# import pytest

# import dlt
# from dlt.transformations.ergonomics import transformation
# import pandas as pd


# @dlt.resource
# def items():
#     yield from (
#         {"id": 0, "value": "foo"},
#         {"id": 1, "value": "bar"},
#         {"id": 2, "value": "baz"},
#     )


# @transformation
# def total_value_length_lazy() -> str:
#     """Eager transformation using raw SQL"""
#     return "SELECT SUM(LENGTH(value)) AS total_length FROM items"


# @transformation
# def total_value_length_eager(dataset: dlt.Dataset) -> pd.DataFrame:
#     df = dataset.table("items").df()
#     total_length = df["value"].str.len().sum()
#     return pd.DataFrame([{"total_length": total_length}])


# def test_standalone_lazy_execution():
#     """Lazy transformation functions should return the query expression"""
#     query = total_value_length_lazy()
#     assert query == "SELECT SUM(LENGTH(value)) AS total_length FROM items"


# def test_standalone_eager_execution():
#     with pytest.raises(TypeError):
#         total_value_length_eager()  # missing an arg

#     extract_pipeline = dlt.pipeline("el", destination="duckdb", dev_mode=True)
#     extract_pipeline.run([items()])
#     input_dataset = extract_pipeline.dataset(dataset_type="default")

#     results = total_value_length_eager(input_dataset)

#     assert isinstance(results, pd.DataFrame)
#     assert results.shape == (1, 1)


# def test_simple_case():
#     destination = dlt.destinations.duckdb()
#     extract_pipeline = dlt.pipeline("el", destination=destination, dev_mode=True)
#     transform_pipeline = dlt.pipeline("t", destination=destination, dev_mode=True)

#     extract_pipeline.run([items()])
#     transform_pipeline.run([total_value_length_lazy()])

#     output_dataset = transform_pipeline.dataset(dataset_type="default")
#     output_table = output_dataset.table("total_value_length_lazy").df()
#     assert False
