import dlt


@dlt.resource
def quads_resource():
    for idx in range(10):
        yield {"id": idx, "num": idx**4}


@dlt.resource
def numbers_resource():
    for idx in range(10):
        yield {"id": idx, "num": idx + 1}


@dlt.destination(loader_file_format="parquet")
def null_sink(_items, _table) -> None:
    pass


quads_resource_instance = quads_resource()
numbers_resource_instance = numbers_resource()

quads_pipeline = dlt.pipeline(
    pipeline_name="numbers_quadruples_pipeline",
    destination=null_sink,
)

numbers_pipeline = dlt.pipeline(
    pipeline_name="my_numbers_pipeline",
    destination="duckdb",
)

if __name__ == "__main__":
    load_info = numbers_pipeline.run(numbers_resource(), schema=dlt.Schema("bobo-schema"))
