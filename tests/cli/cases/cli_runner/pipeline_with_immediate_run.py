import dlt


@dlt.resource
def numbers_resource():
    for idx in range(100):
        yield {"id": idx, "num": idx}


@dlt.destination(loader_file_format="parquet")
def null_sink(_items, _table) -> None:
    pass


numbers_pipeline = dlt.pipeline(
    pipeline_name="numbers_pipeline",
    destination=null_sink,
)

load_info = numbers_pipeline.run()
print(load_info)
