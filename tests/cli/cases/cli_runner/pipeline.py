import dlt


@dlt.resource
def quads_resource():
    for idx in range(10):
        yield {"id": idx, "quad": idx**4}


@dlt.resource
def squares_resource():
    for idx in range(10):
        yield {"id": idx, "square": idx * idx}


@dlt.destination(loader_file_format="parquet")
def null_sink(_items, _table) -> None:
    pass


quads_resource_instance = quads_resource()
squares_resource_instance = squares_resource()

quads_pipeline = dlt.pipeline(
    pipeline_name="numbers_quadruples_pipeline",
    destination=null_sink,
)

squares_pipeline = dlt.pipeline(
    pipeline_name="numbers_pipeline",
    destination="duckdb",
)

if __name__ == "__main__":
    load_info = squares_pipeline.run(squares_resource(), schema=dlt.Schema("bobo-schema"))
