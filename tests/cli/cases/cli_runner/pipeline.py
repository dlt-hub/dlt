import dlt


@dlt.resource
def squares_resource():
    for idx in range(10):
        yield {"id": idx, "square": idx * idx}


squares_resource_instance = squares_resource()

squares_pipeline = dlt.pipeline(pipeline_name="numbers_pipeline", destination="duckdb")
load_info = squares_pipeline.run(squares_resource)

print(load_info)
