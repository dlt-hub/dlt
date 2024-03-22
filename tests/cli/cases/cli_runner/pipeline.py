import dlt


@dlt.resource
def quads_resource():
    for idx in range(10):
        yield {"id": idx, "quad": idx**4}


@dlt.resource
def squares_resource():
    for idx in range(10):
        yield {"id": idx, "square": idx * idx}


quads_resource_instance = quads_resource()
squares_resource_instance = squares_resource()

quads_pipeline = dlt.pipeline(pipeline_name="numbers_quadruples_pipeline", destination="duckdb")
squares_pipeline = dlt.pipeline(pipeline_name="numbers_pipeline", destination="duckdb")

load_info = squares_pipeline.run(quads_resource())
load_info_2 = squares_pipeline.run(squares_resource)

print(load_info)
