import dlt


@dlt.resource
def squares_resource():
    for idx in range(10):
        yield {"id": idx, "square": idx * idx}


if __name__ == "__main__":
    p = dlt.pipeline(pipeline_name="dummy_pipeline", destination="dummy")
    load_info = p.run(squares_resource)
    print(load_info)
