import dlt


@dlt.resource
def example_resource(api_url=dlt.config.value, api_key=dlt.secrets.value, last_id=0):
    yield [api_url, api_key, str(last_id), "param4", "param5"]


@dlt.source
def example_source(api_url=dlt.config.value, api_key=dlt.secrets.value, last_id=0):
    # return all the resources to be loaded
    return example_resource(api_url, api_key, last_id)


if __name__ == "__main__":
    p = dlt.pipeline(
        pipeline_name="debug_pipeline",
        destination="postgres",
        dataset_name="debug_pipeline_data",
        dev_mode=False,
    )
    load_info = p.run(example_source(last_id=819273998))
    print(load_info)
