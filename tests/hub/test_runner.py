import dlt


def test_runner_instance() -> None:
    pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

    @dlt.resource(table_name="my_table")
    def my_resource():
        return [1, 2, 3]

    load_info = dlt.hub.runner(pipeline).run(my_resource())
    print(load_info)
