# Python internals
import os
from typing import Any

# Other libraries
import dlt

if __name__ == "__main__":

    @dlt.resource(table_name="foo_data")
    def foo() -> Any:
        for i in range(10):
            yield {"id": i, "name": f"This is item {i}"}

    pipeline = dlt.pipeline(
        pipeline_name="python_data_example",
        destination="duckdb",
    )

    load_info = pipeline.run(foo)

    # print load info and the "foo_data" table as dataframe
    print(load_info)
    print(pipeline.dataset().foo_data.df())
    print("Example pipeline run completed")  # keep this for ci integration tests
    print(f"RUNTIME__RUN_ID: {os.getenv('RUNTIME__RUN_ID')}")
    print(f"RUNTIME__WORKSPACE_ID: {os.getenv('RUNTIME__WORKSPACE_ID')}")
