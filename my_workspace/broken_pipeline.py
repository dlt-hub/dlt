# Python internals
import os
from typing import Any

# Other libraries
import dlt

if __name__ == "__main__":

    @dlt.resource(table_name="foo_data")
    def foo() -> Any:
        assert False, "This is a broken pipeline"
        for i in range(10):
            yield {"id": i, "name": f"This is item {i}"}

    pipeline = dlt.pipeline(
        pipeline_name="python_data_example",
        destination="duckdb",
    )

    load_info = pipeline.run(foo)