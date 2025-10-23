# Python internals
import os
from typing import Any
import signal
from time import sleep

# Other libraries
import dlt

if __name__ == "__main__":
    
    running = True

    def handle_signal(signum, frame):
        global running
        print(f"Received signal {signum}, stopping loop...")
        running = False

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_signal)   # Ctrl+C
    signal.signal(signal.SIGTERM, handle_signal)  # Termination signal

    
    while running:

        @dlt.resource(table_name="foo_data")
        def foo() -> Any:
            for i in range(10):
                yield {"id": i, "name": f"This is item {i}"}

        pipeline = dlt.pipeline(
            pipeline_name="python_data_example",
            destination="duckdb",
        )

        load_info = pipeline.run(foo)
        sleep(1)
        print(load_info)
        print(pipeline.dataset().foo_data.df())

    # print load info and the "foo_data" table as dataframe

    print("Example pipeline run completed")  # keep this for ci integration tests
    print(f"RUNTIME__RUN_ID: {os.getenv('RUNTIME__RUN_ID')}")
    print(f"RUNTIME__WORKSPACE_ID: {os.getenv('RUNTIME__WORKSPACE_ID')}")
