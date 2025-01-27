from utils import parse_toml_file


def parallel_config_snippet() -> None:
    # @@@DLT_SNIPPET_START parallel_config
    import os
    import dlt
    from itertools import islice
    from dlt.common import pendulum

    @dlt.resource(name="table")
    def read_table(limit):
        rows = iter(range(limit))
        while item_slice := list(islice(rows, 1000)):
            now = pendulum.now().isoformat()
            yield [
                {"row": _id, "description": "this is row with id {_id}", "timestamp": now}
                for _id in item_slice
            ]

    # this prevents process pool to run the initialization code again
    if __name__ == "__main__" or "PYTEST_CURRENT_TEST" in os.environ:
        pipeline = dlt.pipeline("parallel_load", destination="duckdb", dev_mode=True)
        pipeline.extract(read_table(1000000))

        load_id = pipeline.list_extracted_load_packages()[0]
        extracted_package = pipeline.get_load_package_info(load_id)
        # we should have 11 files (10 pieces for `table` and 1 for state)
        extracted_jobs = extracted_package.jobs["new_jobs"]
        print([str(job.job_file_info) for job in extracted_jobs])
        # normalize and print counts
        print(pipeline.normalize(loader_file_format="jsonl"))
        # print jobs in load package (10 + 1 as above)
        load_id = pipeline.list_normalized_load_packages()[0]
        print(pipeline.get_load_package_info(load_id))
        print(pipeline.load())
        # @@@DLT_SNIPPET_END parallel_config

        assert len(extracted_jobs) == 11
        loaded_package = pipeline.get_load_package_info(load_id)
        assert len(loaded_package.jobs["completed_jobs"]) == 11


def parallel_extract_callables_snippet() -> None:
    # @@@DLT_SNIPPET_START parallel_extract_callables
    import dlt
    import time
    from threading import currentThread

    @dlt.resource(parallelized=True)
    def list_users(n_users):
        for i in range(1, 1 + n_users):
            # Simulate network delay of a rest API call fetching a page of items
            if i % 10 == 0:
                time.sleep(0.1)
            yield i

    @dlt.transformer(parallelized=True)
    def get_user_details(user_id):
        # Transformer that fetches details for users in a page
        time.sleep(0.1)  # Simulate latency of a rest API call
        print(f"user_id {user_id} in thread {currentThread().name}")
        return {"entity": "user", "id": user_id}

    @dlt.resource(parallelized=True)
    def list_products(n_products):
        for i in range(1, 1 + n_products):
            if i % 10 == 0:
                time.sleep(0.1)
            yield i

    @dlt.transformer(parallelized=True)
    def get_product_details(product_id):
        time.sleep(0.1)
        print(f"product_id {product_id} in thread {currentThread().name}")
        return {"entity": "product", "id": product_id}

    @dlt.source
    def api_data():
        return [
            list_users(24) | get_user_details,
            list_products(32) | get_product_details,
        ]

    # evaluate the pipeline and print all the items
    # sources are iterators and they are evaluated in the same way in the pipeline.run
    print(list(api_data()))
    # @@@DLT_SNIPPET_END parallel_extract_callables

    # @@@DLT_SNIPPET_START parallel_extract_awaitables
    import asyncio

    @dlt.resource
    async def a_list_items(start, limit):
        # simulate a slow REST API where you wait 0.3 sec for each item
        index = start
        while index < start + limit:
            await asyncio.sleep(0.3)
            yield index
            index += 1

    @dlt.transformer
    async def a_get_details(item_id):
        # simulate a slow REST API where you wait 0.3 sec for each item
        await asyncio.sleep(0.3)
        print(f"item_id {item_id} in thread {currentThread().name}")
        # just return the results, if you yield, generator will be evaluated in main thread
        return {"row": item_id}

    print(list(a_list_items(0, 10) | a_get_details))
    # @@@DLT_SNIPPET_END parallel_extract_awaitables


def performance_chunking_snippet() -> None:
    # @@@DLT_SNIPPET_START performance_chunking
    import dlt

    def get_rows(limit):
        yield from map(lambda n: {"row": n}, range(limit))

    @dlt.resource
    def database_cursor():
        # here we yield each row returned from database separately
        yield from get_rows(10000)

    # @@@DLT_SNIPPET_END performance_chunking

    # @@@DLT_SNIPPET_START performance_chunking_chunk
    from itertools import islice

    @dlt.resource
    def database_cursor_chunked():
        # here we yield chunks of size 1000
        rows = get_rows(10000)
        while item_slice := list(islice(rows, 1000)):
            print(f"got chunk of length {len(item_slice)}")
            yield item_slice

    # @@@DLT_SNIPPET_END performance_chunking_chunk

    assert len(list(database_cursor())) == 10000
    assert len(list(database_cursor_chunked())) == 10000


def parallel_pipelines_asyncio_snippet() -> None:
    # @@@DLT_SNIPPET_START parallel_pipelines
    import asyncio
    import dlt
    from time import sleep
    from concurrent.futures import ThreadPoolExecutor

    # create both asyncio and thread parallel resources
    @dlt.resource
    async def async_table():
        for idx_ in range(10):
            await asyncio.sleep(0.1)
            yield {"async_gen": idx_}

    @dlt.resource(parallelized=True)
    def defer_table():
        for idx_ in range(5):
            sleep(0.1)
            yield idx_

    def _run_pipeline(pipeline, gen_):
        # run the pipeline in a thread, also instantiate generators here!
        # Python does not let you use generators across threads
        return pipeline.run(gen_())

    # declare pipelines in main thread then run them "async"
    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
    pipeline_2 = dlt.pipeline("pipeline_2", destination="duckdb", dev_mode=True)

    async def _run_async():
        loop = asyncio.get_running_loop()
        # from Python 3.9 you do not need explicit pool. loop.to_thread will suffice
        with ThreadPoolExecutor() as executor:
            results = await asyncio.gather(
                loop.run_in_executor(executor, _run_pipeline, pipeline_1, async_table),
                loop.run_in_executor(executor, _run_pipeline, pipeline_2, defer_table),
            )
        # results contains two LoadInfo instances
        print("pipeline_1", results[0])
        print("pipeline_2", results[1])

    # load data
    asyncio.run(_run_async())
    # activate pipelines before they are used
    pipeline_1.activate()
    assert pipeline_1.last_trace.last_normalize_info.row_counts["async_table"] == 10
    pipeline_2.activate()
    assert pipeline_2.last_trace.last_normalize_info.row_counts["defer_table"] == 5
    # @@@DLT_SNIPPET_END parallel_pipelines


def test_toml_snippets() -> None:
    parse_toml_file("./toml-snippets.toml")
