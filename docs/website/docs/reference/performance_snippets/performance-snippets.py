from utils import parse_toml_file


def parallel_config_snippet() -> None:
    # @@@DLT_SNIPPET_START parallel_config
    import os
    from itertools import islice

    import dlt
    from dlt.common import pendulum

    @dlt.resource(name="table")
    def read_table(limit):
        rows = iter(range(limit))
        while item_slice := list(islice(rows, 1000)):
            now = pendulum.now().isoformat()
            yield [
                {
                    "row": _id,
                    "description": "this is row with id {_id}",
                    "timestamp": now,
                }
                for _id in item_slice
            ]

    # this prevents process pool to run the initialization code again
    if __name__ == "__main__" or "PYTEST_CURRENT_TEST" in os.environ:
        pipeline = dlt.pipeline("parallel_load", destination="duckdb", full_refresh=True)
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
    from threading import currentThread
    from time import sleep

    import dlt

    @dlt.resource
    def list_items(start, limit):
        yield from range(start, start + limit)

    @dlt.transformer
    @dlt.defer
    def get_details(item_id):
        # simulate a slow REST API where you wait 0.3 sec for each item
        sleep(0.3)
        print(f"item_id {item_id} in thread {currentThread().name}")
        # just return the results, if you yield, generator will be evaluated in main thread
        return {"row": item_id}

    # evaluate the pipeline and print all the items
    # resources are iterators and they are evaluated in the same way in the pipeline.run
    print(list(list_items(0, 10) | get_details))
    # @@@DLT_SNIPPET_END parallel_extract_callables

    # @@@DLT_SNIPPET_START parallel_extract_awaitables
    import asyncio

    @dlt.transformer
    async def a_get_details(item_id):
        # simulate a slow REST API where you wait 0.3 sec for each item
        await asyncio.sleep(0.3)
        print(f"item_id {item_id} in thread {currentThread().name}")
        # just return the results, if you yield, generator will be evaluated in main thread
        return {"row": item_id}

    print(list(list_items(0, 10) | a_get_details))
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
    from concurrent.futures import ThreadPoolExecutor
    from time import sleep

    import dlt

    # create both futures and thread parallel resources

    def async_table():
        async def _gen(idx):
            await asyncio.sleep(0.1)
            return {"async_gen": idx}

        # just yield futures in a loop
        for idx_ in range(10):
            yield _gen(idx_)

    def defer_table():
        @dlt.defer
        def _gen(idx):
            sleep(0.1)
            return {"thread_gen": idx}

        # just yield futures in a loop
        for idx_ in range(5):
            yield _gen(idx_)

    def _run_pipeline(pipeline, gen_):
        # run the pipeline in a thread, also instantiate generators here!
        # Python does not let you use generators across threads
        return pipeline.run(gen_())

    # declare pipelines in main thread then run them "async"
    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
    pipeline_2 = dlt.pipeline("pipeline_2", destination="duckdb", full_refresh=True)

    async def _run_async():
        loop = asyncio.get_running_loop()
        # from Python 3.9 you do not need explicit pool. loop.to_thread will suffice
        with ThreadPoolExecutor() as executor:
            results = await asyncio.gather(
                loop.run_in_executor(executor, _run_pipeline, pipeline_1, async_table),
                loop.run_in_executor(executor, _run_pipeline, pipeline_2, defer_table),
            )
        # result contains two LoadInfo instances
        results[0].raise_on_failed_jobs()
        results[1].raise_on_failed_jobs()

    # load data
    asyncio.run(_run_async())
    # activate pipelines before they are used
    pipeline_1.activate()
    # assert load_data_table_counts(pipeline_1) == {"async_table": 10}
    pipeline_2.activate()
    # assert load_data_table_counts(pipeline_2) == {"defer_table": 5}
    # @@@DLT_SNIPPET_END parallel_pipelines


def test_toml_snippets() -> None:
    parse_toml_file("./toml-snippets.toml")
