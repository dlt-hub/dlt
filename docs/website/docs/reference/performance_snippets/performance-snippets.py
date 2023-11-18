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
            yield [{"row": _id, "description": "this is row with id {_id}", "timestamp": now} for _id in item_slice]

    # this prevents process pool to run the initialization code again
    if __name__ == "__main__" or "PYTEST_CURRENT_TEST" in os.environ:
        pipeline = dlt.pipeline("parallel_load", destination="duckdb", full_refresh=True)
        pipeline.extract(read_table(1000000))

        # we should have 11 files (10 pieces for `table` and 1 for state)
        extracted_files = pipeline.list_extracted_resources()
        print(extracted_files)
        # normalize and print counts
        print(pipeline.normalize(loader_file_format="jsonl"))
        # print jobs in load package (10 + 1 as above)
        load_id = pipeline.list_normalized_load_packages()[0]
        print(pipeline.get_load_package_info(load_id))
        print(pipeline.load())
        # @@@DLT_SNIPPET_END parallel_config

        assert len(extracted_files) == 11
        loaded_package = pipeline.get_load_package_info(load_id)
        assert len(loaded_package.jobs["completed_jobs"]) == 11


def parallel_extract_callables_snippet() -> None:
    # @@@DLT_SNIPPET_START parallel_extract_callables
    import dlt
    from time import sleep
    from threading import currentThread

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


def test_toml_snippets() -> None:
    parse_toml_file("./toml-snippets.toml")



