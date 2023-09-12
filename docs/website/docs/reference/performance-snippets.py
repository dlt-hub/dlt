from conftest import string_toml_provider

PARALLEL_CONFIG_TOML = """
# @@@DLT_SNIPPET_START parallel_config_toml
# the pipeline name is default source name when loading resources

[sources.parallel_load.data_writer]
file_max_items=100000

[normalize]
workers=3

[data_writer]
file_max_items=100000

[load]
workers=11

# @@@DLT_SNIPPET_END parallel_config_toml
"""

def parallel_config_snippet() -> None:
    string_toml_provider.update(PARALLEL_CONFIG_TOML)

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

    string_toml_provider.update("""
# @@@DLT_SNIPPET_START buffer_toml
# set buffer size for extract and normalize stages
[data_writer]
buffer_max_items=100

# set buffers only in extract stage - for all sources
[sources.data_writer]
buffer_max_items=100

# set buffers only for a source with name zendesk_support
[sources.zendesk_support.data_writer]
buffer_max_items=100

# set buffers in normalize stage
[normalize.data_writer]
buffer_max_items=100
# @@@DLT_SNIPPET_END buffer_toml
""")

    string_toml_provider.update("""
# @@@DLT_SNIPPET_START file_size_toml
# extract and normalize stages
[data_writer]
file_max_items=100000
max_file_size=1000000

# only for the extract stage - for all sources
[sources.data_writer]
file_max_items=100000
max_file_size=1000000

# only for the extract stage of a source with name zendesk_support
[sources.zendesk_support.data_writer]
file_max_items=100000
max_file_size=1000000

# only for the normalize stage
[normalize.data_writer]
file_max_items=100000
max_file_size=1000000 
# @@@DLT_SNIPPET_END file_size_toml
""")

    string_toml_provider.update("""
# @@@DLT_SNIPPET_START extract_workers_toml
# for all sources and resources being extracted
[extract]
worker=1

# for all resources in the zendesk_support source
[sources.zendesk_support.extract]
workers=2

# for the tickets resource in the zendesk_support source
[sources.zendesk_support.tickets.extract]
workers=4
# @@@DLT_SNIPPET_END extract_workers_toml
""")

    string_toml_provider.update("""
# @@@DLT_SNIPPET_START extract_parallel_items_toml
# for all sources and resources being extracted
[extract]
max_parallel_items=10

# for all resources in the zendesk_support source
[sources.zendesk_support.extract]
max_parallel_items=10

# for the tickets resource in the zendesk_support source
[sources.zendesk_support.tickets.extract]
max_parallel_items=10
# @@@DLT_SNIPPET_END extract_parallel_items_toml
""")
                                
string_toml_provider.update("""
# @@@DLT_SNIPPET_START normalize_workers_toml
 [extract.data_writer]
# force extract file rotation if size exceeds 1MiB
max_file_size=1000000

[normalize]
# use 3 worker processes to process 3 files in parallel
workers=3
# @@@DLT_SNIPPET_END normalize_workers_toml
""")
                            
string_toml_provider.update("""
# @@@DLT_SNIPPET_START normalize_workers_2_toml
[normalize.data_writer]
# force normalize file rotation if it exceeds 1MiB
max_file_size=1000000

[load]
# have 50 concurrent load jobs
workers=50
# @@@DLT_SNIPPET_END normalize_workers_2_toml
""")

string_toml_provider.update("""
# @@@DLT_SNIPPET_START retry_toml
[runtime]
request_max_attempts = 10  # Stop after 10 retry attempts instead of 5
request_backoff_factor = 1.5  # Multiplier applied to the exponential delays. Default is 1
request_timeout = 120  # Timeout in seconds
request_max_retry_delay = 30  # Cap exponential delay to 30 seconds
# @@@DLT_SNIPPET_END retry_toml
""")
                            
string_toml_provider.update("""
# @@@DLT_SNIPPET_START item_mode_toml
[extract] # global setting
next_item_mode="round_robin"

[sources.my_pipeline.extract] # setting for the "my_pipeline" pipeline
next_item_mode="fifo"                     
# @@@DLT_SNIPPET_END item_mode_toml
""")
                               
string_toml_provider.update("""
# @@@DLT_SNIPPET_START compression_toml
[normalize.data_writer]
disable_compression=true
# @@@DLT_SNIPPET_END compression_toml
""")
