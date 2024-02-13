from typing import Any
import time
import threading
import random

import dlt, asyncio, pytest, os


def test_async_iterator_resource() -> None:
    # define an asynchronous iterator
    @dlt.resource()
    class AsyncIterator:
        def __init__(self):
            self.counter = 0

        def __aiter__(self):
            return self

        # return the next awaitable
        async def __anext__(self):
            # check for no further items
            if self.counter >= 5:
                raise StopAsyncIteration()
            # increment the counter
            self.counter += 1
            # simulate work
            await asyncio.sleep(0.1)
            # return the counter value
            return {"i": self.counter}

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
    pipeline_1.run(AsyncIterator, table_name="async")
    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM async") as cur:
            rows = list(cur.fetchall())
            assert [r[0] for r in rows] == [1, 2, 3, 4, 5]


#
# async generators resource tests
#
def test_async_generator_resource() -> None:
    async def async_gen_table():
        for l_ in ["a", "b", "c"]:
            await asyncio.sleep(0.1)
            yield {"letter": l_}

    @dlt.resource
    async def async_gen_resource():
        for l_ in ["d", "e", "f"]:
            await asyncio.sleep(0.1)
            yield {"letter": l_}

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)

    # pure async function
    pipeline_1.run(async_gen_table(), table_name="async")
    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM async") as cur:
            rows = list(cur.fetchall())
            assert [r[0] for r in rows] == ["a", "b", "c"]

    # async resource
    pipeline_1.run(async_gen_resource(), table_name="async")
    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM async") as cur:
            rows = list(cur.fetchall())
            assert [r[0] for r in rows] == ["a", "b", "c", "d", "e", "f"]


def test_async_generator_nested() -> None:
    def async_inner_table():
        async def _gen(idx):
            for l_ in ["a", "b", "c"]:
                await asyncio.sleep(0.1)
                yield {"async_gen": idx, "letter": l_}

        # just yield futures in a loop
        for idx_ in range(3):
            yield _gen(idx_)

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
    pipeline_1.run(async_inner_table(), table_name="async")
    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM async") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 9
            assert {(r[0], r[1]) for r in rows} == {
                (0, "a"),
                (0, "b"),
                (0, "c"),
                (1, "a"),
                (1, "b"),
                (1, "c"),
                (2, "a"),
                (2, "b"),
                (2, "c"),
            }


def test_async_generator_transformer() -> None:
    @dlt.resource
    async def async_resource():
        for l_ in ["a", "b", "c"]:
            await asyncio.sleep(0.1)
            yield {"letter": l_}

    @dlt.transformer(data_from=async_resource)
    async def async_transformer(item):
        await asyncio.sleep(0.1)
        yield {
            "letter": item["letter"] + "t",
        }

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
    pipeline_1.run(async_transformer(), table_name="async")

    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM async") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3
            assert {r[0] for r in rows} == {"at", "bt", "ct"}


@pytest.mark.parametrize("next_item_mode", ["fifo", "round_robin"])
@pytest.mark.parametrize(
    "resource_mode", ["both_sync", "both_async", "first_async", "second_async"]
)
def test_parallel_async_generators(next_item_mode: str, resource_mode: str) -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = next_item_mode
    execution_order = []

    @dlt.resource(table_name="table1")
    def sync_resource1():
        for l_ in ["a", "b", "c"]:
            nonlocal execution_order
            execution_order.append("one")
            yield {"letter": l_}

    @dlt.resource(table_name="table2")
    def sync_resource2():
        for l_ in ["e", "f", "g"]:
            nonlocal execution_order
            execution_order.append("two")
            yield {"letter": l_}

    @dlt.resource(table_name="table1")
    async def async_resource1():
        for l_ in ["a", "b", "c"]:
            await asyncio.sleep(1)
            nonlocal execution_order
            execution_order.append("one")
            yield {"letter": l_}

    @dlt.resource(table_name="table2")
    async def async_resource2():
        await asyncio.sleep(0.5)
        for l_ in ["e", "f", "g"]:
            await asyncio.sleep(1)
            nonlocal execution_order
            execution_order.append("two")
            yield {"letter": l_}

    @dlt.source
    def source():
        if resource_mode == "both_sync":
            return [sync_resource1(), sync_resource2()]
        elif resource_mode == "both_async":
            return [async_resource1(), async_resource2()]
        elif resource_mode == "first_async":
            return [async_resource1(), sync_resource2()]
        elif resource_mode == "second_async":
            return [sync_resource1(), async_resource2()]

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
    pipeline_1.run(source())

    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM table1") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3
            assert {r[0] for r in rows} == {"a", "b", "c"}

        with c.execute_query("SELECT * FROM table2") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3
            assert {r[0] for r in rows} == {"e", "f", "g"}

    # in both item modes there will be parallel execution
    if resource_mode in ["both_async"]:
        assert execution_order == ["one", "two", "one", "two", "one", "two"]
    # first the first resouce is exhausted, then the second
    elif resource_mode in ["both_sync"] and next_item_mode == "fifo":
        assert execution_order == ["one", "one", "one", "two", "two", "two"]
    # round robin is executed in sync
    elif resource_mode in ["both_sync"] and next_item_mode == "round_robin":
        assert execution_order == ["one", "two", "one", "two", "one", "two"]
    elif resource_mode in ["first_async"]:
        assert execution_order == ["two", "two", "two", "one", "one", "one"]
    elif resource_mode in ["second_async"]:
        assert execution_order == ["one", "one", "one", "two", "two", "two"]
    else:
        raise AssertionError("Unknown combination")


def test_limit_async_resource() -> None:
    @dlt.resource(table_name="table1")
    async def async_resource1():
        for l_ in range(20):
            print(l_)
            await asyncio.sleep(0.1)
            yield {"index": l_}

    result = list(async_resource1().add_limit(13))
    assert len(result) == 13


@pytest.mark.parametrize("parallelized", [True, False])
def test_parallelized_resource(parallelized: bool) -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"
    execution_order = []
    threads = set()

    @dlt.resource(parallelized=parallelized)
    def resource1():
        for l_ in ["a", "b", "c"]:
            time.sleep(0.5)
            nonlocal execution_order
            execution_order.append("one")
            threads.add(threading.get_ident())
            yield {"letter": l_}

    @dlt.resource(parallelized=parallelized)
    def resource2():
        time.sleep(0.25)
        for l_ in ["e", "f", "g"]:
            time.sleep(0.5)
            nonlocal execution_order
            execution_order.append("two")
            threads.add(threading.get_ident())
            yield {"letter": l_}

    @dlt.source
    def source():
        return [resource1(), resource2()]

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
    pipeline_1.run(source())

    # all records should be here
    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM resource1") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3
            assert {r[0] for r in rows} == {"a", "b", "c"}

        with c.execute_query("SELECT * FROM resource2") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 3
            assert {r[0] for r in rows} == {"e", "f", "g"}

    if parallelized:
        assert len(threads) > 1
        assert execution_order == ["one", "two", "one", "two", "one", "two"]
    else:
        assert execution_order == ["one", "one", "one", "two", "two", "two"]
        assert len(threads) == 1


# Parametrize with different resource counts to excersize the worker pool:
# 1. More than number of workers
# 2. 1 resource only
# 3. Exact number of workers
# 4. More than future pool max size
# 5. Exact future pool max size
@pytest.mark.parametrize("n_resources", [8, 1, 5, 25, 20])
def test_parallelized_resource_extract_order(n_resources: int) -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"

    item_counts = [random.randrange(10, 30) for _ in range(n_resources)]
    item_ranges = []  # Create numeric ranges that each resource will yield
    # Use below to check the extraction order
    for i, n_items in enumerate(item_counts):
        if i == 0:
            start_range = 0
        else:
            start_range = sum(item_counts[:i])
        end_range = start_range + n_items
        item_ranges.append(range(start_range, end_range))

    def _sleep_duration() -> float:
        # Sleep for random duration each yield
        return random.uniform(0.005, 0.012)

    @dlt.source
    def some_source():
        def some_data(resource_num: int):
            for item in item_ranges[resource_num]:
                print(f"RESOURCE {resource_num}")
                # Sleep for a random duration each yield
                time.sleep(random.uniform(0.005, 0.012))
                yield f"item-{item}"
                print(f"RESOURCE {resource_num}:", item)

        for i in range(n_resources):
            yield dlt.resource(some_data, name=f"some_data_{i}", parallelized=True)(i)

    source = some_source()
    result = list(source)
    result = [int(item.split("-")[1]) for item in result]

    assert len(result) == sum(item_counts)

    # Check extracted results from each resource
    chunked_results = []
    start_range = 0
    for item_range in item_ranges:
        chunked_results.append([item for item in result if item in item_range])

    for i, chunk in enumerate(chunked_results):
        # All items are included
        assert len(chunk) == item_counts[i]
        assert len(set(chunk)) == len(chunk)
        # Items are extracted in order per resource
        assert chunk == sorted(chunk)
