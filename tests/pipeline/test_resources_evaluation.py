from typing import Any

import dlt, asyncio, pytest, os, threading, inspect, time
from functools import wraps


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
        assert False, "Should not reach here"


def test_limit_async_resource() -> None:
    @dlt.resource(table_name="table1")
    async def async_resource1():
        for l_ in range(20):
            print(l_)
            await asyncio.sleep(0.1)
            yield {"index": l_}

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", full_refresh=True)
    pipeline_1.run(async_resource1().add_limit(13))

    with pipeline_1.sql_client() as c:
        with c.execute_query("SELECT * FROM table1") as cur:
            rows = list(cur.fetchall())
            assert len(rows) == 13


@pytest.mark.parametrize("parallelized", [True, False])
def test_async_decorator_experiment(parallelized) -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"
    execution_order = []
    threads = set()

    def parallelize(f) -> Any:
        exhausted = False
        lock = threading.Lock()

        """converts regular itarable to generator of functions that can be run in parallel in the pipe"""
        @wraps(f)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            gen = f(*args, **kwargs)
            # unpack generator
            if inspect.isfunction(gen):
                gen = gen()
            # if we have an async gen, no further action is needed
            if inspect.isasyncgen(gen):
                raise Exception("Already async gen")

            # get next item from generator
            def _gen():
                nonlocal exhausted
                with lock:
                    # await asyncio.sleep(0.1)
                    try:
                        return next(gen)
                    # on stop iteration mark as exhausted
                    except StopIteration:
                        exhausted = True
                        return None
            try:
                while not exhausted:
                    while lock.locked():
                        yield None
                    yield _gen
            except GeneratorExit:
                # clean up inner generator
                gen.close()

        return _wrap

    @parallelize
    def resource1():
        for l_ in ["a", "b", "c"]:
            time.sleep(0.1)
            nonlocal execution_order
            execution_order.append("one")
            threads.add(threading.get_ident())
            yield {"letter": l_}

    @parallelize
    def resource2():
        time.sleep(0.05)
        for l_ in ["e", "f", "g"]:
            time.sleep(0.1)
            nonlocal execution_order
            execution_order.append("two")
            threads.add(threading.get_ident())
            yield {"letter": l_}

    @dlt.source
    def source():
        if parallelized:
            return [resource1(), resource2()]
        else:  # return unwrapped resources
            return [resource1.__wrapped__(), resource2.__wrapped__()]

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
