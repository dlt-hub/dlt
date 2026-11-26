from typing import Any, AsyncIterator, Dict, Iterator, List, Set, Tuple
import time
import threading
import random
from itertools import product

import dlt, asyncio, pytest, os
from dlt.common.configuration.container import Container
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common.utils import uniq_id
from dlt.extract import DltResource
from dlt.extract.exceptions import ResourceExtractionError


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

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
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

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)

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

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
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

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
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
            execution_order.append("one")
            yield {"letter": l_}

    @dlt.resource(table_name="table2")
    def sync_resource2():
        for l_ in ["e", "f", "g"]:
            execution_order.append("two")
            yield {"letter": l_}

    @dlt.resource(table_name="table1")
    async def async_resource1():
        for l_ in ["a", "b", "c"]:
            await asyncio.sleep(1)
            execution_order.append("one")
            yield {"letter": l_}

    @dlt.resource(table_name="table2")
    async def async_resource2():
        await asyncio.sleep(0.5)
        for l_ in ["e", "f", "g"]:
            await asyncio.sleep(1)
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

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
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
    # there's a race condition when limiting async generators. to be investigated
    assert len(result) in (13, 14)


@pytest.mark.parametrize("parallelized", [True, False])
def test_parallelized_resource(parallelized: bool) -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"
    execution_order = []
    threads = set()

    @dlt.resource(parallelized=parallelized)
    def resource1():
        for l_ in ["a", "b", "c"]:
            time.sleep(0.01)
            execution_order.append("one")
            threads.add(threading.get_ident())
            yield {"letter": l_}

    @dlt.resource(parallelized=parallelized)
    def resource2():
        for l_ in ["e", "f", "g"]:
            time.sleep(0.01)
            execution_order.append("two")
            threads.add(threading.get_ident())
            yield {"letter": l_}

    @dlt.source
    def source():
        return [resource1(), resource2()]

    pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
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
        assert (
            len(threads) > 1 and threading.get_ident() not in threads
        )  # Nothing runs in main thread
    else:
        assert execution_order == ["one", "one", "one", "two", "two", "two"]
        assert threads == {threading.get_ident()}  # Everything runs in main thread


@pytest.mark.parametrize("with_config", [True, False], ids=["with-config", "no-config"])
def test_parallelized_resource_called_with_args_runs_in_pool(with_config: bool) -> None:
    """A parallelized resource that is called with arguments runs on the extract pool, also when
    it takes config values."""
    os.environ["API_URL"] = "https://api.example.com"
    threads: Set[str] = set()

    @dlt.resource(parallelized=True)
    def items_with_config(n: int, api_url: str = dlt.config.value) -> Iterator[Dict[str, Any]]:
        for i in range(n):
            threads.add(threading.current_thread().name)
            yield {"i": i, "api_url": api_url}

    @dlt.resource(parallelized=True)
    def items(n: int) -> Iterator[Dict[str, Any]]:
        for i in range(n):
            threads.add(threading.current_thread().name)
            yield {"i": i}

    resource = items_with_config if with_config else items
    dlt.pipeline("pipeline_" + uniq_id()).extract(resource(3))

    assert threads and all("extract-threads" in name for name in threads), threads


@pytest.mark.parametrize("mode", ["threads", "async", "mixed"])
def test_current_resource_is_affine_in_parallel(mode: str) -> None:
    """`dlt.current` returns the running resource, its source and its state in pool threads and
    async tasks while other resources inject their own contexts."""
    n_steps = 20
    errors: List[str] = []

    def _check(name: str, i: int) -> Dict[str, Any]:
        try:
            assert dlt.current.resource_name() == name
            assert dlt.current.resource().name == name
            assert dlt.current.source().name == "affinity"
            assert dlt.current.source_schema().name == "affinity"
            dlt.current.resource_state().setdefault("steps", []).append(i)
            with dlt.current.pipeline().destination_client():
                pass
        except Exception as ex:
            errors.append(f"{name}: {type(ex).__name__}: {ex}")
        return {"i": i}

    def _iterate_other_resource() -> None:
        # injects the source, state and section contexts of a single resource source
        assert list(dlt.resource([1, 2], name="inner")) == [1, 2]

    def _threaded(name: str, noisy: bool) -> DltResource:
        @dlt.resource(name=name, parallelized=True)
        def _gen() -> Iterator[Dict[str, Any]]:
            for i in range(n_steps):
                if noisy:
                    _iterate_other_resource()
                yield _check(name, i)

        return _gen()

    def _async(name: str, noisy: bool) -> DltResource:
        @dlt.resource(name=name)
        async def _gen() -> AsyncIterator[Dict[str, Any]]:
            for i in range(n_steps):
                if noisy:
                    _iterate_other_resource()
                await asyncio.sleep(0)
                yield _check(name, i)

        return _gen()

    def _deferred(name: str) -> DltResource:
        @dlt.defer
        def _item(i: int) -> Dict[str, Any]:
            return _check(name, i)

        return dlt.resource((_item(i) for i in range(n_steps)), name=name)

    @dlt.transformer(parallelized=True)
    def threaded_tx(item: Dict[str, Any]) -> Dict[str, Any]:
        return _check("threaded_tx", item["i"])

    @dlt.transformer
    async def async_tx(item: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0)
        return _check("async_tx", item["i"])

    @dlt.source(name="affinity")
    def affinity() -> List[DltResource]:
        numbers = dlt.resource([{"i": i} for i in range(n_steps)], name="numbers")
        resources = [numbers]
        if mode in ("threads", "mixed"):
            resources += [_threaded(f"threaded_{n}", n < 2) for n in range(4)]
            resources += [numbers | threaded_tx, _deferred("deferred")]
        if mode in ("async", "mixed"):
            resources += [_async(f"async_{n}", n < 2) for n in range(4)]
            resources += [numbers | async_tx]
        return resources

    pipeline = dlt.pipeline("pipeline_" + uniq_id(), destination="duckdb")
    source = affinity()
    pipeline.extract(source)

    assert errors == []
    writers = [name for name in source.resources if name != "numbers"]
    resources_state = pipeline.state["sources"]["affinity"]["resources"]
    assert {name: sorted(state["steps"]) for name, state in resources_state.items()} == {
        name: list(range(n_steps)) for name in writers
    }


@pytest.mark.parametrize("mode", ["threads", "async"])
def test_parallel_generator_keeps_injected_section(mode: str) -> None:
    """A context injected around `yield` stays with its parallel generator on every step."""
    n_steps = 10
    seen: List[Tuple[str, Tuple[str, ...]]] = []

    def _threaded(name: str) -> DltResource:
        @dlt.resource(name=name, parallelized=True)
        def _gen() -> Iterator[Dict[str, Any]]:
            with inject_section(ConfigSectionContext(sections=("own", name))):
                for i in range(n_steps):
                    seen.append((name, Container()[ConfigSectionContext].sections))
                    yield {"i": i}

        return _gen()

    def _async(name: str) -> DltResource:
        @dlt.resource(name=name)
        async def _gen() -> AsyncIterator[Dict[str, Any]]:
            with inject_section(ConfigSectionContext(sections=("own", name))):
                for i in range(n_steps):
                    seen.append((name, Container()[ConfigSectionContext].sections))
                    await asyncio.sleep(0)
                    yield {"i": i}

        return _gen()

    make = _threaded if mode == "threads" else _async
    dlt.pipeline("pipeline_" + uniq_id()).extract([make(f"res_{n}") for n in range(4)])

    assert len(seen) == 4 * n_steps
    assert [sections for name, sections in seen if sections != ("own", name)] == []


# Parametrize with different resource counts to excersize the worker pool:
# 1. More than number of workers
# 2. 1 resource only
# 3. Exact number of workers
# 4. More than future pool max size
# 5. Exact future pool max size
@pytest.mark.parametrize(
    "n_resources,next_item_mode", product([8, 1, 5, 25, 20], ["fifo", "round_robin"])
)
def test_parallelized_resource_extract_order(n_resources: int, next_item_mode: str) -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = next_item_mode

    threads = set()

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

    @dlt.source
    def some_source():
        def some_data(resource_num: int):
            for item in item_ranges[resource_num]:
                threads.add(threading.get_ident())
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

    assert len(threads) >= min(2, n_resources) and threading.get_ident() not in threads


def test_test_parallelized_resource_transformers() -> None:
    item_count = 6
    threads = set()
    transformer_threads = set()

    @dlt.resource(parallelized=True)
    def pos_data():
        for i in range(1, item_count + 1):
            threads.add(threading.get_ident())
            time.sleep(0.1)
            yield i

    @dlt.resource(parallelized=True)
    def neg_data():
        for i in range(-1, -item_count - 1, -1):
            threads.add(threading.get_ident())
            time.sleep(0.1)
            yield i

    @dlt.transformer(parallelized=True)
    def multiply(item):
        transformer_threads.add(threading.get_ident())
        time.sleep(0.05)
        yield item * 10

    @dlt.source
    def some_source():
        return [
            neg_data | multiply.with_name("t_a"),
            pos_data | multiply.with_name("t_b"),
        ]

    result = list(some_source())

    expected_result = [i * 10 for i in range(-item_count, item_count + 1)]
    expected_result.remove(0)

    assert sorted(result) == expected_result
    # Nothing runs in main thread
    assert threads and threading.get_ident() not in threads
    assert transformer_threads and threading.get_ident() not in transformer_threads

    threads = set()
    transformer_threads = set()

    @dlt.transformer(parallelized=True)  # type: ignore[no-redef]
    def multiply(item):
        # Transformer that is not a generator
        transformer_threads.add(threading.get_ident())
        time.sleep(0.05)
        return item * 10

    @dlt.source  # type: ignore[no-redef]
    def some_source():
        return [
            neg_data | multiply.with_name("t_a"),
            pos_data | multiply.with_name("t_b"),
        ]

    result = list(some_source())

    expected_result = [i * 10 for i in range(-item_count, item_count + 1)]
    expected_result.remove(0)

    assert sorted(result) == expected_result

    # Nothing runs in main thread
    assert len(threads) > 1 and threading.get_ident() not in threads
    assert len(transformer_threads) > 1 and threading.get_ident() not in transformer_threads


def test_parallelized_resource_bare_generator() -> None:
    main_thread = threading.get_ident()
    threads = set()

    def pos_data():
        for i in range(1, 6):
            threads.add(threading.get_ident())
            time.sleep(0.01)
            yield i

    def neg_data():
        for i in range(-1, -6, -1):
            threads.add(threading.get_ident())
            time.sleep(0.01)
            yield i

    @dlt.source
    def some_source():
        return [
            # Resources created from generators directly (not generator functions) can be parallelized
            dlt.resource(pos_data(), parallelized=True, name="pos_data"),
            dlt.resource(neg_data(), parallelized=True, name="neg_data"),
        ]

    result = list(some_source())

    assert len(threads) > 1 and main_thread not in threads
    assert set(result) == {1, 2, 3, 4, 5, -1, -2, -3, -4, -5}
    assert len(result) == 10


def test_parallelized_resource_wrapped_generator() -> None:
    threads = set()

    def some_data():
        for i in range(1, 6):
            time.sleep(0.01)
            threads.add(threading.get_ident())
            yield i

    def some_data2():
        for i in range(-1, -6, -1):
            time.sleep(0.01)
            threads.add(threading.get_ident())
            yield i

    @dlt.source
    def some_source():
        # Bound resources result in a wrapped generator function,
        return [
            dlt.resource(some_data, parallelized=True, name="some_data")(),
            dlt.resource(some_data2, parallelized=True, name="some_data2")(),
        ]

    source = some_source()

    result = list(source)

    assert len(threads) > 1 and threading.get_ident() not in threads
    assert set(result) == {1, 2, 3, 4, 5, -1, -2, -3, -4, -5}


def test_parallelized_resource_exception_pool_is_closed() -> None:
    """Checking that futures pool is closed before generators are closed when a parallel resource raises.
    For now just checking that we don't get any "generator is already closed" errors, as would happen
    when futures aren't cancelled before closing generators.
    """

    def some_data():
        for i in range(1, 6):
            time.sleep(0.1)
            yield i

    def some_data2():
        for i in range(1, 6):
            time.sleep(0.005)
            yield i
            if i == 3:
                raise RuntimeError("we have failed")

    @dlt.source
    def some_source():
        yield dlt.resource(some_data, parallelized=True, name="some_data")
        yield dlt.resource(some_data2, parallelized=True, name="some_data2")

    source = some_source()

    with pytest.raises(ResourceExtractionError) as einfo:
        list(source)

    assert "we have failed" in str(einfo.value)
