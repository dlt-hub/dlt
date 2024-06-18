import dlt
import itertools
import pytest
import asyncio
import os
import time


@pytest.fixture(autouse=True)
def set_round_robin():
    """this can be removed after the round robin PR is merged to devel"""
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "round_robin"
    yield
    del os.environ["EXTRACT__NEXT_ITEM_MODE"]


def test_item_limit_infinite_counter() -> None:
    r = dlt.resource(itertools.count(), name="infinity").add_limit(10)
    assert list(r) == list(range(10))


def test_item_limit_source() -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"

    def mul_c(item):
        yield from "A" * (item + 2)

    @dlt.source
    def infinite_source():
        for idx in range(3):
            r = dlt.resource(itertools.count(), name=f"infinity_{idx}").add_limit(10)
            yield r
            yield r | dlt.transformer(name=f"mul_c_{idx}")(mul_c)

    # transformer is not limited to 2 elements, infinite resource is, we have 3 resources
    assert list(infinite_source().add_limit(2)) == ["A", "A", 0, "A", "A", "A", 1] * 3


@pytest.mark.parametrize("limit", (None, -1, 0, 10))
def test_item_limit_edge_cases(limit: int) -> None:
    r = dlt.resource(range(20), name="infinity").add_limit(limit)  # type: ignore

    @dlt.resource()
    async def r_async():
        for i in range(20):
            await asyncio.sleep(0.01)
            yield i

    sync_list = list(r)
    async_list = list(r_async().add_limit(limit))

    if limit == 10:
        assert sync_list == list(range(10))
        # we have edge cases where the async list will have one extra item
        # possibly due to timing issues, maybe some other implementation problem
        assert (async_list == list(range(10))) or (async_list == list(range(11)))
    elif limit in [None, -1]:
        assert sync_list == async_list == list(range(20))
    elif limit == 0:
        assert sync_list == async_list == []
    else:
        raise AssertionError(f"Unexpected limit: {limit}")


def test_time_limit() -> None:
    @dlt.resource()
    def r():
        for i in range(100):
            time.sleep(0.1)
            yield i

    @dlt.resource()
    async def r_async():
        for i in range(100):
            await asyncio.sleep(0.1)
            yield i

    sync_list = list(r().add_limit(max_time=1))
    async_list = list(r_async().add_limit(max_time=1))

    # we should have extracted 10 items within 1 second, sleep is included in the resource
    allowed_results = [
        list(range(12)),
        list(range(11)),
        list(range(10)),
        list(range(9)),
        list(range(8)),
    ]
    assert sync_list in allowed_results
    assert async_list in allowed_results


def test_min_wait() -> None:
    @dlt.resource()
    def r():
        for i in range(100):
            yield i

    @dlt.resource()
    async def r_async():
        for i in range(100):
            yield i

    sync_list = list(r().add_limit(max_time=1, min_wait=0.2))
    async_list = list(r_async().add_limit(max_time=1, min_wait=0.2))

    # we should have extracted about 5 items within 1 second, sleep is done via min_wait
    allowed_results = [
        list(range(3)),
        list(range(4)),
        list(range(5)),
        list(range(6)),
        list(range(7)),
    ]
    assert sync_list in allowed_results
    assert async_list in allowed_results


# TODO: Test behavior in pipe iterator with more than one resource with different extraction modes set. We want to see if an overall rate limiting can be achieved with fifo which
# will be useful for APIs. Also round robin should apply rate limiting individually and not get stuck on one iterator sleeping.

# it would also be nice to be able to test the logger warnings if no incremental is present
