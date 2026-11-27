"""Sample code whose `code` input locations are checked by `tests/pipeline/test_data_locations.py`.

Run with `python -m` it extracts `shop` and prints the input locations of the module run as `__main__`.
"""

from typing import Any

import dlt
from dlt.common import json
from dlt.common.utils import uniq_id


def rows() -> Any:
    yield [{"id": 1}]


async def async_rows() -> Any:
    yield {"id": 1}


def passthrough(item: Any) -> Any:
    yield item


@dlt.resource
def users() -> Any:
    yield [{"id": 1}]


@dlt.transformer(data_from=users)
def enrich(item: Any) -> Any:
    yield item


@dlt.source
def shop() -> Any:
    return users, enrich, dlt.resource([{"id": 1}], name="static")


@dlt.source
async def async_shop() -> Any:
    return users


def inner_resource() -> Any:
    @dlt.resource
    def inner() -> Any:
        yield [{"id": 1}]

    return inner()


if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="code_location_" + uniq_id())
    info = pipeline.extract(shop())
    print(json.dumps(info.metrics[info.loads_ids[0]][0]["inputs"]))
