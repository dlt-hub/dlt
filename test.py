
import asyncio, inspect
from typing import Awaitable
from asyncio import Future

async def async_gen_resource():
    for l in ["a", "b", "c"]:
        # await asyncio.sleep(0.1)
        yield {"async_gen": 1, "letter": l}


async def run() -> None:
    gen = async_gen_resource()
    result = []
    try:
        while item := await gen.__anext__():
            result.append(item)#
    except StopAsyncIteration:
        return [result]


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    print(loop.run_until_complete(run()))