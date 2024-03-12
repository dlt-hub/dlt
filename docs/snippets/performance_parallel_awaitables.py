"""
description: Extracting awaitables in parallel
tags: performance, extract, parallelization
"""

import dlt

if __name__ == "__main__":

    import asyncio
    from threading import current_thread

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
        print(f"item_id {item_id} in thread {current_thread().name}")
        # just return the results, if you yield, generator will be evaluated in main thread
        return {"row": item_id}

    result = list(a_list_items(0, 10) | a_get_details)
    print(result)

    assert len(result) == 10