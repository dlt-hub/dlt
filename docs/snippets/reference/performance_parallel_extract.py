import os
os.environ["DLT_PROJECT_DIR"] = os.path.dirname(__file__)
# @@@SNIPSTART parallel_extract_callables
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
# @@@SNIPEND
# @@@SNIPSTART parallel_extract_awaitables
import asyncio

@dlt.transformer
async def a_get_details(item_id):
    # simulate a slow REST API where you wait 0.3 sec for each item
    await asyncio.sleep(0.3)
    print(f"item_id {item_id} in thread {currentThread().name}")
    # just return the results, if you yield, generator will be evaluated in main thread
    return {"row": item_id}


print(list(list_items(0, 10) | a_get_details))
# @@@SNIPEND