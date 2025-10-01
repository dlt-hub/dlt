import dlt

@dlt.resource(keep_history=True)
def users():
    yield {"id": 1, "name": "Alice"}
    yield {"id": 2, "name": "Bob"}

@dlt.transformer(data_from=users)
def items(user):
    yield from [f"item_{i}_for_{user['name']}" for i in range(2)]

@dlt.transformer(data_from=items)
def sub_items(item, history: dlt.History = None):
    # but let's say we now need to access the original user data for this certain request
    user = history[users]
    yield from [
        {"user": user, "item": f"{item}_sub_{i}"}
        for i in range(2)
    ]

for sub_item in sub_items():
    print(sub_item)
