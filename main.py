import dlt

# Step 1: Yield user IDs
@dlt.resource(keep_history=True)
def user_ids():
    yield from [1001, 1002, 1003]

# Step 2: Fetch orders for each user
@dlt.transformer(data_from=user_ids)
def fetch_orders(user_id: int):
    # Simulated orders per user
    for order_id in [f"{user_id}-A", f"{user_id}-B"]:
        yield order_id

# Step 3: Fetch payments for each order, but retain original user_id via history
@dlt.transformer(data_from=fetch_orders)
def fetch_payments(order_id: str, history: dlt.History = None):
    user_id = history.user_ids  # Access top-level parent
    # request.get(user_id, order_id)
    yield {
        "payment_status": "PAID"
    }

# Run the pipeline and print output
for payment in fetch_payments():
    print(payment)
