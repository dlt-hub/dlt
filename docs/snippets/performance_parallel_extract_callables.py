"""
description: Extracting callables in parallel
tags: performance, extract, parallelization
"""

import dlt

if __name__ == "__main__":

    import time
    from threading import current_thread

    @dlt.resource(parallelized=True)
    def list_users(n_users):
        for i in range(1, 1 + n_users):
            # Simulate network delay of a rest API call fetching a page of items
            if i % 10 == 0:
                time.sleep(0.1)
            yield i

    @dlt.transformer(parallelized=True)
    def get_user_details(user_id):
        # Transformer that fetches details for users in a page
        time.sleep(0.1)  # Simulate latency of a rest API call
        print(f"user_id {user_id} in thread {current_thread().name}")
        return {"entity": "user", "id": user_id}

    @dlt.resource(parallelized=True)
    def list_products(n_products):
        for i in range(1, 1 + n_products):
            if i % 10 == 0:
                time.sleep(0.1)
            yield i

    @dlt.transformer(parallelized=True)
    def get_product_details(product_id):
        time.sleep(0.1)
        print(f"product_id {product_id} in thread {current_thread().name}")
        return {"entity": "product", "id": product_id}

    @dlt.source
    def api_data():
        return [
            list_users(24) | get_user_details,
            list_products(32) | get_product_details,
        ]

    # evaluate the pipeline and print all the items
    # sources are iterators and they are evaluated in the same way in the pipeline.run
    result = list(api_data())
    
    assert len(result) == 56