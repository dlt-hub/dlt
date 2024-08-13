"""Example of a composable pipeline"""

import dlt
import os
import random
from dlt.destinations import filesystem, duckdb

# fixtures
customers = [
    {"id": 1, "name": "dave"},
    {"id": 2, "name": "marcin"},
    {"id": 3, "name": "anton"},
    {"id": 4, "name": "alena"},
]

products = [
    {"name": "apple", "price": 1},
    {"name": "pear", "price": 2},
    {"name": "banana", "price": 3},
    {"name": "schnaps", "price": 10},
]

if __name__ == "__main__":
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"

    #
    # 1. let's load some stuff to a duckdb pipeline (standin for a remote location)
    #
    duck_pipeline = dlt.pipeline(
        pipeline_name="warehouse", destination=duckdb(credentials="warehouse.duckdb")
    )

    @dlt.resource(write_disposition="replace", table_name="customers")
    def c():
        yield from customers

    @dlt.resource(write_disposition="replace", table_name="orders")
    def o():
        order_no = 0
        # every customer orders 4 things everyday
        for weekday in ["monday", "tuesday", "wednesday"]:
            for customer in customers:
                for i in range(4):
                    order_no += 1
                    product = random.choice(products)
                    yield {
                        "order_day": weekday,
                        "id": order_no,
                        "customer_id": customer["id"],
                        "product": product["name"],
                        "price": product["price"],
                    }

    # run and print result
    print("RUNNING WAREHOUSE INGESTION")
    print(duck_pipeline.run([c(), o()]))
    print(duck_pipeline.dataset().customers.df())
    print(duck_pipeline.dataset().orders.df())
    print("===========================")

    #
    # 2. now we want a local snapshot of the customers and all orders on tuesday in a datalake
    #
    lake_pipeline = dlt.pipeline(
        pipeline_name="local_lake", destination=filesystem(bucket_url="./local_lake")
    )

    print("RUNNING LOCAL SNAPSHOT EXTRACTION")
    lake_pipeline.run(
        duck_pipeline.dataset().customers.iter_df(500),
        loader_file_format="parquet",
        table_name="customers",
        write_disposition="replace",
    )
    lake_pipeline.run(
        duck_pipeline.dataset().query(
            "SELECT * FROM orders WHERE orders.order_day = 'tuesday'"
        ).iter_df(500),
        loader_file_format="parquet",
        table_name="orders",
        write_disposition="replace",
    )

    print(lake_pipeline.dataset().customers.df())
    print(lake_pipeline.dataset().orders.df())
    print("===========================")

    #
    # 3. now we create a denormalized table locally
    #

    print("RUNNING DENORMALIZED TABLE EXTRACTION")
    denom_pipeline = dlt.pipeline(
        pipeline_name="denom_lake", destination=filesystem(bucket_url="./denom_lake")
    )

    denom_pipeline.run(
        lake_pipeline.dataset().query(
            sql=(
                "SELECT orders.*, customers.name FROM orders LEFT JOIN customers ON"
                " orders.customer_id = customers.id"
            ),
        ).iter_df(500),
        loader_file_format="parquet",
        table_name="customers",
        write_disposition="replace",
    )
    print(denom_pipeline.dataset().customers.df())
