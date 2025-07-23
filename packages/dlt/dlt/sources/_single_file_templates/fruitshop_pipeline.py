"""The Default Pipeline Template provides a simple starting point for your dlt pipeline with locally generated data"""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt
import random
from datetime import datetime, timedelta  # noqa: I251

from dlt.common import Decimal


# NOTE: we add a custom hint to the name column to indicate that it contains PII (personally identifiable information)
@dlt.resource(primary_key="id", columns={"name": {"x-annotation-pii": True}})  # type: ignore[typeddict-unknown-key]
def customers():
    """Load customer data from three cities from a simple python list."""
    yield [
        {"id": 1, "name": "simon", "city": "berlin"},
        {"id": 2, "name": "violet", "city": "montreal"},
        {"id": 3, "name": "tammo", "city": "new york"},
        {"id": 4, "name": "dave", "city": "berlin"},
        {"id": 5, "name": "andrea", "city": "montreal"},
        {"id": 6, "name": "marcin", "city": "new york"},
        {"id": 7, "name": "sarah", "city": "berlin"},
        {"id": 8, "name": "miguel", "city": "new york"},
        {"id": 9, "name": "yuki", "city": "montreal"},
        {"id": 10, "name": "olivia", "city": "berlin"},
        {"id": 11, "name": "raj", "city": "montreal"},
        {"id": 12, "name": "sofia", "city": "new york"},
        {"id": 13, "name": "chen", "city": "berlin"},
    ]


@dlt.resource(primary_key="id")
def inventory_categories():
    """Load inventory categories from a simple python list."""
    yield [
        {"id": 1, "name": "pomes"},
        {"id": 2, "name": "berries"},
        {"id": 3, "name": "stone fruits"},
    ]


@dlt.resource(primary_key="id")
def inventory():
    """Load inventory data from a simple python list."""
    yield [
        {"id": 1, "name": "apple", "price": Decimal("1.50"), "category_id": 1},
        {"id": 2, "name": "pear", "price": Decimal("1.70"), "category_id": 1},
        {"id": 3, "name": "cherry", "price": Decimal("2.50"), "category_id": 2},
        {"id": 4, "name": "strawberry", "price": Decimal("1.00"), "category_id": 2},
        {"id": 5, "name": "peach", "price": Decimal("1.20"), "category_id": 3},
        {"id": 6, "name": "plum", "price": Decimal("1.30"), "category_id": 3},
    ]


@dlt.resource(primary_key="id")
def purchases():
    """Generate 100 seeded random purchases between Mon. Oct 1 and Sun. Oct 14, 2018."""
    random.seed(42)
    start_date = datetime(2018, 10, 1)
    customers_ids = list(range(1, 14))  # 13 customers
    inventory_ids = list(range(1, 7))  # 6 inventory items

    yield [
        {
            "id": i + 1,
            "customer_id": random.choice(customers_ids),
            "inventory_id": random.choice(inventory_ids),
            "quantity": random.randint(1, 5),
            "date": (start_date + timedelta(days=random.randint(0, 13))).strftime("%Y-%m-%d"),
        }
        for i in range(100)
    ]


@dlt.source
def fruitshop():
    """A source function groups all resources into one schema."""
    return customers(), inventory_categories(), inventory(), purchases()


def load_shop() -> None:
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name="fruitshop",
        destination="duckdb",
        dataset_name="fruitshop_data",
    )

    load_info = p.run(fruitshop())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_shop()
