"""The Default Pipeline Template provides a simple starting point for your dlt pipeline"""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt
from dlt.common import Decimal


@dlt.resource(primary_key="id")
def customers():
    """Load customer data from a simple python list."""
    yield [
        {"id": 1, "name": "simon", "city": "berlin"},
        {"id": 2, "name": "violet", "city": "london"},
        {"id": 3, "name": "tammo", "city": "new york"},
    ]


@dlt.resource(primary_key="id")
def inventory():
    """Load inventory data from a simple python list."""
    yield [
        {"id": 1, "name": "apple", "price": Decimal("1.50")},
        {"id": 2, "name": "banana", "price": Decimal("1.70")},
        {"id": 3, "name": "pear", "price": Decimal("2.50")},
    ]


@dlt.source
def fruitshop():
    """A source function groups all resources into one schema."""
    return customers(), inventory()


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
