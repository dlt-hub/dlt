"""The Arrow Pipeline Template will show how to load and transform arrow tables."""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt
import time
import pyarrow as pa


def create_example_arrow_table() -> pa.Table:
    return pa.Table.from_pylist([{"name": "tom", "age": 25}, {"name": "angela", "age": 23}])


@dlt.resource(write_disposition="append", name="people")
def resource():
    """One resource function will materialize as a table in the destination, wie yield example data here"""
    yield create_example_arrow_table()


def add_updated_at(item: pa.Table):
    """Map function to add an updated at column to your incoming data."""
    column_count = len(item.columns)
    # you will receive and return and arrow table
    return item.set_column(column_count, "updated_at", [[time.time()] * item.num_rows])


# apply transformer to resource
resource.add_map(add_updated_at)


@dlt.source
def source():
    """A source function groups all resources into one schema."""
    # return resources
    return resource()


def load_arrow_tables() -> None:
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name="arrow",
        destination="duckdb",
        dataset_name="arrow_data",
    )

    data = list(source().people)

    # print the data yielded from resource without loading it
    print(data)  # noqa: T201

    # run the pipeline with your parameters
    load_info = pipeline.run(source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_arrow_tables()
