"""The Arrow Pipeline Template will show how to load and transform arrow tables."""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt
import time
import pyarrow as pa


@dlt.resource(write_disposition="append", name="people")
def resource():
    # here we create an arrow table from a list of python objects for demonstration
    # in the real world you will have a source that already has arrow tables
    yield pa.Table.from_pylist([{"name": "tom", "age": 25}, {"name": "angela", "age": 23}])


def add_updated_at(item: pa.Table):
    """Map function to add an updated at column to your incoming data."""
    column_count = len(item.columns)
    # you will receive and return and arrow table
    return item.set_column(column_count, "updated_at", [[time.time()] * item.num_rows])


@dlt.source
def source():
    """A source function groups all resources into one schema."""

    # apply tranformer to source
    resource.add_map(add_updated_at)

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
