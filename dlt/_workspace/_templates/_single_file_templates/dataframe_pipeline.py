"""The DataFrame Pipeline Template will show how to load and transform pandas dataframes."""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt
import time
import pandas as pd


def create_example_dataframe() -> pd.DataFrame:
    return pd.DataFrame({"name": ["tom", "angela"], "age": [25, 23]}, columns=["name", "age"])


@dlt.resource(write_disposition="append", name="people")
def resource():
    """One resource function will materialize as a table in the destination, wie yield example data here"""
    yield create_example_dataframe()


def add_updated_at(item: pd.DataFrame):
    """Map function to add an updated at column to your incoming data."""
    column_count = len(item.columns)
    # you will receive and return and arrow table
    item.insert(column_count, "updated_at", [time.time()] * 2, True)
    return item


# apply tranformer to resource
resource.add_map(add_updated_at)


@dlt.source
def source():
    """A source function groups all resources into one schema."""

    # return resources
    return resource()


def load_dataframe() -> None:
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name="dataframe",
        destination="duckdb",
        dataset_name="dataframe_data",
    )

    data = list(source().people)

    # print the data yielded from resource without loading it
    print(data)  # noqa: T201

    # run the pipeline with your parameters
    load_info = pipeline.run(source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_dataframe()
