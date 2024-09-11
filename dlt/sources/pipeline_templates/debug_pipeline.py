"""The Debug Pipeline Template will load a column with each datatype to your destination."""

# mypy: disable-error-code="no-untyped-def,arg-type"

import dlt

from dlt.common import Decimal


@dlt.resource(write_disposition="append", name="all_datatypes")
def resource():
    """this is the test data for loading validation, delete it once you yield actual data"""
    yield [
        {
            "col1": 989127831,
            "col2": 898912.821982,
            "col3": True,
            "col4": "2022-05-23T13:26:45.176451+00:00",
            "col5": "string data \n \r  🦆",
            "col6": Decimal("2323.34"),
            "col7": b"binary data \n \r ",
            "col8": 2**56 + 92093890840,
            "col9": {
                "json": [1, 2, 3, "a"],
                "link": (
                    "?commen\ntU\nrn=urn%3Ali%3Acomment%3A%28acti\012 \6"
                    " \\vity%3A69'08444473\n\n551163392%2C6n \r 9085"
                ),
            },
            "col10": "2023-02-27",
            "col11": "13:26:45.176451",
        }
    ]


@dlt.source
def source():
    """A source function groups all resources into one schema."""
    return resource()


def load_all_datatypes() -> None:
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name="debug",
        destination="duckdb",
        dataset_name="debug_data",
    )

    data = list(source().all_datatypes)

    # print the data yielded from resource without loading it
    print(data)  # noqa: T201

    # run the pipeline with your parameters
    load_info = pipeline.run(source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_all_datatypes()
