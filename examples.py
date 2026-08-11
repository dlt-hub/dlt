import dlt
from dlt.hub import data_quality as dq


@dq.with_checks(
    dq.checks.is_in("name", ["Bob", "Charlie"]),
)
@dq.with_metrics(
    dq.metrics.table.row_count(),
)
@dlt.resource
def customers():
    yield from [
        {"id": 1, "Name": "Alice", "tags": ["a", "b"]},
        {"id": 2, "Name": "Bob", "tags": []},
    ]


def data_quality_tables() -> None:
    """this does multiple .extract() and .normalize() calls with a single .load() call.

    The dashboard properly show the 2 extract, 2 normalize, and 1 load as "1 run". It
    lists the schema `_dlt_dq_schema`, but it fails to read data in the SQL editor.
    """
    dataset_name = "local_dq"

    pipeline = dlt.pipeline(
        "local_dq",
        destination=dlt.destinations.filesystem(bucket_url="."),
        dataset_name=dataset_name,
    )

    pipeline.extract(customers)
    pipeline.normalize()

    local_dataset = pipeline.local_dataset()
    pipeline.extract(
        [
            dq.data_quality_checks(local_dataset),
            dq.data_quality_metrics(local_dataset)
        ],
    )
    pipeline.normalize()

    pipeline.load()
    #
    dataset = pipeline.dataset()
    print(dataset.row_counts().df())
    print()

    print(dataset("SELECT * FROM _dlt_checks", _execute_raw_query=True).df())
    print()

    print(dataset("SELECT * FROM _dlt_dq_metrics", _execute_raw_query=True).df())


if __name__ == "__main__":
    data_quality_tables()
