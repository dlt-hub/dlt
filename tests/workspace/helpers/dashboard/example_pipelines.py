#
# A list of pipelines that are execute and are in various states for testing the dashboard
# TODO: generalize these and make them available for other tests
# TODO: consolidate these test pipelines with the ones in tests/e2e/helpers/dashboard
#

import dlt
import pytest
from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
)

import tempfile

SUCCESS_PIPELINE_DUCKDB = "success_pipeline_duckdb"
SUCCESS_PIPELINE_FILESYSTEM = "success_pipeline_filesystem"
EXTRACT_EXCEPTION_PIPELINE = "extract_exception_pipeline"
NORMALIZE_EXCEPTION_PIPELINE = "normalize_exception_pipeline"
NEVER_RAN_PIPELINE = "never_ran_pipline"
LOAD_EXCEPTION_PIPELINE = "load_exception_pipeline"
NO_DESTINATION_PIPELINE = "no_destination_pipeline"

ALL_PIPELINES = [
    SUCCESS_PIPELINE_DUCKDB,
    EXTRACT_EXCEPTION_PIPELINE,
    NORMALIZE_EXCEPTION_PIPELINE,
    NEVER_RAN_PIPELINE,
    LOAD_EXCEPTION_PIPELINE,
    NO_DESTINATION_PIPELINE,
    SUCCESS_PIPELINE_FILESYSTEM,
]

PIPELINES_WITH_EXCEPTIONS = [
    EXTRACT_EXCEPTION_PIPELINE,
    NORMALIZE_EXCEPTION_PIPELINE,
    LOAD_EXCEPTION_PIPELINE,
]
PIPELINES_WITH_LOAD = [SUCCESS_PIPELINE_DUCKDB, SUCCESS_PIPELINE_FILESYSTEM]


def run_success_pipeline(pipeline: dlt.Pipeline):
    """
    Create a success pipeline with
       * fruitshop dataset
       * child table
       * second schema
       * incremental column
    """
    pipeline.run(fruitshop_source(), schema=dlt.Schema("fruitshop"))

    # we overwrite the purchases table to have a child table and an incomplete column
    @dlt.resource(  # type: ignore
        primary_key="id",
        write_disposition="merge",
        columns={"incomplete": {}, "id": {"x-custom": "foo"}},
    )
    def purchases(inc_id=dlt.sources.incremental("id")):
        """Load purchases data from a simple python list."""
        yield [
            {
                "id": 1,
                "customer_id": 1,
                "inventory_id": 1,
                "quantity": 1,
                "child": [
                    {"id": 1, "name": "child 1"},
                    {"id": 2, "name": "child 2"},
                    {"id": 3, "name": "child 3"},
                ],
            },
            {"id": 2, "customer_id": 1, "inventory_id": 2, "quantity": 2},
            {"id": 3, "customer_id": 2, "inventory_id": 3, "quantity": 3},
        ]

    pipeline.run(purchases(), schema=dlt.Schema("fruitshop"))

    # write something to another schema so we have multiple schemas
    customers = fruitshop_source().customers

    customers.apply_hints(
        nested_hints={
            ("child",): dlt.mark.make_nested_hints(columns=[{"name": "name", "data_type": "text"}]),
            ("child", "child_inner"): dlt.mark.make_nested_hints(
                columns=[{"name": "name", "data_type": "text"}]
            ),
        },
    )
    pipeline.run(
        customers,
        schema=dlt.Schema("fruitshop_customers"),
        table_name="other_customers",
    )


def create_success_pipeline_duckdb(pipelines_dir: str = None, db_location: str = None):
    """Create a test pipeline with in memory duckdb destination, properties see `run_success_pipeline`"""
    import duckdb

    pipeline = dlt.pipeline(
        pipeline_name=SUCCESS_PIPELINE_DUCKDB,
        pipelines_dir=pipelines_dir,
        destination=dlt.destinations.duckdb(
            credentials=duckdb.connect(db_location) if db_location else None
        ),
    )

    run_success_pipeline(pipeline)

    return pipeline


def create_success_pipeline_filesystem(
    pipelines_dir: str = None, bucket_url: str = "_storage/data"
):
    """Create a test pipeline with filesystem destination, properties see `run_success_pipeline`"""

    pipeline = dlt.pipeline(
        pipeline_name=SUCCESS_PIPELINE_FILESYSTEM,
        pipelines_dir=pipelines_dir,
        destination=dlt.destinations.filesystem(bucket_url=bucket_url),
    )

    run_success_pipeline(pipeline)

    return pipeline


def create_extract_exception_pipeline(pipelines_dir: str = None):
    """Create a test pipeline with duckdb destination, raises an exception in the extract step"""
    import duckdb

    pipeline = dlt.pipeline(
        pipeline_name=EXTRACT_EXCEPTION_PIPELINE,
        pipelines_dir=pipelines_dir,
        destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
    )

    @dlt.resource
    def broken_resource():
        raise AssertionError("I am broken")

    with pytest.raises(Exception):
        pipeline.run(broken_resource())

    return pipeline


def create_normalize_exception_pipeline(pipelines_dir: str = None):
    """Create a test pipeline with duckdb destination, raises an exception in the normalize step"""
    import duckdb

    pipeline = dlt.pipeline(
        pipeline_name=NORMALIZE_EXCEPTION_PIPELINE,
        pipelines_dir=pipelines_dir,
        destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
    )

    @dlt.resource
    def data_with_type_conflict():
        # First yield double, then string for same column - causes normalize failure with strict schema contract
        yield [{"id": 1, "value": 123.4}]
        yield [{"id": 2, "value": "string"}]

    with pytest.raises(Exception):
        pipeline.run(
            data_with_type_conflict(),
            schema=dlt.Schema("fruitshop"),
            table_name="items",
            schema_contract={"data_type": "freeze"},  # Strict mode - fail on type conflicts
        )

    return pipeline


def create_never_ran_pipeline(pipelines_dir: str = None):
    """Create a test pipeline with duckdb destination which never was run"""
    import duckdb

    pipeline = dlt.pipeline(
        pipeline_name=NEVER_RAN_PIPELINE,
        pipelines_dir=pipelines_dir,
        destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
    )

    return pipeline


def create_load_exception_pipeline(pipelines_dir: str = None):
    """Create a test pipeline with duckdb destination, raises an exception in the load step"""

    @dlt.destination
    def failing_destination(one, two):
        raise DestinationTerminalException()

    pipeline = dlt.pipeline(
        pipeline_name=LOAD_EXCEPTION_PIPELINE,
        pipelines_dir=pipelines_dir,
        destination=dlt.destinations.dummy(timeout=0.1),
    )

    with pytest.raises(Exception):
        pipeline.run([1, 2, 3], table_name="items", schema=dlt.Schema("fruitshop"))

    return pipeline


def create_no_destination_pipeline(pipelines_dir: str = None):
    """Create a test pipeline with no destination"""
    pipeline = dlt.pipeline(
        pipeline_name=NO_DESTINATION_PIPELINE,
        pipelines_dir=pipelines_dir,
    )
    return pipeline

    pipeline.extract(fruitshop_source())

    return pipeline


# NOTE: this sript can be run to create the test pipelines globally for manual testing of the dashboard app and cli
if __name__ == "__main__":
    create_success_pipeline_duckdb()
    create_success_pipeline_filesystem()
    create_extract_exception_pipeline()
    create_normalize_exception_pipeline()
    create_never_ran_pipeline()
    create_load_exception_pipeline()
    create_no_destination_pipeline()
