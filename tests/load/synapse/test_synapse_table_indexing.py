import os
import pytest
from typing import Iterator, List, Any, Union

import dlt
from dlt.common.schema import TColumnSchema

from dlt.destinations.adapters import synapse_adapter
from dlt.destinations.impl.synapse.synapse_adapter import TTableIndexType

from tests.load.utils import TABLE_UPDATE, TABLE_ROW_ALL_DATA_TYPES
from tests.load.synapse.utils import get_storage_table_index_type

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

TABLE_INDEX_TYPE_COLUMN_SCHEMA_PARAM_GRID = [
    ("heap", None),
    # For "clustered_columnstore_index" tables, different code paths exist
    # when no column schema is specified versus when a column schema is
    # specified, so we test both.
    ("clustered_columnstore_index", None),
    ("clustered_columnstore_index", TABLE_UPDATE),
]


@pytest.mark.parametrize(
    "table_index_type,column_schema", TABLE_INDEX_TYPE_COLUMN_SCHEMA_PARAM_GRID
)
def test_default_table_index_type_configuration(
    table_index_type: TTableIndexType,
    column_schema: Union[List[TColumnSchema], None],
) -> None:
    # Configure default_table_index_type.
    os.environ["DESTINATION__SYNAPSE__DEFAULT_TABLE_INDEX_TYPE"] = table_index_type

    @dlt.resource(
        name="items_without_table_index_type_specified",
        write_disposition="append",
        columns=column_schema,
    )
    def items_without_table_index_type_specified() -> Iterator[Any]:
        yield TABLE_ROW_ALL_DATA_TYPES

    pipeline = dlt.pipeline(
        pipeline_name=f"test_default_table_index_type_{table_index_type}",
        destination="synapse",
        dataset_name=f"test_default_table_index_type_{table_index_type}",
        dev_mode=True,
    )

    job_client = pipeline.destination_client()
    # Assert configuration value gets properly propagated to job client configuration.
    assert job_client.config.default_table_index_type == table_index_type  # type: ignore[attr-defined]

    # Run the pipeline and create the tables.
    pipeline.run(items_without_table_index_type_specified)

    # For all tables, assert the applied index type equals the expected index type.
    # Child tables, if any, inherit the index type of their parent.
    tables = pipeline.default_schema.tables
    for table_name in tables:
        applied_table_index_type = get_storage_table_index_type(job_client.sql_client, table_name)  # type: ignore[attr-defined]
        if table_name in pipeline.default_schema.data_table_names():
            # For data tables, the applied table index type should be the default value.
            assert applied_table_index_type == job_client.config.default_table_index_type  # type: ignore[attr-defined]
        elif table_name in pipeline.default_schema.dlt_table_names():
            # For dlt tables, the applied table index type should always be "heap".
            assert applied_table_index_type == "heap"

    # Test overriding the default_table_index_type from a resource configuration.
    if job_client.config.default_table_index_type == "heap":  # type: ignore[attr-defined]

        @dlt.resource(
            name="items_with_table_index_type_specified",
            write_disposition="append",
            columns=column_schema,
        )
        def items_with_table_index_type_specified() -> Iterator[Any]:
            yield TABLE_ROW_ALL_DATA_TYPES

        pipeline.run(
            synapse_adapter(items_with_table_index_type_specified, "clustered_columnstore_index")
        )
        applied_table_index_type = get_storage_table_index_type(
            job_client.sql_client,  # type: ignore[attr-defined]
            "items_with_table_index_type_specified",
        )
        # While the default is "heap", the applied index type should be "clustered_columnstore_index"
        # because it was provided as argument to the resource.
        assert applied_table_index_type == "clustered_columnstore_index"


@pytest.mark.parametrize(
    "table_index_type,column_schema", TABLE_INDEX_TYPE_COLUMN_SCHEMA_PARAM_GRID
)
def test_resource_table_index_type_configuration(
    table_index_type: TTableIndexType,
    column_schema: Union[List[TColumnSchema], None],
) -> None:
    os.environ["DESTINATION__REPLACE_STRATEGY"] = "insert-from-staging"

    @dlt.resource(
        name="items_with_table_index_type_specified",
        write_disposition="replace",
        columns=column_schema,
    )
    def items_with_table_index_type_specified() -> Iterator[Any]:
        yield TABLE_ROW_ALL_DATA_TYPES

    pipeline = dlt.pipeline(
        pipeline_name=f"test_table_index_type_{table_index_type}",
        destination="synapse",
        dataset_name=f"test_table_index_type_{table_index_type}",
        dev_mode=True,
    )

    # An invalid value for `table_index_type` should raise a ValueError.
    with pytest.raises(ValueError):
        pipeline.run(synapse_adapter(items_with_table_index_type_specified, "foo"))  # type: ignore[arg-type]

    # Run the pipeline and create the tables.
    pipeline.run(synapse_adapter(items_with_table_index_type_specified, table_index_type))

    # For all tables, assert the applied index type equals the expected index type.
    # Child tables, if any, inherit the index type of their parent.
    job_client = pipeline.destination_client()
    tables = pipeline.default_schema.tables
    for table_name in tables:
        applied_table_index_type = get_storage_table_index_type(job_client.sql_client, table_name)  # type: ignore[attr-defined]
        if table_name in pipeline.default_schema.data_table_names():
            # For data tables, the applied table index type should be the type
            # configured in the resource.
            assert applied_table_index_type == table_index_type
        elif table_name in pipeline.default_schema.dlt_table_names():
            # For dlt tables, the applied table index type should always be "heap".
            assert applied_table_index_type == "heap"
