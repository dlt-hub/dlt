import os
import pytest
from typing import Iterator, List, Any, Union
from textwrap import dedent

import dlt
from dlt.common.schema import TColumnSchema
from dlt.common.schema.typing import TTableIndexType, TSchemaTables
from dlt.common.schema.utils import get_table_index_type

from dlt.destinations.sql_client import SqlClientBase

from tests.load.utils import TABLE_UPDATE, TABLE_ROW_ALL_DATA_TYPES
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)


TABLE_INDEX_TYPE_COLUMN_SCHEMA_PARAM_GRID = [
    ("heap", None),
    # For "clustered_columnstore_index" tables, different code paths exist
    # when no column schema is specified versus when a column schema is
    # specified, so we test both.
    ("clustered_columnstore_index", None),
    ("clustered_columnstore_index", TABLE_UPDATE),
]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["synapse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "table_index_type,column_schema", TABLE_INDEX_TYPE_COLUMN_SCHEMA_PARAM_GRID
)
def test_default_table_index_type_configuration(
    destination_config: DestinationTestConfiguration,
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

    pipeline = destination_config.setup_pipeline(
        f"test_default_table_index_type_{table_index_type}",
        full_refresh=True,
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
        applied_table_index_type = job_client.get_storage_table_index_type(table_name)  # type: ignore[attr-defined]
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
            table_index_type="clustered_columnstore_index",
            columns=column_schema,
        )
        def items_with_table_index_type_specified() -> Iterator[Any]:
            yield TABLE_ROW_ALL_DATA_TYPES

        pipeline.run(items_with_table_index_type_specified)
        applied_table_index_type = job_client.get_storage_table_index_type(  # type: ignore[attr-defined]
            "items_with_table_index_type_specified"
        )
        # While the default is "heap", the applied index type should be "clustered_columnstore_index"
        # because it was provided as argument to the resource.
        assert applied_table_index_type == "clustered_columnstore_index"


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["synapse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "table_index_type,column_schema", TABLE_INDEX_TYPE_COLUMN_SCHEMA_PARAM_GRID
)
def test_resource_table_index_type_configuration(
    destination_config: DestinationTestConfiguration,
    table_index_type: TTableIndexType,
    column_schema: Union[List[TColumnSchema], None],
) -> None:
    @dlt.resource(
        name="items_with_table_index_type_specified",
        write_disposition="append",
        table_index_type=table_index_type,
        columns=column_schema,
    )
    def items_with_table_index_type_specified() -> Iterator[Any]:
        yield TABLE_ROW_ALL_DATA_TYPES

    pipeline = destination_config.setup_pipeline(
        f"test_table_index_type_{table_index_type}",
        full_refresh=True,
    )

    # Run the pipeline and create the tables.
    pipeline.run(items_with_table_index_type_specified)

    # For all tables, assert the applied index type equals the expected index type.
    # Child tables, if any, inherit the index type of their parent.
    job_client = pipeline.destination_client()
    tables = pipeline.default_schema.tables
    for table_name in tables:
        applied_table_index_type = job_client.get_storage_table_index_type(table_name)  # type: ignore[attr-defined]
        if table_name in pipeline.default_schema.data_table_names():
            # For data tables, the applied table index type should be the type
            # configured in the resource.
            assert applied_table_index_type == table_index_type
        elif table_name in pipeline.default_schema.dlt_table_names():
            # For dlt tables, the applied table index type should always be "heap".
            assert applied_table_index_type == "heap"
