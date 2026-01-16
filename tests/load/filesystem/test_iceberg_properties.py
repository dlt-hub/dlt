import os
import shutil
from typing import cast
import pytest
from unittest.mock import MagicMock, patch

import dlt
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


def test_iceberg_namespace_properties_mock() -> None:
    storage_path = os.path.abspath("_storage_test_iceberg_ns_props")
    if os.path.exists(storage_path):
        shutil.rmtree(storage_path)

    # Set up namespace properties in config
    os.environ["DESTINATION__FILESYSTEM__ICEBERG_NAMESPACE_PROPERTIES"] = '{"prop1": "val1"}'
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = f"file://{storage_path}"

    pipeline = dlt.pipeline(
        pipeline_name="test_iceberg_ns_props",
        destination="filesystem",
        dataset_name="test_iceberg_ds_ns_props",
    )

    @dlt.resource
    def my_table():
        yield {"id": 1, "name": "test"}

    # Mock external dependencies
    with (
        patch("dlt.common.libs.pyiceberg.get_catalog") as mock_get_catalog,
        patch("dlt.common.libs.pyiceberg.create_table") as mock_create_table,
    ):
        mock_catalog = MagicMock()
        mock_get_catalog.return_value = mock_catalog

        # We don't care about table creation here, but we need pipeline to run
        # create_table will be mocked so no actual IO

        try:
            pipeline.run(my_table, table_format="iceberg")
        except Exception:
            pass

        # Verify namespace creation properties
        mock_catalog.create_namespace.assert_called_with(
            "test_iceberg_ds_ns_props", {"prop1": "val1"}
        )


def test_iceberg_table_properties_mock() -> None:
    storage_path = os.path.abspath("_storage_test_iceberg_tbl_props")
    if os.path.exists(storage_path):
        shutil.rmtree(storage_path)

    # Set up bucket url
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = f"file://{storage_path}"

    pipeline = dlt.pipeline(
        pipeline_name="test_iceberg_tbl_props",
        destination="filesystem",
        dataset_name="test_iceberg_ds_tbl_props",
    )

    @dlt.resource
    def my_table():
        yield {"id": 1, "name": "test"}

    # Apply hints to specify table properties
    my_table.apply_hints(
        additional_table_hints={"x-iceberg-table-properties": {"table.prop1": "table_val1"}}
    )

    try:
        from pyiceberg.exceptions import NoSuchTableError
    except ImportError:

        class NoSuchTableError(Exception):
            pass

    with (
        patch("dlt.common.libs.pyiceberg.get_catalog") as mock_get_catalog,
        patch("dlt.common.libs.pyiceberg.create_table") as mock_create_table,
        patch(
            "dlt.destinations.impl.filesystem.iceberg_partition_spec.build_iceberg_partition_spec"
        ) as mock_build_spec,
    ):
        mock_catalog = MagicMock()
        mock_get_catalog.return_value = mock_catalog

        # Make load_table raise NoSuchTableError so it triggers create_table path
        mock_catalog.load_table.side_effect = NoSuchTableError("Table does not exist")

        mock_build_spec.return_value = (MagicMock(), MagicMock())
        try:
            pipeline.run(my_table, table_format="iceberg")
        except Exception:
            pass

        # Verify table creation properties
        assert mock_create_table.called

        calls = mock_create_table.call_args_list
        found_table_call = False
        for args, kwargs in calls:
            # Check table_id (2nd arg)
            if "test_iceberg_ds_tbl_props.my_table" in args[1]:
                # Check properties
                if kwargs.get("properties") == {"table.prop1": "table_val1"}:
                    found_table_call = True

        assert (
            found_table_call
        ), f"create_table was not called with expected properties. Calls: {calls}"
