import os
import pytest
import shutil
from unittest.mock import MagicMock, patch

import dlt
import dlt.common.libs.pyiceberg as pyiceberg_module
from dlt.destinations.impl.filesystem.iceberg_adapter import TABLE_PROPERTIES_HINT
from pyiceberg.exceptions import NoSuchTableError


def test_iceberg_namespace_properties_mock() -> None:
    storage_path = os.path.abspath("_storage_test_iceberg_ns_props")
    if os.path.exists(storage_path):
        shutil.rmtree(storage_path)

    # Set up namespace properties in config using iceberg_catalog section
    os.environ["ICEBERG_CATALOG__NAMESPACE_PROPERTIES"] = '{"prop1": "val1"}'
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = f"file://{storage_path}"

    pipeline = dlt.pipeline(
        pipeline_name="test_iceberg_ns_props",
        destination="filesystem",
        dataset_name="test_iceberg_ds_ns_props",
    )

    @dlt.resource
    def my_table():
        yield {"id": 1, "name": "test"}

    # Mock external dependencies - use sys.modules to patch before the import
    import dlt.common.libs.pyiceberg as pyiceberg_module

    with (patch.object(pyiceberg_module, "get_catalog") as mock_get_catalog,):
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
    my_table.apply_hints(additional_table_hints={TABLE_PROPERTIES_HINT: {"prop1": "val1"}})

    with (
        patch.object(pyiceberg_module, "get_catalog") as mock_get_catalog,
        patch.object(pyiceberg_module, "create_table") as mock_create_table,
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
            if "test_iceberg_ds_tbl_props.my_table" in args[1] and kwargs.get("properties") == {
                "prop1": "val1"
            }:
                found_table_call = True

        assert (
            found_table_call
        ), f"create_table was not called with expected properties. Calls: {calls}"


def test_iceberg_adapter_table_properties() -> None:
    """Test iceberg_adapter with table_properties parameter."""
    from dlt.destinations.adapters import iceberg_adapter

    data = [{"id": 1, "category": "A"}]

    resource = iceberg_adapter(
        data,
        partition="category",
        table_properties={"format-version": "2", "write.delete.mode": "delete-file"},
    )

    table_schema = resource.compute_table_schema()
    table_hints = table_schema.get(TABLE_PROPERTIES_HINT, {})

    assert table_hints == {"format-version": "2", "write.delete.mode": "delete-file"}


def test_iceberg_adapter_table_properties_validation() -> None:
    """Test that iceberg_adapter validates table_properties."""
    from dlt.destinations.adapters import iceberg_adapter

    data = [{"id": 1}]

    with pytest.raises(ValueError, match=r"`table_properties` must be a dictionary"):
        iceberg_adapter(data, partition="id", table_properties="not a dict")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Table property keys must be strings"):
        iceberg_adapter(data, partition="id", table_properties={123: "value"})  # type: ignore[dict-item]
