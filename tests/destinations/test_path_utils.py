import pytest

from dlt.destinations import path_utils
from dlt.destinations.exceptions import InvalidFilesystemLayout, CantExtractTablePrefix


def test_layout_validity() -> None:
    path_utils.check_layout("{table_name}")
    path_utils.check_layout("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}")
    with pytest.raises(InvalidFilesystemLayout) as exc:
        path_utils.check_layout("{other_ph}.{table_name}")
    assert exc.value.invalid_placeholders == ["other_ph"]


def test_create_path() -> None:
    path_vars = {
        "schema_name": "schema_name",
        "table_name": "table_name",
        "load_id": "load_id",
        "file_id": "file_id",
        "ext": "ext",
    }
    path = path_utils.create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}", **path_vars
    )
    assert path == "schema_name/table_name/load_id.file_id.ext"

    # extension gets added automatically
    path = path_utils.create_path("{schema_name}/{table_name}/{load_id}", **path_vars)
    assert path == "schema_name/table_name/load_id.ext"


def test_get_table_prefix_layout() -> None:
    prefix_layout = path_utils.get_table_prefix_layout(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}"
    )
    assert prefix_layout == "{schema_name}/{table_name}/"
    assert (
        prefix_layout.format(schema_name="my_schema", table_name="my_table")
        == "my_schema/my_table/"
    )

    prefix_layout = path_utils.get_table_prefix_layout(
        "some_random{schema_name}/stuff_in_between/{table_name}/{load_id}"
    )
    assert prefix_layout == "some_random{schema_name}/stuff_in_between/{table_name}/"
    assert (
        prefix_layout.format(schema_name="my_schema", table_name="my_table")
        == "some_randommy_schema/stuff_in_between/my_table/"
    )

    # disallow missing table_name
    with pytest.raises(CantExtractTablePrefix):
        path_utils.get_table_prefix_layout("some_random{schema_name}/stuff_in_between/")

    # disallow other params before table_name
    with pytest.raises(CantExtractTablePrefix):
        path_utils.get_table_prefix_layout(
            "{file_id}some_random{table_name}/stuff_in_between/"
        )

    # disallow any placeholders before table name (ie. Athena)
    with pytest.raises(CantExtractTablePrefix):
        path_utils.get_table_prefix_layout(
            "{schema_name}some_random{table_name}/stuff_in_between/",
            supported_prefix_placeholders=[],
        )

    # disallow table_name without following separator
    with pytest.raises(CantExtractTablePrefix):
        path_utils.get_table_prefix_layout(
            "{schema_name}/{table_name}{load_id}.{file_id}.{ext}"
        )
