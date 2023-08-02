import pytest

from dlt.destinations import path_utils
from dlt.destinations.exceptions import InvalidFilesystemLayout, CantExtractTablePrefix


def test_layout_validity() -> None:
    path_utils.check_layout("{table_name}")
    path_utils.check_layout("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}")
    with pytest.raises(InvalidFilesystemLayout) as exc:
        path_utils.check_layout("{other_ph}.{table_name}")
        assert exc.invalid_placeholders == {"other_ph"}

def test_create_path() -> None:
    path_vars = {
        "schema_name": "schema_name",
        "table_name": "table_name",
        "load_id": "load_id",
        "file_id": "file_id",
        "ext": "ext"
    }
    path = path_utils.create_path("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}", **path_vars)
    assert path == "schema_name/table_name/load_id.file_id.ext"

    # extension gets added automatically
    path = path_utils.create_path("{schema_name}/{table_name}/{load_id}", **path_vars)
    assert path == "schema_name/table_name/load_id.ext"

def test_get_table_prefix() -> None:

    pf = path_utils.get_table_prefix("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}", schema_name="my_schema", table_name="my_table")
    assert pf == "my_schema/my_table/"

    pf = path_utils.get_table_prefix("some_random{schema_name}/stuff_in_between/{table_name}/{load_id}", schema_name="my_schema", table_name="my_table")
    assert pf == "some_randommy_schema/stuff_in_between/my_table/"

    # disallow missing table_name
    with pytest.raises(CantExtractTablePrefix):
        pf = path_utils.get_table_prefix("some_random{schema_name}/stuff_in_between/", schema_name="my_schema", table_name="my_table")

    # disallow other params before table_name
    with pytest.raises(CantExtractTablePrefix):
        pf = path_utils.get_table_prefix("{file_id}some_random{table_name}/stuff_in_between/", schema_name="my_schema", table_name="my_table")

    # disallow table_name without following separator
    with pytest.raises(CantExtractTablePrefix):
        pf = path_utils.get_table_prefix("{schema_name}/{table_name}{load_id}.{file_id}.{ext}", schema_name="my_schema", table_name="my_table")
