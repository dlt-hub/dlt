from typing import Tuple

import pytest

from dlt.common.storages import LoadStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.path_utils import (
    create_path,
    get_table_prefix_layout,
    ExtraParams,
    LayoutHelper,
)
from dlt.destinations.exceptions import InvalidFilesystemLayout, CantExtractTablePrefix
from tests.common.storages.utils import start_loading_file, load_storage


TestLoad = Tuple[str, ParsedLoadJobFileName]


@pytest.fixture
def test_load(load_storage: LoadStorage) -> TestLoad:
    load_id, filename = start_loading_file(load_storage, "test file")
    info = ParsedLoadJobFileName.parse(filename)
    return load_id, info


def test_layout_validity() -> None:
    extra_params = ExtraParams()
    LayoutHelper("{table_name}", extra_params.params).check_layout()
    LayoutHelper(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}", extra_params.params
    ).check_layout()
    with pytest.raises(InvalidFilesystemLayout) as exc:
        LayoutHelper("{other_ph}.{table_name}", extra_params.params).check_layout()
    assert exc.value.invalid_placeholders == ["other_ph"]


def test_create_path(test_load: TestLoad) -> None:
    load_id, job_info = test_load
    path_vars = {
        "schema_name": "schema_name",
        "load_id": load_id,
        "file_name": job_info.file_name(),
    }
    path = create_path("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}", **path_vars)
    assert path == f"schema_name/mock_table/{load_id}.{job_info.file_id}.{job_info.file_format}"

    # extension gets added automatically
    path = create_path("{schema_name}/{table_name}/{load_id}.{ext}", **path_vars)
    assert path == f"schema_name/mock_table/{load_id}.{job_info.file_format}"


def test_get_table_prefix_layout() -> None:
    prefix_layout = get_table_prefix_layout("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}")
    assert prefix_layout == "{schema_name}/{table_name}/"
    assert (
        prefix_layout.format(schema_name="my_schema", table_name="my_table")
        == "my_schema/my_table/"
    )

    prefix_layout = get_table_prefix_layout(
        "some_random{schema_name}/stuff_in_between/{table_name}/{load_id}"
    )
    assert prefix_layout == "some_random{schema_name}/stuff_in_between/{table_name}/"
    assert (
        prefix_layout.format(schema_name="my_schema", table_name="my_table")
        == "some_randommy_schema/stuff_in_between/my_table/"
    )

    # disallow missing table_name
    with pytest.raises(CantExtractTablePrefix):
        get_table_prefix_layout("some_random{schema_name}/stuff_in_between/")

    # disallow other params before table_name
    with pytest.raises(CantExtractTablePrefix):
        get_table_prefix_layout("{file_id}some_random{table_name}/stuff_in_between/")

    # disallow any placeholders before table name (ie. Athena)
    with pytest.raises(CantExtractTablePrefix):
        get_table_prefix_layout(
            "{schema_name}some_random{table_name}/stuff_in_between/",
            supported_prefix_placeholders=[],
        )

    # disallow table_name without following separator
    with pytest.raises(CantExtractTablePrefix):
        get_table_prefix_layout("{schema_name}/{table_name}{load_id}.{file_id}.{ext}")
