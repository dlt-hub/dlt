import contextlib
import io
from typing import List, Tuple
from unittest.mock import patch

import pendulum
import pytest
from dlt.common.storages import LoadStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.path_utils import (
    check_layout,
    create_path,
    get_table_prefix_layout,
)

from dlt.destinations.exceptions import InvalidFilesystemLayout, CantExtractTablePrefix
from tests.common.storages.utils import start_loading_file, load_storage


TestLoad = Tuple[str, ParsedLoadJobFileName]
EXTRA_PLACEHOLDERS = {"type": "one-for-all", "vm": "beam", "module": "__MODULE__"}
ALL_LAYOUTS = (  # type: ignore
    # Usual layouts
    ("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}", True, []),
    ("{schema_name}.{table_name}.{load_id}.{file_id}.{ext}", True, []),
    ("{table_name}88{load_id}-u-{file_id}.{ext}", True, []),
    # Extra layouts
    ("{table_name}/{curr_date}/{load_id}.{file_id}.{ext}{timestamp}", True, []),
    ("{table_name}/{YYYY}-{MM}-{DD}/{load_id}.{file_id}.{ext}", True, []),
    ("{table_name}/{YYYY}-{MMM}-{D}/{load_id}.{file_id}.{ext}", True, []),
    ("{table_name}/{DD}/{HH}/{m}/{load_id}.{file_id}.{ext}", True, []),
    ("{table_name}/{D}/{HH}/{mm}/{load_id}.{file_id}.{ext}", True, []),
    ("{table_name}/{timestamp}/{load_id}.{file_id}.{ext}", True, []),
    (
        "{table_name}/{timestamp}/{type}-{vm}-{module}/{load_id}.{file_id}.{ext}",
        True,
        [],
    ),
    ("{table_name}/dayofweek-{ddd}/{load_id}.{file_id}.{ext}", True, []),
    ("{table_name}/{ddd}/{load_id}.{file_id}.{ext}", True, []),
    # invalid layouts
    ("{illegal_placeholder}{table_name}", False, ["illegal_placeholder"]),
    (
        "{table_name}/{abc}/{load_id}.{ext}{timestamp}-{random}",
        False,
        ["abc", "random"],
    ),
)


@pytest.fixture
def test_load(load_storage: LoadStorage) -> TestLoad:
    load_id, filename = start_loading_file(load_storage, "test file")  # type: ignore[arg-type]
    info = ParsedLoadJobFileName.parse(filename)
    return load_id, info


@pytest.mark.parametrize("layout,is_valid,invalid_placeholders", ALL_LAYOUTS)
def test_layout_validity(layout: str, is_valid: bool, invalid_placeholders: List[str]) -> None:
    if is_valid:
        check_layout(layout, EXTRA_PLACEHOLDERS)
    else:
        with pytest.raises(InvalidFilesystemLayout) as exc:
            check_layout("{other_ph}.{table_name}", EXTRA_PLACEHOLDERS)
            assert set(exc.value.invalid_placeholders) == set(invalid_placeholders)


def test_create_path(test_load: TestLoad) -> None:
    load_id, job_info = test_load
    path = create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        file_name=job_info.file_name(),
    )
    assert path == f"schema_name/mock_table/{load_id}.{job_info.file_id}.{job_info.file_format}"

    # extension gets added automatically
    path = create_path(
        "{schema_name}/{table_name}/{load_id}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        file_name=job_info.file_name(),
    )
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


def test_create_path_uses_provided_load_package_timestamp(test_load: TestLoad) -> None:
    load_id, job_info = test_load
    now = pendulum.now()
    timestamp = str(int(now.timestamp()))
    path = create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        load_package_timestamp=now.to_iso8601_string(),
        file_name=job_info.file_name(),
    )

    assert timestamp in path
    assert path.endswith(f"{timestamp}.{job_info.file_format}")


def test_create_path_resolves_current_datetime(test_load: TestLoad) -> None:
    """Check the flow when the current_datetime is passed

    Happy path checks

    1. Callback,
    2. pendulum.DateTime

    Failures when neither callback nor the value is not of pendulum.DateTime
    """
    load_id, job_info = test_load
    now = pendulum.now()
    calls = 0

    def current_datetime_callback():
        nonlocal calls
        calls += 1
        return now

    create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        current_datetime=current_datetime_callback,
        file_name=job_info.file_name(),
    )

    create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        current_datetime=now,
        file_name=job_info.file_name(),
    )

    # expect only one call
    assert calls == 1

    # If the value for current_datetime is not pendulum.DateTime
    # it should fail with RuntimeError exception
    with pytest.raises(RuntimeError):
        create_path(
            "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
            schema_name="schema_name",
            load_id=load_id,
            current_datetime="now",  # type: ignore
            file_name=job_info.file_name(),
        )
    with pytest.raises(RuntimeError):
        create_path(
            "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
            schema_name="schema_name",
            load_id=load_id,
            current_datetime=lambda: 1234,  # type: ignore
            file_name=job_info.file_name(),
        )


def test_create_path_uses_current_moment_if_current_datetime_is_not_given(
    test_load: TestLoad,
) -> None:
    load_id, job_info = test_load
    now = pendulum.now()
    with patch("pendulum.now", wraps=pendulum.DateTime, return_value=now) as mock:
        create_path(
            "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
            schema_name="schema_name",
            load_id=load_id,
            file_name=job_info.file_name(),
        )

        assert len(mock.mock_calls) == 1


def test_create_path_uses_current_moment_if_load_package_timestamp_is_not_given(
    test_load: TestLoad,
) -> None:
    load_id, job_info = test_load
    now = pendulum.now()
    timestamp = str(int(now.timestamp()))
    with patch("pendulum.now", wraps=pendulum.DateTime, return_value=now) as mock:
        path = create_path(
            "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
            schema_name="schema_name",
            load_id=load_id,
            file_name=job_info.file_name(),
        )

        assert len(mock.mock_calls) == 1
        assert timestamp in path
        assert path.endswith(f"{timestamp}.{job_info.file_format}")


def test_create_path_resolves_extra_placeholders(test_load: TestLoad) -> None:
    load_id, job_info = test_load

    class Counter:
        def __init__(self) -> None:
            self.count = 0

        def inc(self, *args, **kwargs):
            self.count += 1

    counter = Counter()
    extra_placeholders = {
        "value": 1,
        "callable_1": counter.inc,
        "otter": counter.inc,
        "otters": "lab",
        "dlt": "labs",
        "dlthub": "platform",
        "x": "files",
    }

    create_path(
        "{schema_name}/{table_name}/{callable_1}-{otter}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        extra_placeholders=extra_placeholders,
        file_name=job_info.file_name(),
    )

    assert counter.count == 2


def test_create_path_reports_unused_placeholders(test_load: TestLoad) -> None:
    load_id, job_info = test_load
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        extra_placeholders = {
            "value": 1,
            "otters": "lab",
            "dlt": "labs",
            "dlthub": "platform",
            "x": "files",
        }

        create_path(
            "{schema_name}/{table_name}/{otters}-x-{x}/{load_id}.{file_id}.{timestamp}.{ext}",
            schema_name="schema_name",
            load_id=load_id,
            extra_placeholders=extra_placeholders,
            file_name=job_info.file_name(),
        )

        output = buf.getvalue()
        assert "Found unused layout placeholders: value, dlt, dlthub" in output
