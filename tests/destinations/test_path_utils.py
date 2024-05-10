from typing import List, Tuple

import pytest
import dlt.destinations.path_utils

from dlt.common import logger, pendulum
from dlt.common.storages import LoadStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName

from dlt.destinations.path_utils import create_path, get_table_prefix_layout

from dlt.destinations.exceptions import InvalidFilesystemLayout, CantExtractTablePrefix
from tests.common.storages.utils import start_loading_file, load_storage


TestLoad = Tuple[str, ParsedLoadJobFileName]


def dummy_callback(*args, **kwargs):
    assert len(args) == 5
    return "-".join(args)


def dummy_callback2(*args, **kwargs):
    assert len(args) == 5
    return "random-value"


EXTRA_PLACEHOLDERS = {
    "type": "one-for-all",
    "vm": "beam",
    "module": "__MODULE__",
    "bobo": "is-name",
    "callback": dummy_callback,
    "random_value": dummy_callback2,
}

frozen_datetime = pendulum.DateTime(
    year=2024,
    month=4,
    day=14,
    hour=8,
    minute=32,
    second=0,
    microsecond=0,
)

# Each layout example is a tuple of
# 1. layout to expand,
# 2. expected path,
# 3. valid or not flag,
# 4. the list of expected invalid or unknown placeholders.
ALL_LAYOUTS = (  # type: ignore
    # Usual placeholders
    (
        "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}",
        "schema-name/mocked-table/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{schema_name}.{table_name}.{load_id}.{file_id}.{ext}",
        "schema-name.mocked-table.mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}88{load_id}-u-{file_id}.{ext}",
        "mocked-table88mocked-load-id-u-mocked-file-id.jsonl",
        True,
        [],
    ),
    # Additional placeholders
    (
        "{table_name}/{curr_date}/{load_id}.{file_id}.{ext}{timestamp}",
        f"mocked-table/{str(frozen_datetime.date())}/mocked-load-id.mocked-file-id.jsonl{int(frozen_datetime.timestamp())}",
        True,
        [],
    ),
    (
        "{table_name}/{YYYY}-{MM}-{DD}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('YYYY-MM-DD')}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{YYYY}-{MMM}-{D}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('YYYY-MMM-D').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{DD}/{HH}/{m}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('DD/HH/m').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{D}/{HH}/{mm}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('D/HH/mm').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{timestamp}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{int(frozen_datetime.timestamp())}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/dayofweek-{ddd}/{load_id}.{file_id}.{ext}",
        f"mocked-table/dayofweek-{frozen_datetime.format('ddd').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{ddd}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('ddd').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{Q}/{MM}/{ddd}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('Q/MM/ddd').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{YY}-{MM}-{D}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('YY-MM-D').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{Y}-{M}-{D}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('Y-M-D').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{HH}/{mm}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('HH/mm').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{mm}/{ss}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('mm/ss').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{mm}/{s}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('mm/s').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{ss}/{s}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('ss/s').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{SSS}/{SS}/{S}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('SSS/SS/S').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{SS}/{S}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('SS/S').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/ms={SSS}/mcs={SSSS}/{load_id}.{file_id}.{ext}",
        f"mocked-table/ms={frozen_datetime.format('SSS').lower()}/mcs={frozen_datetime.format('SSSS').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{H}/{m}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('H/m').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{M}/{dddd}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('M/dddd').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{M}/{ddd}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('M/ddd').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{M}/{dd}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('M/dd').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{M}/{d}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{frozen_datetime.format('M/d').lower()}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{load_package_timestamp}/{d}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{int(frozen_datetime.timestamp())}/{frozen_datetime.format('d')}/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    # Extra placehodlers
    (
        "{table_name}/{timestamp}/{type}-{vm}-{module}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{int(frozen_datetime.timestamp())}/one-for-all-beam-__MODULE__/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{timestamp}/{type}-{bobo}-{module}/{load_id}.{file_id}.{ext}",
        f"mocked-table/{int(frozen_datetime.timestamp())}/one-for-all-is-name-__MODULE__/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{type}/{bobo}/{module}/{load_id}.{file_id}.{ext}",
        "mocked-table/one-for-all/is-name/__MODULE__/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{callback}/{callback}/{type}/{load_id}.{file_id}.{ext}",
        "mocked-table/schema-name-mocked-table-mocked-load-id-mocked-file-id-jsonl/schema-name-mocked-table-mocked-load-id-mocked-file-id-jsonl/one-for-all/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    (
        "{table_name}/{random_value}/{callback}/{load_id}.{file_id}.{ext}",
        "mocked-table/random-value/schema-name-mocked-table-mocked-load-id-mocked-file-id-jsonl/mocked-load-id.mocked-file-id.jsonl",
        True,
        [],
    ),
    ("{timestamp}{table_name}", f"{int(frozen_datetime.timestamp())}mocked-table", True, []),
    ("{ddd}/{MMM}/{table_name}", "sun/apr/mocked-table", True, []),
    (
        "{Y}/{timestamp}/{table_name}",
        f"{frozen_datetime.format('YYYY')}/{int(frozen_datetime.timestamp())}/mocked-table",
        True,
        [],
    ),
    (
        "{Y}/{timestamp_ms}/{table_name}",
        f"{frozen_datetime.year}/{str(int(frozen_datetime.timestamp()*1000))}/mocked-table",
        True,
        [],
    ),
    (
        "{Y}/{load_package_timestamp_ms}/{table_name}",
        f"{frozen_datetime.year}/{str(int(frozen_datetime.timestamp()*1000))}/mocked-table",
        True,
        [],
    ),
    ("{load_id}/{ext}/{table_name}", "mocked-load-id/jsonl/mocked-table", True, []),
    ("{HH}/{mm}/{schema_name}", f"{frozen_datetime.format('HH/mm')}/schema-name", True, []),
    ("{type}/{bobo}/{table_name}", "one-for-all/is-name/mocked-table", True, []),
    # invalid placeholders
    ("{illegal_placeholder}{table_name}", "", False, ["illegal_placeholder"]),
    ("{unknown_placeholder}/{volume}/{table_name}", "", False, ["unknown_placeholder", "volume"]),
    (
        "{table_name}/{abc}/{load_id}.{ext}{timestamp}-{random}",
        "",
        False,
        ["abc", "random"],
    ),
)


@pytest.fixture
def test_load(load_storage: LoadStorage) -> TestLoad:
    load_id, filename = start_loading_file(load_storage, "test file")  # type: ignore[arg-type]
    info = ParsedLoadJobFileName.parse(filename)
    return load_id, info


@pytest.mark.parametrize("layout,expected_path,is_valid,invalid_placeholders", ALL_LAYOUTS)
def test_layout_validity(
    layout: str,
    expected_path: str,
    is_valid: bool,
    invalid_placeholders: List[str],
    mocker,
) -> None:
    job_info = ParsedLoadJobFileName(
        table_name="mocked-table",
        file_id="mocked-file-id",
        retry_count=0,
        file_format="jsonl",
    )
    load_id = "mocked-load-id"
    mocker.patch("pendulum.now", return_value=frozen_datetime)
    mocker.patch(
        "pendulum.DateTime.timestamp",
        return_value=frozen_datetime.timestamp(),
    )

    mocker.patch(
        "dlt.common.storages.load_package.ParsedLoadJobFileName.parse",
        return_value=job_info,
    )

    now_timestamp = frozen_datetime
    if is_valid:
        path = create_path(
            layout,
            schema_name="schema-name",
            load_id=load_id,
            load_package_timestamp=now_timestamp,
            file_name=job_info.file_name(),
            extra_placeholders=EXTRA_PLACEHOLDERS,
        )
        assert path == expected_path
        assert len(path.split("/")) == len(layout.split("/"))
    else:
        with pytest.raises(InvalidFilesystemLayout) as exc:
            create_path(
                layout,
                schema_name="schema-name",
                load_id=load_id,
                load_package_timestamp=now_timestamp,
                file_name=job_info.file_name(),
                extra_placeholders=EXTRA_PLACEHOLDERS,
            )
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
        load_package_timestamp=now,
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
    timestamp = int(now.timestamp())
    now_timestamp = now
    calls = 0

    def current_datetime_callback():
        nonlocal calls
        calls += 1
        return now

    created_path_1 = create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        current_datetime=current_datetime_callback,
        load_package_timestamp=now_timestamp,
        file_name=job_info.file_name(),
    )

    created_path_2 = create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        current_datetime=now,
        load_package_timestamp=now_timestamp,
        file_name=job_info.file_name(),
    )

    assert created_path_1 == created_path_2
    assert (
        created_path_1 == f"schema_name/mock_table/{load_id}.{job_info.file_id}.{timestamp}.jsonl"
    )
    assert (
        created_path_2 == f"schema_name/mock_table/{load_id}.{job_info.file_id}.{timestamp}.jsonl"
    )

    # expect only one call
    assert calls == 1

    # If the value for current_datetime is not pendulum.DateTime
    # it should fail with RuntimeError exception
    with pytest.raises(AttributeError):
        create_path(
            "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
            schema_name="schema_name",
            load_id=load_id,
            current_datetime="now",  # type: ignore
            load_package_timestamp=now_timestamp,
            file_name=job_info.file_name(),
        )
    with pytest.raises(RuntimeError):
        create_path(
            "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
            schema_name="schema_name",
            load_id=load_id,
            current_datetime=lambda: 1234,  # type: ignore
            load_package_timestamp=now_timestamp,
            file_name=job_info.file_name(),
        )


def test_create_path_uses_current_moment_if_current_datetime_is_not_given(
    test_load: TestLoad, mocker
) -> None:
    load_id, job_info = test_load
    logger_spy = mocker.spy(logger, "info")
    pendulum_spy = mocker.spy(pendulum, "now")
    path = create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        file_name=job_info.file_name(),
    )
    timestamp = int(pendulum_spy.spy_return.timestamp())
    assert path == f"schema_name/mock_table/{load_id}.{job_info.file_id}.{timestamp}.jsonl"
    logger_spy.assert_called_once_with("current_datetime is not set, using pendulum.now()")
    pendulum_spy.assert_called()


def test_create_path_uses_load_package_timestamp_as_current_datetime(
    test_load: TestLoad, mocker
) -> None:
    load_id, job_info = test_load
    now = pendulum.now()
    timestamp = int(now.timestamp())
    now_timestamp = now
    logger_spy = mocker.spy(logger, "info")
    ensure_pendulum_datetime_spy = mocker.spy(
        dlt.destinations.path_utils, "ensure_pendulum_datetime"
    )
    path = create_path(
        "{schema_name}/{table_name}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        load_package_timestamp=now_timestamp,
        file_name=job_info.file_name(),
    )
    assert path == f"schema_name/mock_table/{load_id}.{job_info.file_id}.{timestamp}.jsonl"
    logger_spy.assert_called_once_with(
        "current_datetime is not set, using timestamp from load package"
    )
    ensure_pendulum_datetime_spy.assert_called_once_with(now_timestamp)


def test_create_path_resolves_extra_placeholders(test_load: TestLoad) -> None:
    load_id, job_info = test_load

    class Counter:
        def __init__(self) -> None:
            self.count = 0

        def inc(self, *args, **kwargs):
            schema_name, table_name, passed_load_id, file_id, ext = args
            assert len(args) == 5
            assert schema_name == "schema_name"
            assert table_name == "mock_table"
            assert passed_load_id == load_id
            assert file_id == job_info.file_id
            assert ext == job_info.file_format
            self.count += 1
            return "boo"

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
    now = pendulum.now()
    timestamp = int(now.timestamp())
    now_timestamp = now
    created_path = create_path(
        "{schema_name}/{table_name}/{callable_1}-{otter}/{load_id}.{file_id}.{timestamp}.{ext}",
        schema_name="schema_name",
        load_id=load_id,
        extra_placeholders=extra_placeholders,
        file_name=job_info.file_name(),
        load_package_timestamp=now_timestamp,
    )
    assert (
        created_path
        == f"schema_name/mock_table/boo-boo/{load_id}.{job_info.file_id}.{timestamp}.jsonl"
    )
    assert counter.count == 2
