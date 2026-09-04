"""Loads every input awareness x `timezone` hint pair under a UTC and a `Europe/Berlin` context timezone.

A naive value is read in the context timezone, so its instant moves. An aware value keeps its
instant, so its wall clock moves. A destination without tz support stores the context wall clock.
UTC is the default and must keep today's behavior.
"""
from datetime import datetime, timezone, tzinfo  # noqa: I251
from typing import Any, Dict, List
from zoneinfo import ZoneInfo
import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import UnsupportedDataType
from dlt.common.configuration.specs.timezone_context import TimezoneContext
from dlt.common.destination import TLoaderFileFormat
from dlt.common.time import ensure_datetime
from dlt.common.utils import uniq_id
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info, load_tables_to_dicts

pytestmark = pytest.mark.essential

COLUMNS: Any = {
    "id": {"data_type": "bigint"},
    # the same instant is stored twice, once with an offset and once without
    "ts_aware": {"data_type": "timestamp", "timezone": True},
    "ts_naive": {"data_type": "timestamp", "timezone": False},
}

# january, so Berlin is +01:00 and a DST-free offset keeps the expectations readable
NAIVE_IN = datetime(2024, 1, 15, 23, 30)
AWARE_IN = datetime(2024, 1, 15, 23, 30, tzinfo=timezone.utc)


def _pipeline(destination_config: DestinationTestConfiguration) -> dlt.Pipeline:
    return destination_config.setup_pipeline("tz_ctx_" + uniq_id(), dev_mode=True)


@dlt.resource(name="events", columns=COLUMNS)
def _events() -> Any:
    yield [
        {"id": 1, "ts_aware": NAIVE_IN, "ts_naive": NAIVE_IN},
        {"id": 2, "ts_aware": AWARE_IN, "ts_naive": AWARE_IN},
    ]


def _arrow_events(pyarrow: Any) -> List[Any]:
    """The same two rows as `_events`, as arrow tables."""
    naive, aware = pyarrow.timestamp("us"), pyarrow.timestamp("us", tz="UTC")
    return [
        pyarrow.table(
            {
                "id": pyarrow.array([1], type=pyarrow.int64()),
                "ts_aware": pyarrow.array([NAIVE_IN], type=naive),
                "ts_naive": pyarrow.array([NAIVE_IN], type=naive),
            }
        ),
        pyarrow.table(
            {
                "id": pyarrow.array([2], type=pyarrow.int64()),
                "ts_aware": pyarrow.array([AWARE_IN], type=aware),
                "ts_naive": pyarrow.array([AWARE_IN], type=aware),
            }
        ),
    ]


def _rows_by_id(pipeline: dlt.Pipeline) -> Dict[int, Dict[str, Any]]:
    tables = load_tables_to_dicts(pipeline, "events", exclude_system_cols=True, sortkey="id")
    return {row["id"]: _parse_timestamps(row) for row in tables["events"]}


def _parse_timestamps(row: Dict[str, Any]) -> Dict[str, Any]:
    """Coerces timestamp columns that a destination returns as text, as sqlite does.

    `ensure_datetime` keeps awareness as written, so a naive string stays naive.
    """
    return {
        key: ensure_datetime(value) if key.startswith("ts_") and isinstance(value, str) else value
        for key, value in row.items()
    }


def _run(pipeline: dlt.Pipeline, data: Any, **run_kwargs: Any) -> None:
    try:
        info = pipeline.run(data, **run_kwargs)
    except PipelineStepFailed as ex:
        # a type this destination cannot load from this file format is not a timezone finding
        if isinstance(ex.__cause__, UnsupportedDataType):
            pytest.skip(str(ex.__cause__))
        raise
    assert_load_info(info)


def _assert_instant(
    value: Any,
    expected: datetime,
    caps: DestinationCapabilitiesContext,
    context_tz: tzinfo,
    case: str,
) -> None:
    """An aware column keeps `expected`'s instant, whatever offset the destination renders.

    A destination that cannot store an offset holds the context wall clock, naive.
    """
    assert value is not None, case
    if value.tzinfo is not None:
        assert (
            caps.supports_tz_aware_datetime
        ), f"{case}: destination does not support tz-aware datetime, got {value!r}"
        assert value == expected, case
    else:
        assert (
            not caps.supports_tz_aware_datetime
        ), f"{case}: destination supports tz-aware datetime, got naive {value!r}"
        assert value == expected.astimezone(context_tz).replace(tzinfo=None), case


def _assert_wall_clock(
    value: Any,
    expected: datetime,
    caps: DestinationCapabilitiesContext,
    context_tz: tzinfo,
    case: str,
) -> None:
    """A naive column keeps `expected`'s wall clock, read in the context timezone.

    A destination that cannot store a naive value holds the instant instead.
    """
    assert value is not None, case
    if value.tzinfo is None:
        assert (
            caps.supports_naive_datetime
        ), f"{case}: destination does not support naive datetime, got {value!r}"
        assert value == expected, case
    else:
        assert (
            not caps.supports_naive_datetime
        ), f"{case}: destination supports naive datetime, got tz-aware {value!r}"
        assert value.astimezone(context_tz).replace(tzinfo=None) == expected, case


@pytest.mark.parametrize("items_format", ["dict", "arrow"])
@pytest.mark.parametrize("loader_file_format", ["jsonl", "parquet"])
@pytest.mark.parametrize(
    "context_tz", ["UTC", "Europe/Berlin"], ids=["utc-context", "berlin-context"]
)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_context_timezone_matrix(
    destination_config: DestinationTestConfiguration,
    context_tz: str,
    loader_file_format: TLoaderFileFormat,
    items_format: str,
) -> None:
    """All four pairs, loaded and read back under the context timezone.

    Both item formats and both file formats must agree. The UTC context pins today's behavior: an
    aware UTC value written to a destination without tz support reads back naive, as written.
    """
    configured_format = destination_config.file_format
    if destination_config.staging and configured_format and configured_format != loader_file_format:
        # a staged config pins the format its staging path can actually load
        pytest.skip(f"`{destination_config.name}` is pinned to `{configured_format}`")
    caps = destination_config.raw_capabilities()
    supported = list(caps.supported_loader_file_formats or [])
    if destination_config.staging or not supported:
        # athena and dremio load only through a bucket
        supported += caps.supported_staging_file_formats or []
    if loader_file_format not in supported:
        # the text path of other destinations is the typed variant or insert values
        text_format = next((f for f in ("typed-jsonl", "insert_values") if f in supported), None)
        if loader_file_format == "jsonl" and text_format:
            loader_file_format = text_format  # type: ignore[assignment]
        else:
            pytest.skip(
                f"`{destination_config.destination_type}` cannot load `{loader_file_format}`"
            )

    tz = ZoneInfo(context_tz)
    pipeline = _pipeline(destination_config)
    run_kwargs = {**destination_config.run_kwargs, "loader_file_format": loader_file_format}

    with Container().injectable_context(TimezoneContext(context_tz)):
        if items_format == "dict":
            _run(pipeline, _events(), **run_kwargs)
        else:
            pyarrow = pytest.importorskip("pyarrow")
            _run(
                pipeline, _arrow_events(pyarrow), table_name="events", columns=COLUMNS, **run_kwargs
            )
        rows = _rows_by_id(pipeline)
    caps = pipeline._get_destination_capabilities()

    # naive input, timezone=True: read as a context wall clock, so the instant moves with the zone
    _assert_instant(rows[1]["ts_aware"], NAIVE_IN.replace(tzinfo=tz), caps, tz, "naive -> aware")
    # naive input, timezone=False: nothing to convert, the wall clock is untouched
    _assert_wall_clock(rows[1]["ts_naive"], NAIVE_IN, caps, tz, "naive -> naive")
    # aware input, timezone=True: the instant is kept, only the rendered offset differs
    _assert_instant(rows[2]["ts_aware"], AWARE_IN, caps, tz, "aware -> aware")
    # aware input, timezone=False: converted to the context zone, then stripped, so the wall clock moves
    _assert_wall_clock(
        rows[2]["ts_naive"],
        AWARE_IN.astimezone(tz).replace(tzinfo=None),
        caps,
        tz,
        "aware -> naive",
    )
