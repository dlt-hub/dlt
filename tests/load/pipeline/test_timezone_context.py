"""Loads every input awareness x `timezone` hint pair under a `Europe/Berlin` context timezone.

A naive value is read in the context timezone, so its instant moves. An aware value keeps its
instant, so its wall clock moves. UTC is the default and must keep today's behavior.
"""
from datetime import datetime, timezone  # noqa: I251
from typing import Any, Dict, List
import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.configuration.specs.timezone_context import TimezoneContext
from dlt.common.destination import TLoaderFileFormat
from dlt.common.time import ensure_datetime
from dlt.common.utils import uniq_id

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


def _assert_instant(
    value: Any, expected: datetime, caps: DestinationCapabilitiesContext, case: str
) -> None:
    """An aware column keeps `expected`'s instant, whatever offset the destination renders.

    A destination that cannot store an offset drops the label only, leaving the UTC wall clock.
    """
    assert value is not None, case
    if value.tzinfo is not None:
        assert (
            caps.supports_tz_aware_datetime
        ), f"{case}: destination does not support tz-aware datetime, got {value!r}"
        assert value.astimezone(timezone.utc) == expected, case
    else:
        assert (
            not caps.supports_tz_aware_datetime
        ), f"{case}: destination supports tz-aware datetime, got naive {value!r}"
        assert value == expected.replace(tzinfo=None), case


def _assert_wall_clock(
    value: Any, expected: datetime, caps: DestinationCapabilitiesContext, case: str
) -> None:
    """A naive column keeps `expected`'s wall clock, which a destination may label UTC."""
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
        assert value.replace(tzinfo=None) == expected, case


@pytest.mark.parametrize("items_format", ["dict", "arrow"])
@pytest.mark.parametrize("loader_file_format", ["jsonl", "parquet"])
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_berlin_context_timezone_matrix(
    destination_config: DestinationTestConfiguration,
    loader_file_format: TLoaderFileFormat,
    items_format: str,
) -> None:
    """All four pairs, loaded and read back under a Berlin context timezone.

    Both item formats and both file formats must agree: the offset travels in the value text on
    the `jsonl` path and as a column label on the `parquet` path.
    """
    supported = destination_config.raw_capabilities().supported_loader_file_formats or []
    if loader_file_format not in supported:
        pytest.skip(f"`{destination_config.destination_type}` cannot load `{loader_file_format}`")

    pipeline = _pipeline(destination_config)
    run_kwargs = {**destination_config.run_kwargs, "loader_file_format": loader_file_format}

    with Container().injectable_context(TimezoneContext("Europe/Berlin")):
        if items_format == "dict":
            assert_load_info(pipeline.run(_events(), **run_kwargs))
        else:
            pyarrow = pytest.importorskip("pyarrow")
            assert_load_info(
                pipeline.run(
                    _arrow_events(pyarrow), table_name="events", columns=COLUMNS, **run_kwargs
                )
            )
        rows = _rows_by_id(pipeline)
    caps = pipeline._get_destination_capabilities()

    # naive input, timezone=True: read as a Berlin wall clock, so the instant is 22:30 UTC
    _assert_instant(
        rows[1]["ts_aware"],
        datetime(2024, 1, 15, 22, 30, tzinfo=timezone.utc),
        caps,
        "naive -> aware",
    )
    # naive input, timezone=False: nothing to convert, the wall clock is untouched
    _assert_wall_clock(rows[1]["ts_naive"], NAIVE_IN, caps, "naive -> naive")
    # aware input, timezone=True: the instant is kept, only the rendered offset differs
    _assert_instant(rows[2]["ts_aware"], AWARE_IN, caps, "aware -> aware")
    # aware input, timezone=False: converted to Berlin, then stripped, so the wall clock moves
    _assert_wall_clock(rows[2]["ts_naive"], datetime(2024, 1, 16, 0, 30), caps, "aware -> naive")
