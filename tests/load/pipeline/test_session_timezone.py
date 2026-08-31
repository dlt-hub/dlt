import os
from datetime import datetime, timedelta, timezone  # noqa: I251
from typing import Any

import pytest

from dlt.common.utils import uniq_id

from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info
from tests.utils import TestDataItemFormat

# far from UTC midnight, so a read shifted into the session timezone lands on another date
INSTANT = datetime(2024, 1, 1, tzinfo=timezone.utc)
SESSION_TIMEZONE = "America/New_York"
# `INSTANT` is in January, so New York is on standard time and the offset is not a DST guess
SESSION_OFFSET = timedelta(hours=-5)

# destinations that render a stored instant in the session timezone, so the read carries its offset.
# clickhouse ties the offset to the value instead. it writes the zone into the column type
# as `DateTime64(p,'UTC')`
SESSION_OFFSET_ON_READ = (
    "duckdb",
    "motherduck",
    "ducklake",
    "postgres",
    "redshift",
    "snowflake",
)


def _items(item_type: TestDataItemFormat) -> Any:
    rows = [{"id": 1, "ts": INSTANT}]
    if item_type == "object":
        return rows

    import pandas as pd

    frame = pd.DataFrame(rows)
    if item_type == "pandas":
        return frame

    import pyarrow as pa

    return pa.Table.from_pandas(frame)


def _assert_instant(value: datetime) -> None:
    """Every read carries a timezone and lands on the instant that was loaded."""
    assert value.utcoffset() is not None
    assert value.astimezone(timezone.utc) == INSTANT


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["object", "pandas", "arrow-table"])
def test_session_timezone_keeps_instant(
    destination_config: DestinationTestConfiguration, item_type: TestDataItemFormat
) -> None:
    """A non-UTC session timezone changes the offset a destination returns, not the instant dlt
    stores. Every dataset read carries a timezone, never a naive value.

    Only the driver-native read of a destination in `SESSION_OFFSET_ON_READ` is asserted to carry
    the session offset. `arrow` and `df` go through a conversion that some destinations, postgres
    among them, normalize to UTC.
    """
    if not destination_config.destination_factory().capabilities().supports_session_timezone:
        pytest.skip(f"`{destination_config.destination_type}` has no session timezone")

    os.environ[
        f"DESTINATION__{destination_config.destination_type.upper()}__CREDENTIALS__SESSION_TIMEZONE"
    ] = SESSION_TIMEZONE
    pipeline = destination_config.setup_pipeline("session_tz_" + uniq_id(), dev_mode=True)
    info = pipeline.run(_items(item_type), table_name="events", **destination_config.run_kwargs)
    assert_load_info(info)

    events = pipeline.dataset().events.select("ts")

    rows = events.fetchall()
    assert len(rows) == 1
    if destination_config.destination_type in SESSION_OFFSET_ON_READ:
        assert rows[0][0].utcoffset() == SESSION_OFFSET
    _assert_instant(rows[0][0])

    # arrow keeps a timezone in the column type, though some destinations normalize it to UTC
    table = events.arrow()
    assert table.schema.field("ts").type.tz is not None
    _assert_instant(table.column("ts")[0].as_py())

    # pandas keeps it in the dtype
    frame = events.df()
    assert frame["ts"].dt.tz is not None
    _assert_instant(frame["ts"][0].to_pydatetime())
