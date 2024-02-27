from copy import deepcopy
from datetime import timezone, datetime, timedelta
import pyarrow as pa

from dlt.common import pendulum
from dlt.common.libs.pyarrow import (
    from_arrow_scalar,
    get_py_arrow_timestamp,
    py_arrow_to_table_schema_columns,
    get_py_arrow_datatype,
    to_arrow_scalar,
)
from dlt.common.destination import DestinationCapabilitiesContext

from tests.cases import TABLE_UPDATE_COLUMNS_SCHEMA


def test_py_arrow_to_table_schema_columns():
    dlt_schema = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    caps = DestinationCapabilitiesContext.generic_capabilities()
    # The arrow schema will add precision
    dlt_schema["col4"]["precision"] = caps.timestamp_precision
    dlt_schema["col6"]["precision"], dlt_schema["col6"]["scale"] = caps.decimal_precision
    dlt_schema["col11"]["precision"] = caps.timestamp_precision
    dlt_schema["col4_null"]["precision"] = caps.timestamp_precision
    dlt_schema["col6_null"]["precision"], dlt_schema["col6_null"]["scale"] = caps.decimal_precision
    dlt_schema["col11_null"]["precision"] = caps.timestamp_precision

    # Ignoring wei as we can't distinguish from decimal
    dlt_schema["col8"]["precision"], dlt_schema["col8"]["scale"] = (76, 0)
    dlt_schema["col8"]["data_type"] = "decimal"
    dlt_schema["col8_null"]["precision"], dlt_schema["col8_null"]["scale"] = (76, 0)
    dlt_schema["col8_null"]["data_type"] = "decimal"
    # No json type
    dlt_schema["col9"]["data_type"] = "text"
    del dlt_schema["col9"]["variant"]
    dlt_schema["col9_null"]["data_type"] = "text"
    del dlt_schema["col9_null"]["variant"]

    # arrow string fields don't have precision
    del dlt_schema["col5_precision"]["precision"]

    # Convert to arrow schema
    arrow_schema = pa.schema(
        [
            pa.field(
                column["name"],
                get_py_arrow_datatype(column, caps, "UTC"),
                nullable=column["nullable"],
            )
            for column in dlt_schema.values()
        ]
    )

    result = py_arrow_to_table_schema_columns(arrow_schema)

    # Resulting schema should match the original
    assert result == dlt_schema


def test_to_arrow_scalar() -> None:
    naive_dt = get_py_arrow_timestamp(6, tz=None)
    # print(naive_dt)
    # naive datetimes are converted as UTC when time aware python objects are used
    assert to_arrow_scalar(datetime(2021, 1, 1, 5, 2, 32), naive_dt).as_py() == datetime(
        2021, 1, 1, 5, 2, 32
    )
    assert to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone.utc), naive_dt
    ).as_py() == datetime(2021, 1, 1, 5, 2, 32)
    assert to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone(timedelta(hours=-8))), naive_dt
    ).as_py() == datetime(2021, 1, 1, 5, 2, 32) + timedelta(hours=8)

    # naive datetimes are treated like UTC
    utc_dt = get_py_arrow_timestamp(6, tz="UTC")
    dt_converted = to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone(timedelta(hours=-8))), utc_dt
    ).as_py()
    assert dt_converted.utcoffset().seconds == 0
    assert dt_converted == datetime(2021, 1, 1, 13, 2, 32, tzinfo=timezone.utc)

    berlin_dt = get_py_arrow_timestamp(6, tz="Europe/Berlin")
    dt_converted = to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone(timedelta(hours=-8))), berlin_dt
    ).as_py()
    # no dst
    assert dt_converted.utcoffset().seconds == 60 * 60
    assert dt_converted == datetime(2021, 1, 1, 13, 2, 32, tzinfo=timezone.utc)


def test_from_arrow_scalar() -> None:
    naive_dt = get_py_arrow_timestamp(6, tz=None)
    sc_dt = to_arrow_scalar(datetime(2021, 1, 1, 5, 2, 32), naive_dt)

    # this value is like UTC
    py_dt = from_arrow_scalar(sc_dt)
    assert isinstance(py_dt, pendulum.DateTime)
    # and we convert to explicit UTC
    assert py_dt == datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone.utc)

    # converts to UTC
    berlin_dt = get_py_arrow_timestamp(6, tz="Europe/Berlin")
    sc_dt = to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone(timedelta(hours=-8))), berlin_dt
    )
    py_dt = from_arrow_scalar(sc_dt)
    assert isinstance(py_dt, pendulum.DateTime)
    assert py_dt.tzname() == "UTC"
    assert py_dt == datetime(2021, 1, 1, 13, 2, 32, tzinfo=timezone.utc)
