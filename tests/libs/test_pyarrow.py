from copy import deepcopy

import pyarrow as pa

from dlt.common.libs.pyarrow import py_arrow_to_table_schema_columns, get_py_arrow_datatype
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
