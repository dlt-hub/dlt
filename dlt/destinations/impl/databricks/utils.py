from dlt.common.destination.typing import PreparedTableSchema

from dlt.destinations.impl.databricks.databricks_adapter import INSERT_API_HINT
from dlt.destinations.impl.databricks.typing import (
    DEFAULT_DATABRICKS_INSERT_API,
    TDatabricksInsertApi,
)


def get_databricks_insert_api(table: PreparedTableSchema) -> TDatabricksInsertApi:
    return table.get(INSERT_API_HINT, DEFAULT_DATABRICKS_INSERT_API)  # type: ignore[return-value]
