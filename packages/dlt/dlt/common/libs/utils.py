from typing import Dict, Any, Optional

from dlt.common.destination.client import SupportsOpenTables
from dlt.common.pipeline import SupportsPipeline
from dlt.common.schema.typing import TTableFormat


def load_open_tables(
    pipeline: SupportsPipeline,
    table_format: TTableFormat,
    *tables: str,
    schema_name: Optional[str] = None,
    include_dlt_tables: bool = False,
) -> Dict[str, Any]:
    with pipeline.destination_client(schema_name=schema_name) as client:
        assert isinstance(
            client, SupportsOpenTables
        ), "This requires destination that supports open tables via SupportsOpenTables interface."

        all_tables = (
            client.schema.tables.values() if include_dlt_tables else client.schema.data_tables()
        )

        schema_open_tables = [
            t["name"] for t in all_tables if client.is_open_table(table_format, t["name"])
        ]
        if len(tables) > 0:
            invalid_tables = set(tables) - set(schema_open_tables)
            if len(invalid_tables) > 0:
                available_schemas_info = ""
                available_schemas = list(pipeline.schemas.keys())
                if len(available_schemas) > 1:
                    available_schemas_info = f" Available schemas are {available_schemas}"
                raise ValueError(
                    f"Schema `{client.schema.name}` does not contain `{table_format=:}` tables with"
                    f" names: {', '.join(invalid_tables)}.{available_schemas_info}"
                )
            schema_open_tables = [t for t in schema_open_tables if t in tables]

        return {name: client.load_open_table(table_format, name) for name in schema_open_tables}
