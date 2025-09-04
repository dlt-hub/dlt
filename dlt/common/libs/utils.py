import sys
from types import ModuleType
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


def is_instance_lib(obj: Any, *, class_ref: str) -> bool:
    """Allows `isinstance()` checks without directly importing 3rd party libraries

    Example:
        ```python
        df = pd.DataFrame(...)
        _isinstance_external(df, class_ref="pandas.DataFrame")
        ```
    """
    import_parts = class_ref.split(".")
    module_name = import_parts[0]

    if module_name not in sys.modules:
        return False

    module: ModuleType = sys.modules[module_name]
    target_class: Any = module
    for part in import_parts[1:]:
        target_class = getattr(target_class, part)

    return isinstance(obj, target_class)
