from typing import Dict
import os

from dlt import version, Pipeline
from dlt.common.libs.pyarrow import cast_arrow_schema_types
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.utils import assert_min_pkg_version
from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

assert_min_pkg_version(
    pkg_name="sqlalchemy",
    version="2.0.18",
    msg="`sqlalchemy>=2.0.18` is needed for `iceberg` table format on `filesystem` destination.",
)

try:
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg.catalog.sql import SqlCatalog
    import pyarrow as pa
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt pyiceberg helpers",
        [f"{version.DLT_PKG_NAME}[pyiceberg]"],
        "Install `pyiceberg` so dlt can create Iceberg tables in the `filesystem` destination.",
    )


DLT_ICEBERG_CATALOGS_DIR = "dlt_iceberg_catalogs"
DLT_ICEBERG_NAMESPACE = "dlt"


def ensure_iceberg_compatible_arrow_schema(schema: pa.Schema) -> pa.Schema:
    ARROW_TO_ICEBERG_COMPATIBLE_ARROW_TYPE_MAP = {
        pa.types.is_time: pa.string(),
        pa.types.is_decimal256: pa.string(),  # pyarrow does not allow downcasting to decimal128
    }
    return cast_arrow_schema_types(schema, ARROW_TO_ICEBERG_COMPATIBLE_ARROW_TYPE_MAP)


def ensure_iceberg_compatible_arrow_data(data: pa.Table) -> pa.Table:
    schema = ensure_iceberg_compatible_arrow_schema(data.schema)
    return data.cast(schema)


def write_iceberg_table(
    table: IcebergTable,
    data: pa.Table,
    write_disposition: TWriteDisposition,
) -> None:
    if write_disposition == "append":
        table.append(ensure_iceberg_compatible_arrow_data(data))
    elif write_disposition == "replace":
        table.overwrite(ensure_iceberg_compatible_arrow_data(data))


def get_catalog(
    client: FilesystemClient,
    table_name: str,
) -> SqlCatalog:
    catalogs_dir = client.dataset_path + "/" + DLT_ICEBERG_CATALOGS_DIR
    os.makedirs(catalogs_dir, exist_ok=True)
    return SqlCatalog(
        "default",
        uri=f"sqlite:///{catalogs_dir}/{table_name}_catalog.db",
    )


def get_iceberg_tables(
    pipeline: Pipeline, *tables: str, schema_name: str = None
) -> Dict[str, IcebergTable]:
    from dlt.common.schema.utils import get_table_format

    with pipeline.destination_client(schema_name=schema_name) as client:
        assert isinstance(
            client, FilesystemClient
        ), "The `get_iceberg_tables` function requires a `filesystem` destination."

        schema_iceberg_tables = [
            t["name"]
            for t in client.schema.tables.values()
            if get_table_format(client.schema.tables, t["name"]) == "iceberg"
        ]
        if len(tables) > 0:
            invalid_tables = set(tables) - set(schema_iceberg_tables)
            if len(invalid_tables) > 0:
                available_schemas = ""
                if len(pipeline.schema_names) > 1:
                    available_schemas = f" Available schemas are {pipeline.schema_names}"
                raise ValueError(
                    f"Schema {client.schema.name} does not contain Iceberg tables with these names:"
                    f" {', '.join(invalid_tables)}.{available_schemas}"
                )
            schema_iceberg_tables = [t for t in schema_iceberg_tables if t in tables]

        return {
            name: get_catalog(client, name).load_table(f"{DLT_ICEBERG_NAMESPACE}.{name}")
            for name in schema_iceberg_tables
        }
