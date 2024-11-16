from typing import Dict, Any
import os

from dlt import version, Pipeline
from dlt.common.libs.pyarrow import cast_arrow_schema_types, columns_to_arrow
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.utils import assert_min_pkg_version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithPyicebergConfig
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
    schema: pa.Schema = None,
) -> SqlCatalog:
    """Returns single-table, ephemeral, in-memory Iceberg catalog."""

    # create in-memory catalog
    catalog = SqlCatalog(
        "default",
        uri="sqlite:///:memory:",
        **_get_fileio_config(client.config.credentials),
    )
    catalog.create_namespace(DLT_ICEBERG_NAMESPACE)

    # add table to catalog
    table_id = f"{DLT_ICEBERG_NAMESPACE}.{table_name}"
    table_path = f"{client.dataset_path}/{table_name}"
    metadata_path = f"{table_path}/metadata"
    if client.fs_client.exists(metadata_path):
        # found metadata; register existing table
        table = _register_table(table_id, metadata_path, catalog, client)

        # evolve schema
        if schema is not None:
            with table.update_schema() as update:
                update.union_by_name(ensure_iceberg_compatible_arrow_schema(schema))
    else:
        # found no metadata; create new table
        assert schema is not None
        catalog.create_table(
            table_id,
            schema=ensure_iceberg_compatible_arrow_schema(schema),
            location=client.make_remote_url(table_path),
        )

    return catalog


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


def _get_fileio_config(credentials: CredentialsConfiguration) -> Dict[str, Any]:
    if isinstance(credentials, WithPyicebergConfig):
        return credentials.to_pyiceberg_fileio_config()
    return {}


def _register_table(
    identifier: str,
    metadata_path: str,
    catalog: SqlCatalog,
    client: FilesystemClient,
) -> IcebergTable:
    # TODO: implement faster way to obtain `last_metadata_file` (listing is slow)
    metadata_files = [f for f in client.fs_client.ls(metadata_path) if f.endswith(".json")]
    last_metadata_file = client.make_remote_url(sorted(metadata_files)[-1])
    return catalog.register_table(identifier, last_metadata_file)