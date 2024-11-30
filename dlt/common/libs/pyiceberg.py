from typing import Dict, Any, List, Optional

from dlt import version, Pipeline
from dlt.common.libs.pyarrow import cast_arrow_schema_types
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.utils import assert_min_pkg_version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithPyicebergConfig
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


try:
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg.catalog import MetastoreCatalog
    import pyarrow as pa
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt pyiceberg helpers",
        [f"{version.DLT_PKG_NAME}[pyiceberg]"],
        "Install `pyiceberg` so dlt can create Iceberg tables in the `filesystem` destination.",
    )


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


def get_sql_catalog(credentials: FileSystemCredentials) -> "SqlCatalog":  # type: ignore[name-defined] # noqa: F821
    assert_min_pkg_version(
        pkg_name="sqlalchemy",
        version="2.0.18",
        msg=(
            "`sqlalchemy>=2.0.18` is needed for `iceberg` table format on `filesystem` destination."
        ),
    )

    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog(
        "default",
        uri="sqlite:///:memory:",
        **_get_fileio_config(credentials),
    )


def create_or_evolve_table(
    catalog: MetastoreCatalog,
    client: FilesystemClient,
    table_name: str,
    namespace_name: Optional[str] = None,
    schema: Optional[pa.Schema] = None,
    partition_columns: Optional[List[str]] = None,
) -> MetastoreCatalog:
    # add table to catalog
    table_id = f"{namespace_name}.{table_name}"
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
        with catalog.create_table_transaction(
            table_id,
            schema=ensure_iceberg_compatible_arrow_schema(schema),
            location=_make_path(table_path, client),
        ) as txn:
            # add partitioning
            with txn.update_spec() as update_spec:
                for col in partition_columns:
                    update_spec.add_identity(col)

    return catalog


def get_catalog(
    client: FilesystemClient,
    table_name: str,
    namespace_name: Optional[str] = None,
    schema: Optional[pa.Schema] = None,
    partition_columns: Optional[List[str]] = None,
) -> MetastoreCatalog:
    """Returns single-table, ephemeral, in-memory Iceberg catalog."""

    # create in-memory catalog
    catalog: MetastoreCatalog = get_sql_catalog(client.config.credentials)

    # create namespace
    if namespace_name is None:
        namespace_name = client.dataset_name
    catalog.create_namespace(namespace_name)

    # add table to catalog
    catalog = create_or_evolve_table(
        catalog=catalog,
        client=client,
        table_name=table_name,
        namespace_name=namespace_name,
        schema=schema,
        partition_columns=partition_columns,
    )

    return catalog


def get_iceberg_tables(
    pipeline: Pipeline, *tables: str, schema_name: Optional[str] = None
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
            name: get_catalog(client, name).load_table(f"{pipeline.dataset_name}.{name}")
            for name in schema_iceberg_tables
        }


def _get_fileio_config(credentials: CredentialsConfiguration) -> Dict[str, Any]:
    if isinstance(credentials, WithPyicebergConfig):
        return credentials.to_pyiceberg_fileio_config()
    return {}


def _get_last_metadata_file(metadata_path: str, client: FilesystemClient) -> str:
    # TODO: implement faster way to obtain `last_metadata_file` (listing is slow)
    metadata_files = [f for f in client.fs_client.ls(metadata_path) if f.endswith(".json")]
    return _make_path(sorted(metadata_files)[-1], client)


def _register_table(
    identifier: str,
    metadata_path: str,
    catalog: MetastoreCatalog,
    client: FilesystemClient,
) -> IcebergTable:
    last_metadata_file = _get_last_metadata_file(metadata_path, client)
    return catalog.register_table(identifier, last_metadata_file)


def _make_path(path: str, client: FilesystemClient) -> str:
    # don't use file protocol for local files because duckdb does not support it
    # https://github.com/duckdb/duckdb/issues/13669
    return path if client.is_local_filesystem else client.config.make_url(path)
