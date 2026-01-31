import os
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import warnings

from fsspec import AbstractFileSystem
from packaging.version import Version

from dlt import version
from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.time import precise_time
from dlt.common.libs.pyarrow import cast_arrow_schema_types
from dlt.common.libs.utils import load_open_tables
from dlt.common.pipeline import SupportsPipeline
from dlt.common.schema.typing import TWriteDisposition, TTableSchema
from dlt.common.schema.utils import get_first_column_name_with_prop, get_columns_names_with_prop
from dlt.common.utils import assert_min_pkg_version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages.configuration import FileSystemCredentials, FilesystemConfiguration
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithPyicebergConfig
from dlt.common.configuration.inject import with_config

from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


try:
    import pyiceberg
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg.catalog import Catalog as IcebergCatalog
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.partitioning import (
        UNPARTITIONED_PARTITION_SPEC,
        PartitionSpec as IcebergPartitionSpec,
    )
    import pyarrow as pa
    from pydantic import BaseModel, ConfigDict, Field
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt pyiceberg helpers",
        [f"{version.DLT_PKG_NAME}[pyiceberg]"],
        "Install `pyiceberg` so dlt can create Iceberg tables in the `filesystem` destination.",
    )

pyiceberg_semver = Version(pyiceberg.__version__)

if pyiceberg_semver < Version("0.10.0"):
    import pyiceberg.io.pyarrow as _pio

    _orig_get_kwargs = _pio._get_parquet_writer_kwargs

    def _patched_get_parquet_writer_kwargs(table_properties):  # type: ignore[no-untyped-def]
        """Return the original kwargs **plus** store_decimal_as_integer=True."""
        kwargs = _orig_get_kwargs(table_properties)
        kwargs.setdefault("store_decimal_as_integer", True)
        return kwargs

    _pio._get_parquet_writer_kwargs = _patched_get_parquet_writer_kwargs


def ensure_iceberg_compatible_arrow_schema(schema: pa.Schema) -> pa.Schema:
    ARROW_TO_ICEBERG_COMPATIBLE_ARROW_TYPE_MAP = {
        pa.types.is_time32: pa.time64("us"),
        pa.types.is_decimal256: pa.string(),  # pyarrow does not allow downcasting to decimal128
        pa.types.is_dictionary: lambda t_: t_.value_type,
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
    start_ts = precise_time()
    if write_disposition == "append":
        table.append(ensure_iceberg_compatible_arrow_data(data))
    elif write_disposition == "replace":
        table.overwrite(ensure_iceberg_compatible_arrow_data(data))
    logger.debug(
        f"pyiceberg: {write_disposition} arrow with {data.num_rows} rows to table {table.name()} at"
        f" location {table.location()} took {(precise_time() - start_ts)} seconds."
    )


def merge_iceberg_table(
    table: IcebergTable,
    data: pa.Table,
    schema: TTableSchema,
    load_table_name: str,
) -> None:
    """Merges in-memory Arrow data into on-disk Iceberg table."""
    strategy = schema["x-merge-strategy"]  # type: ignore[typeddict-item]
    if strategy == "upsert":
        # evolve schema
        with table.update_schema() as update:
            update.union_by_name(ensure_iceberg_compatible_arrow_schema(data.schema))

        if "parent" in schema:
            join_cols = [get_first_column_name_with_prop(schema, "unique")]
        else:
            join_cols = get_columns_names_with_prop(schema, "primary_key")

        # TODO: replace the batching method with transaction with pyiceberg's release after 0.9.1
        for rb in data.to_batches(max_chunksize=1_000):
            batch_tbl = pa.Table.from_batches([rb])
            batch_tbl = ensure_iceberg_compatible_arrow_data(batch_tbl)

            table.upsert(
                df=batch_tbl,
                join_cols=join_cols,
                when_matched_update_all=True,
                when_not_matched_insert_all=True,
                case_sensitive=True,
            )
    else:
        raise ValueError(
            f'Merge strategy "{strategy}" is not supported for Iceberg tables. '
            f'Table: "{load_table_name}".'
        )


def get_sql_catalog(
    catalog_name: str,
    uri: str,
    credentials: FileSystemCredentials,
    properties: Dict[str, Any] = None,
) -> IcebergCatalog:  # noqa: F821
    assert_min_pkg_version(
        pkg_name="sqlalchemy",
        version="2.0.18",
        msg=(
            "`sqlalchemy>=2.0.18` is needed for `iceberg` table format on `filesystem` destination."
        ),
    )

    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog(
        catalog_name,
        uri=uri,
        **_get_fileio_config(credentials),
        **(properties or {}),
    )


class CatalogNotFoundError(Exception):
    """Raised when a catalog cannot be found in the specified configuration method"""

    pass


class PyicebergCatalogConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str = Field(..., description="Iceberg catalog type")  # noqa
    uri: str = Field(..., description="Iceberg catalog URI")
    warehouse: str = Field(..., description="Warehouse name")


@configspec
class IcebergConfig(BaseConfiguration):
    # Iceberg catalog configuration
    iceberg_catalog_name: str = "default"
    """Name of the Iceberg catalog to use. Corresponds to catalog name in .pyiceberg.yaml"""

    iceberg_catalog_type: Optional[str] = "sql"
    """Type of Iceberg catalog: 'sql', 'rest', 'glue', 'hive', etc."""

    iceberg_catalog_config: Optional[Dict[str, Any]] = None
    """
    Optional dictionary with complete catalog configuration.
    If provided, will be used instead of loading from .pyiceberg.yaml.
    Example for REST catalog:
        {
            'type': 'rest',
            'uri': 'https://catalog.example.com',
            'warehouse': 'my_warehouse',
            'credential': 'token',
            'scope': 'PRINCIPAL_ROLE:ALL'
        }
    Example for SQL catalog:
        {
            'type': 'sql',
            'uri': 'postgresql://user:pass@localhost/catalog'
        }

    Example for secrets.toml:
        [iceberg_catalog]
        iceberg_catalog_name = "default"
        iceberg_catalog_type = "rest"

        [iceberg_catalog.iceberg_catalog_config]
        uri = "http://localhost:8181/catalog"
        warehouse = "default"
        header.X-Iceberg-Access-Delegation = "remote-signing"
        py-io-impl = "pyiceberg.io.fsspec.FsspecFileIO"
        s3.endpoint = "https://cool-bucket.com/"
        s3.access-key-id = "cool-bucket-access-key"
        s3.secret-access-key = "cool-bucket-secret-key"
        s3.region = "cool-bucket-region"
    """


def _load_catalog_from_pyiceberg(
    catalog_name: str,
) -> IcebergCatalog:
    """Load Iceberg catalog through pyiceberg load_catalog mechanism. See https://py.iceberg.apache.org/configuration/#setting-configuration-values

    Args:
        catalog_name: Name of the catalog to load from YAML

    Returns:
        IcebergCatalog instance loaded from YAML configuration

    Raises:
        CatalogNotFoundError: If no .pyiceberg.yaml file found or catalog not in file

    Search paths (in order):
        1. PYICEBERG_HOME environment variable (pyiceberg standard)
        2. DLT run directory
        3. DLT settings directory

    Example .pyiceberg.yaml:
        catalog:
          my_catalog:
            type: rest
            uri: https://catalog.example.com
            warehouse: my_warehouse
            credential: token
    """
    from pyiceberg.catalog import load_catalog
    import dlt

    active_run_context = dlt.current.run_context()

    # Search through potential paths for Iceberg Config
    search_paths = []

    pyiceberg_home = os.environ.get("PYICEBERG_HOME")
    if pyiceberg_home:
        search_paths.append(Path(pyiceberg_home) / ".pyiceberg.yaml")

    # Add dlt-specific paths
    search_paths.extend(
        [
            Path(active_run_context.run_dir) / ".pyiceberg.yaml",
            Path(active_run_context.get_setting(".pyiceberg.yaml")),
        ]
    )

    # Search for the first existing config file and confirm 'catalog:' is present
    no_config_file_found = True
    for path in search_paths:
        if path.exists():
            logger.debug(f"Searching for catalog configuration in: {path}")
            with open(path, "r", encoding="utf-8") as f:
                contents = f.read()
                if "catalog:" in contents:
                    no_config_file_found = False
                    break

    # Check if any PYICEBERG_CATALOG_* environment variable is set
    pyiceberg_env_var = any(key.startswith("PYICEBERG_CATALOG_") for key in os.environ)

    # If no config file was found, raise error
    if no_config_file_found and not pyiceberg_env_var:
        raise CatalogNotFoundError(
            "No .pyiceberg.yaml file found. Searched in:"
            f" {', '.join(str(p) for p in search_paths)}. No PYICEBERG_CATALOG_* environment"
            " variables found."
        )

    return load_catalog(catalog_name)


def _load_catalog_from_config(
    catalog_name: str,
    config_dict: Dict[str, Any],
    credentials: Optional[FileSystemCredentials] = None,
) -> IcebergCatalog:
    """Load Iceberg catalog from configuration dictionary

    Args:
        catalog_name: Name of the catalog
        config_dict: Dictionary with catalog configuration (type, uri, warehouse, etc.)

    Returns:
        IcebergCatalog instance

    Raises:
        CatalogNotFoundError: If config_dict is None or empty

    Example:
        config = {
            'type': 'rest',
            'uri': 'https://catalog.example.com',
            'warehouse': 'my_warehouse',
            'credential': 'token'
        }
        catalog = load_catalog_from_config('my_catalog', config)
    """
    from pyiceberg.catalog import load_catalog

    # Validate config
    PyicebergCatalogConfig(**config_dict)

    if not config_dict:
        raise CatalogNotFoundError("No configuration dictionary provided")

    logger.info(f"Loading catalog '{catalog_name}' from provided configuration")

    if credentials:
        config_dict.update(_get_fileio_config(credentials))

    return load_catalog(catalog_name, **config_dict)


@with_config(spec=IcebergConfig, sections="iceberg_catalog")
def get_catalog(
    iceberg_catalog_name: str = "default",
    iceberg_catalog_type: Optional[str] = None,
    iceberg_catalog_config: Optional[Dict[str, Any]] = None,
    credentials: Optional[FileSystemCredentials] = None,
) -> IcebergCatalog:
    """Get an Iceberg catalog using multiple configuration methods.

    This function tries to load a catalog in the following priority order:
    1. From explicit config dictionary (if iceberg_catalog_config provided)
    2. From .pyiceberg.yaml file or from environment variables (PYICEBERG_*). Resolved by pyiceberg load_catalog mechanism. See https://py.iceberg.apache.org/configuration/#setting-configuration-values
    4. Fall back to in-memory SQLite catalog

    Args:
        iceberg_catalog_name: Name of the catalog (default: "default")
        iceberg_catalog_type: Type of catalog ('sql' or 'rest')
        iceberg_catalog_config: Optional dictionary with complete catalog configuration
        credentials: Optional filesystem credentials. This is ONLY used for backward compatibility with in-memory SQLite catalog.

    Returns:
        IcebergCatalog instance

    Examples:

        # Load from config dict
        config = {'type': 'rest', 'uri': 'https://...', 'warehouse': 'wh'}
        catalog = get_catalog('my_catalog', iceberg_catalog_type='rest', iceberg_catalog_config=config)

        # Load from .pyiceberg.yaml
        catalog = get_catalog('my_catalog', iceberg_catalog_type='sql')

        # Load from environment variables
        # (set PYICEBERG_CATALOG_TYPE, PYICEBERG_CATALOG_URI, etc.)
        catalog = get_catalog('my_catalog', iceberg_catalog_type='rest')

    """
    logger.info(f"Attempting to load Iceberg catalog: {iceberg_catalog_name}")

    # Validate catalog type
    supported_catalog_types = ["sql", "rest"]
    if iceberg_catalog_type not in supported_catalog_types:
        raise ValueError(f"Unsupported catalog type: {iceberg_catalog_type}. Use 'sql' or 'rest'.")

    # Priority 1: Explicit config dictionary (most specific and comes from secrets.toml)
    if iceberg_catalog_config:
        try:
            return _load_catalog_from_config(iceberg_catalog_name, iceberg_catalog_config)
        except CatalogNotFoundError as e:
            logger.warning(f"Failed to load catalog from config dict: {e}")

    # Priority 2: .pyiceberg.yaml file (PyIceberg standard)
    try:
        return _load_catalog_from_pyiceberg(iceberg_catalog_name)
    except CatalogNotFoundError as e:
        logger.debug(f"Catalog not found in .pyiceberg.yaml: {e}")

    # Priority 3: Fall back to in-memory SQLite (backward compatibility)
    logger.info(
        "No catalog configuration found, using in-memory SQLite catalog (backward compatibility)"
    )
    return get_sql_catalog(iceberg_catalog_name, "sqlite:///:memory:", credentials)


def evolve_table(
    catalog: IcebergCatalog,
    client: FilesystemClient,
    table_id: str,
    table_location: str,
    schema: Optional[pa.Schema] = None,
) -> IcebergTable:
    try:
        table = catalog.load_table(table_id)
    except NoSuchTableError:
        # add table to catalog
        metadata_path = f"{table_location.rstrip('/')}/metadata"
        if client.fs_client.exists(metadata_path):
            # found metadata; register existing table
            table = register_table(
                table_id, metadata_path, catalog, client.fs_client, client.config
            )
        else:
            raise

    # evolve schema
    if schema is not None:
        with table.update_schema() as update:
            update.union_by_name(ensure_iceberg_compatible_arrow_schema(schema))

    return table


def create_table(
    catalog: IcebergCatalog,
    table_id: str,
    table_location: str,
    schema: Union[pa.Schema, "pyiceberg.schema.Schema"],
    partition_columns: Optional[List[str]] = None,
    partition_spec: Optional[IcebergPartitionSpec] = UNPARTITIONED_PARTITION_SPEC,
) -> None:
    if isinstance(schema, pa.Schema):
        schema = ensure_iceberg_compatible_arrow_schema(schema)

    if partition_columns:
        warnings.warn(
            "partition_columns is deprecated. Use partition_spec instead.", DeprecationWarning
        )
        with catalog.create_table_transaction(
            table_id,
            schema=schema,
            location=table_location,
        ) as txn:
            # add partitioning
            with txn.update_spec() as update_spec:
                for col in partition_columns:
                    update_spec.add_identity(col)
    else:
        catalog.create_table(
            identifier=table_id,
            schema=schema,
            location=table_location,
            partition_spec=partition_spec,
        )


def get_iceberg_tables(
    pipeline: SupportsPipeline,
    *tables: str,
    schema_name: Optional[str] = None,
    include_dlt_tables: bool = False,
) -> Dict[str, IcebergTable]:
    """Returns Iceberg tables in `pipeline.default_schema (default)` or `schema_name` as `pyiceberg.Table` objects.

    Returned object is a dictionary with table names as keys and `Tables` objects as values.
    Optionally filters dictionary by table names specified as `*tables*`.
    Raises ValueError if table name specified as `*tables` is not found. You may try to switch to other
    schemas via `schema_name` argument.
    """
    return load_open_tables(
        pipeline, "iceberg", *tables, schema_name=schema_name, include_dlt_tables=include_dlt_tables
    )


def _get_fileio_config(credentials: CredentialsConfiguration) -> Dict[str, Any]:
    if isinstance(credentials, WithPyicebergConfig):
        return credentials.to_pyiceberg_fileio_config()
    return {}


def get_last_metadata_file(
    metadata_path: str, fs_client: AbstractFileSystem, config: FilesystemConfiguration
) -> str:
    # TODO: read version-hint.txt first and save it in filesystem
    try:
        metadata_files = [f for f in fs_client.ls(metadata_path) if f.endswith(".json")]
    except FileNotFoundError:
        raise DestinationUndefinedEntity(FileNotFoundError(metadata_path))
    if len(metadata_files) == 0:
        raise DestinationUndefinedEntity(FileNotFoundError(metadata_path))
    return make_location(sorted(metadata_files)[-1], config)


def register_table(
    identifier: str,
    metadata_path: str,
    catalog: IcebergCatalog,
    fs_client: AbstractFileSystem,
    config: FilesystemConfiguration,
) -> IcebergTable:
    last_metadata_file = get_last_metadata_file(metadata_path, fs_client, config)
    return catalog.register_table(identifier, last_metadata_file)


def make_location(path: str, config: FilesystemConfiguration) -> str:
    # don't use file protocol for local files because duckdb does not support it
    # https://github.com/duckdb/duckdb/issues/13669
    location = config.make_url(path)
    if config.is_local_filesystem and os.name == "nt":
        # pyiceberg cannot deal with windows absolute urls
        location = location.replace("file:///", "file://")
    return location
