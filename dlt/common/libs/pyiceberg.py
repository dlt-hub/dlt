import os
import yaml
from typing import Dict, Any, List, Optional
from pathlib import Path

from fsspec import AbstractFileSystem
from packaging.version import Version

from dlt import version
from dlt.common import logger
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
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithPyicebergConfig

from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


try:
    import pyiceberg
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg.catalog import Catalog as IcebergCatalog
    from pyiceberg.exceptions import NoSuchTableError
    import pyarrow as pa
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


def load_catalog_from_yaml(
    catalog_name: str,
    config_path: Optional[str] = None,
    credentials: Optional[FileSystemCredentials] = None,
) -> IcebergCatalog:
    """Load Iceberg catalog from .pyiceberg.yaml file

    Args:
        catalog_name: Name of the catalog to load from YAML
        config_path: Optional path to .pyiceberg.yaml file. If None, searches in:
                    1. Current directory (./.pyiceberg.yaml)
                    2. DLT project directory (./.dlt/.pyiceberg.yaml)
                    3. Home directory (~/.pyiceberg.yaml)
        credentials: Optional filesystem credentials to merge into catalog config

    Returns:
        IcebergCatalog instance loaded from YAML configuration

    Raises:
        CatalogNotFoundError: If no .pyiceberg.yaml file found or catalog not in file

    Example .pyiceberg.yaml:
        catalog:
          my_catalog:
            type: rest
            uri: https://catalog.example.com
            warehouse: my_warehouse
            credential: token
    """
    from pyiceberg.catalog import load_catalog

    search_paths = []
    if config_path:
        search_paths.append(Path(config_path))
    else:
        # Search in standard locations
        search_paths.extend(
            [
                Path.cwd() / ".pyiceberg.yaml",
                Path.cwd() / ".dlt" / ".pyiceberg.yaml",
                Path.home() / ".pyiceberg.yaml",
            ]
        )

    # Search for the first existing config file
    config = None
    for path in search_paths:
        if path.exists():
            logger.info(f"Loading Iceberg catalog configuration from: {path}")
            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            break

    # If no config file was found, raise error
    if config is None:
        raise CatalogNotFoundError(
            f"No .pyiceberg.yaml file found. Searched in: {', '.join(str(p) for p in search_paths)}"
        )

    # Check if catalog exists in config
    if "catalog" not in config or catalog_name not in config["catalog"]:
        available = list(config.get("catalog", {}).keys())
        raise CatalogNotFoundError(
            f"Catalog '{catalog_name}' not found in .pyiceberg.yaml. "
            f"Available catalogs: {available}"
        )

    logger.info(f"Found catalog '{catalog_name}' in .pyiceberg.yaml")

    # Get catalog config and merge credentials if provided
    catalog_config = config["catalog"][catalog_name].copy()
    if credentials:
        fileio_config = _get_fileio_config(credentials)
        catalog_config.update(fileio_config)

    return load_catalog(catalog_name, **catalog_config)


def load_catalog_from_config(
    catalog_name: str,
    config_dict: Dict[str, Any],
    credentials: Optional[FileSystemCredentials] = None,
) -> IcebergCatalog:
    """Load Iceberg catalog from configuration dictionary

    Args:
        catalog_name: Name of the catalog
        config_dict: Dictionary with catalog configuration (type, uri, warehouse, etc.)
        credentials: Optional filesystem credentials to merge into config

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

    if not config_dict:
        raise CatalogNotFoundError("No configuration dictionary provided")

    logger.info(f"Loading catalog '{catalog_name}' from provided configuration")

    # Merge filesystem credentials if provided
    config_dict = config_dict.copy()
    if credentials:
        fileio_config = _get_fileio_config(credentials)
        config_dict.update(fileio_config)

    return load_catalog(catalog_name, **config_dict)


def load_catalog_from_env(
    catalog_name: Optional[str] = None,
    credentials: Optional[FileSystemCredentials] = None,
) -> IcebergCatalog:
    """Load Iceberg catalog from environment variables

    Args:
        catalog_name: Optional catalog name (defaults to DLT_ICEBERG_CATALOG_NAME env var)
        credentials: Optional filesystem credentials to merge into config

    Returns:
        IcebergCatalog instance

    Raises:
        CatalogNotFoundError: If required environment variables are not set

    Environment variables:
        DLT_ICEBERG_CATALOG_NAME: Name of the catalog (optional, defaults to 'default')
        DLT_ICEBERG_CATALOG_TYPE: Type of catalog ('sql' or 'rest')
        DLT_ICEBERG_CATALOG_URI: Catalog URI
        DLT_ICEBERG_CATALOG_WAREHOUSE: Warehouse name (for REST catalogs)
        DLT_ICEBERG_CATALOG_PROP_*: Additional properties (e.g., PROP_CREDENTIAL, PROP_SCOPE)
    """
    from pyiceberg.catalog import load_catalog

    catalog_name = catalog_name or os.getenv("DLT_ICEBERG_CATALOG_NAME", "default")
    catalog_type = os.getenv("DLT_ICEBERG_CATALOG_TYPE")

    if not catalog_type:
        raise CatalogNotFoundError(
            "DLT_ICEBERG_CATALOG_TYPE environment variable not set. Set to 'sql' or 'rest'."
        )

    logger.info(f"Loading catalog '{catalog_name}' from environment variables")

    if catalog_type == "rest":
        config = {
            "type": "rest",
            "uri": os.getenv("DLT_ICEBERG_CATALOG_URI"),
            "warehouse": os.getenv("DLT_ICEBERG_CATALOG_WAREHOUSE"),
        }

        if not config["uri"] or not config["warehouse"]:
            raise CatalogNotFoundError(
                "For REST catalog, DLT_ICEBERG_CATALOG_URI and "
                "DLT_ICEBERG_CATALOG_WAREHOUSE must be set"
            )

        # Add custom properties from DLT_ICEBERG_CATALOG_PROP_* variables
        for key, value in os.environ.items():
            if key.startswith("DLT_ICEBERG_CATALOG_PROP_"):
                prop_name = key.replace("DLT_ICEBERG_CATALOG_PROP_", "").lower().replace("_", "-")
                config[prop_name] = value

    elif catalog_type == "sql":
        config = {
            "type": "sql",
            "uri": os.getenv("DLT_ICEBERG_CATALOG_URI", "sqlite:///:memory:"),
        }

    else:
        raise CatalogNotFoundError(
            f"Unsupported catalog type: {catalog_type}. Use 'sql' or 'rest'."
        )

    # Merge filesystem credentials if provided
    if credentials:
        fileio_config = _get_fileio_config(credentials)
        config.update(fileio_config)

    return load_catalog(catalog_name, **config)


def get_catalog(
    catalog_name: str = "default",
    catalog_uri: Optional[str] = None,
    catalog_config: Optional[Dict[str, Any]] = None,
    credentials: Optional[FileSystemCredentials] = None,
) -> IcebergCatalog:
    """Get an Iceberg catalog using multiple configuration methods.

    This function tries to load a catalog in the following priority order:
    1. From explicit config dictionary (if catalog_config provided)
    2. From .pyiceberg.yaml file
    3. From environment variables
    4. Fall back to in-memory SQLite catalog

    Args:
        catalog_name: Name of the catalog (default: "default")
        catalog_type: Type of catalog ('sql' or 'rest') - used for fallback
        catalog_uri: URI for SQL catalog - used for fallback
        catalog_config: Optional dictionary with complete catalog configuration
        credentials: Optional filesystem credentials to merge into config

    Returns:
        IcebergCatalog instance

    Examples:
        # Load from .pyiceberg.yaml
        catalog = get_catalog('my_catalog')

        # Load from config dict
        config = {'type': 'rest', 'uri': 'https://...', 'warehouse': 'wh'}
        catalog = get_catalog('my_catalog', catalog_config=config)

        # Load from environment variables
        # (set DLT_ICEBERG_CATALOG_TYPE, DLT_ICEBERG_CATALOG_URI, etc.)
        catalog = get_catalog()
    """
    logger.info(f"Attempting to load Iceberg catalog: {catalog_name}")

    # Priority 1: Explicit config dictionary
    if catalog_config:
        try:
            return load_catalog_from_config(catalog_name, catalog_config, credentials)
        except Exception as e:
            logger.warning(f"Failed to load catalog from config dict: {e}")

    # Priority 2: .pyiceberg.yaml file
    try:
        return load_catalog_from_yaml(catalog_name, credentials=credentials)
    except CatalogNotFoundError as e:
        logger.debug(f"Catalog not found in .pyiceberg.yaml: {e}")
    except Exception as e:
        logger.warning(f"Error loading catalog from .pyiceberg.yaml: {e}")

    # Priority 3: Environment variables
    try:
        return load_catalog_from_env(catalog_name, credentials)
    except CatalogNotFoundError as e:
        logger.debug(f"Catalog not configured via environment variables: {e}")
    except Exception as e:
        logger.warning(f"Error loading catalog from environment: {e}")

    # Priority 4: Fall back to in-memory SQLite
    logger.info("No catalog configuration found, using in-memory SQLite catalog")
    uri = catalog_uri or "sqlite:///:memory:"
    return get_sql_catalog(catalog_name, uri, credentials)


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
    schema: pa.Schema,
    partition_columns: Optional[List[str]] = None,
) -> None:
    schema = ensure_iceberg_compatible_arrow_schema(schema)

    if partition_columns:
        # If the table is partitioned, create it in two steps:
        # (1) start a create-table transaction, and (2) add the partition spec before committing
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
        catalog.create_table(table_id, schema=schema, location=table_location)


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
