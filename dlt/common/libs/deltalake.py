from typing import Optional, Dict, Union
from pathlib import Path

from dlt import version, Pipeline
from dlt.common import logger
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.libs.pyarrow import cast_arrow_schema_types
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages import FilesystemConfiguration
from dlt.common.utils import assert_min_pkg_version
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

try:
    from deltalake import write_deltalake, DeltaTable
    from deltalake.writer import try_get_deltatable
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt deltalake helpers",
        [f"{version.DLT_PKG_NAME}[deltalake]"],
        "Install `deltalake` so dlt can create Delta tables in the `filesystem` destination.",
    )


def ensure_delta_compatible_arrow_schema(schema: pa.Schema) -> pa.Schema:
    """Returns Arrow schema compatible with Delta table format.

    Casts schema to replace data types not supported by Delta.
    """
    ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP = {
        # maps type check function to type factory function
        pa.types.is_null: pa.string(),
        pa.types.is_time: pa.string(),
        pa.types.is_decimal256: pa.string(),  # pyarrow does not allow downcasting to decimal128
    }
    return cast_arrow_schema_types(schema, ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP)


def ensure_delta_compatible_arrow_data(
    data: Union[pa.Table, pa.RecordBatchReader]
) -> Union[pa.Table, pa.RecordBatchReader]:
    """Returns Arrow data compatible with Delta table format.

    Casts `data` schema to replace data types not supported by Delta.
    """
    # RecordBatchReader.cast() requires pyarrow>=17.0.0
    # cast() got introduced in 16.0.0, but with bug
    assert_min_pkg_version(
        pkg_name="pyarrow",
        version="17.0.0",
        msg="`pyarrow>=17.0.0` is needed for `delta` table format on `filesystem` destination.",
    )
    schema = ensure_delta_compatible_arrow_schema(data.schema)
    return data.cast(schema)


def get_delta_write_mode(write_disposition: TWriteDisposition) -> str:
    """Translates dlt write disposition to Delta write mode."""
    if write_disposition in ("append", "merge"):  # `merge` disposition resolves to `append`
        return "append"
    elif write_disposition == "replace":
        return "overwrite"
    else:
        raise ValueError(
            "`write_disposition` must be `append`, `replace`, or `merge`,"
            f" but `{write_disposition}` was provided."
        )


def write_delta_table(
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[pa.Table, pa.RecordBatchReader],
    write_disposition: TWriteDisposition,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Writes in-memory Arrow table to on-disk Delta table."""

    # throws warning for `s3` protocol: https://github.com/delta-io/delta-rs/issues/2460
    # TODO: upgrade `deltalake` lib after https://github.com/delta-io/delta-rs/pull/2500
    # is released
    write_deltalake(  # type: ignore[call-overload]
        table_or_uri=table_or_uri,
        data=ensure_delta_compatible_arrow_data(data),
        mode=get_delta_write_mode(write_disposition),
        schema_mode="merge",  # enable schema evolution (adding new columns)
        storage_options=storage_options,
        engine="rust",  # `merge` schema mode requires `rust` engine
    )


def get_delta_tables(pipeline: Pipeline, *tables: str) -> Dict[str, DeltaTable]:
    """Returns Delta tables in `pipeline.default_schema` as `deltalake.DeltaTable` objects.

    Returned object is a dictionary with table names as keys and `DeltaTable` objects as values.
    Optionally filters dictionary by table names specified as `*tables*`.
    Raises ValueError if table name specified as `*tables` is not found.
    """
    from dlt.common.schema.utils import get_table_format

    with pipeline.destination_client() as client:
        assert isinstance(
            client, FilesystemClient
        ), "The `get_delta_tables` function requires a `filesystem` destination."

        schema_delta_tables = [
            t["name"]
            for t in pipeline.default_schema.tables.values()
            if get_table_format(pipeline.default_schema.tables, t["name"]) == "delta"
        ]
        if len(tables) > 0:
            invalid_tables = set(tables) - set(schema_delta_tables)
            if len(invalid_tables) > 0:
                raise ValueError(
                    "Schema does not contain Delta tables with these names: "
                    f"{', '.join(invalid_tables)}."
                )
            schema_delta_tables = [t for t in schema_delta_tables if t in tables]
        table_dirs = client.get_table_dirs(schema_delta_tables, remote=True)
        storage_options = _deltalake_storage_options(client.config)
        return {
            name: DeltaTable(_dir, storage_options=storage_options)
            for name, _dir in zip(schema_delta_tables, table_dirs)
        }


def _deltalake_storage_options(config: FilesystemConfiguration) -> Dict[str, str]:
    """Returns dict that can be passed as `storage_options` in `deltalake` library."""
    creds = {}
    extra_options = {}
    if config.protocol in ("az", "gs", "s3"):
        creds = config.credentials.to_object_store_rs_credentials()
    if config.deltalake_storage_options is not None:
        extra_options = config.deltalake_storage_options
    shared_keys = creds.keys() & extra_options.keys()
    if len(shared_keys) > 0:
        logger.warning(
            "The `deltalake_storage_options` configuration dictionary contains "
            "keys also provided by dlt's credential system: "
            + ", ".join([f"`{key}`" for key in shared_keys])
            + ". dlt will use the values in `deltalake_storage_options`."
        )
    return {**creds, **extra_options}
