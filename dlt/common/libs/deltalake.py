from typing import Optional, Dict, Union

from dlt import version
from dlt.common import logger
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.libs.pyarrow import dataset_to_table, cast_arrow_schema_types
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages import FilesystemConfiguration

try:
    from deltalake import write_deltalake
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt deltalake helpers",
        [f"{version.DLT_PKG_NAME}[deltalake]"],
        "Install `deltalake` so dlt can create Delta tables in the `filesystem` destination.",
    )


def ensure_delta_compatible_arrow_table(table: pa.table) -> pa.Table:
    """Returns Arrow table compatible with Delta table format.

    Casts table schema to replace data types not supported by Delta.
    """
    ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP = {
        # maps type check function to type factory function
        pa.types.is_null: pa.string(),
        pa.types.is_time: pa.string(),
        pa.types.is_decimal256: pa.string(),  # pyarrow does not allow downcasting to decimal128
    }
    adjusted_schema = cast_arrow_schema_types(
        table.schema, ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP
    )
    return table.cast(adjusted_schema)


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
    path: str,
    data: Union[pa.Table, pa.dataset.Dataset],
    write_disposition: TWriteDisposition,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Writes in-memory Arrow table to on-disk Delta table."""

    table = dataset_to_table(data)

    # throws warning for `s3` protocol: https://github.com/delta-io/delta-rs/issues/2460
    # TODO: upgrade `deltalake` lib after https://github.com/delta-io/delta-rs/pull/2500
    # is released
    write_deltalake(  # type: ignore[call-overload]
        table_or_uri=path,
        data=ensure_delta_compatible_arrow_table(table),
        mode=get_delta_write_mode(write_disposition),
        schema_mode="merge",  # enable schema evolution (adding new columns)
        storage_options=storage_options,
        engine="rust",  # `merge` schema mode requires `rust` engine
    )


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
