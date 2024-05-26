from typing import Optional, Dict

import pyarrow as pa

from dlt.common.json import json
from dlt.common.libs.pyarrow import adjust_arrow_schema
from dlt.common.schema.typing import TWriteDisposition
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


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
    adjusted_schema = adjust_arrow_schema(table.schema, ARROW_TO_DELTA_COMPATIBLE_ARROW_TYPE_MAP)
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
    table: pa.Table,
    write_disposition: TWriteDisposition,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Writes in-memory Arrow table to on-disk Delta table."""
    from deltalake import write_deltalake

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


def _deltalake_storage_options(client: FilesystemClient) -> Dict[str, str]:
    """Returns dict that can be passed as `storage_options` in `deltalake` library."""
    # TODO: implement method properly and remove hard-coded values.
    # `delta-rs` does not currently (version 0.17.4) support `gdrive` protocol
    storage_options = None
    if client.config.protocol == "s3":
        from dlt.common.configuration.specs import AwsCredentials

        assert isinstance(client.config.credentials, AwsCredentials)
        storage_options = client.config.credentials.to_session_credentials()
        storage_options["AWS_REGION"] = client.config.credentials.region_name
        # https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/#enable-unsafe-writes-in-s3-opt-in
        storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    elif client.config.protocol == "gs":
        from dlt.common.configuration.specs import GcpServiceAccountCredentials

        assert isinstance(client.config.credentials, GcpServiceAccountCredentials)
        gcs_creds = client.config.credentials.to_gcs_credentials()
        gcs_creds["token"]["private_key_id"] = "921837921798379812"
        storage_options = {"GOOGLE_SERVICE_ACCOUNT_KEY": json.dumps(gcs_creds["token"])}
    else:
        storage_options = {  # `delta-rs` requires string values
            k: str(v) for k, v in client.fs_client.storage_options.items()
        }
    return storage_options
