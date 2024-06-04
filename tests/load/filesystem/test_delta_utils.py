import os
from typing import Iterator, Tuple, cast

import pytest
from deltalake import DeltaTable

import dlt
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.configuration.specs import AwsCredentials
from dlt.destinations.impl.filesystem.delta_utils import (
    write_delta_table,
    _deltalake_storage_options,
)
from dlt.destinations.impl.filesystem.filesystem import (
    FilesystemClient,
    FilesystemDestinationClientConfiguration,
)

from tests.cases import arrow_table_all_data_types


@pytest.fixture()
def filesystem_client(default_buckets_env: str) -> Iterator[Tuple[FilesystemClient, str]]:
    """Returns tuple of `FilesystemClient` instance and remote directory string.

    Remote directory is removed on teardown.
    """
    # setup
    client = cast(FilesystemClient, dlt.pipeline(destination="filesystem").destination_client())
    remote_dir = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] + "/tmp_dir"

    yield (client, remote_dir)

    # teardown
    if client.fs_client.exists(remote_dir):
        client.fs_client.rm(remote_dir, recursive=True)


def test_deltalake_storage_options() -> None:
    config = FilesystemDestinationClientConfiguration()

    # no credentials, no deltalake_storage_options
    config.bucket_url = "_storage://foo"
    assert _deltalake_storage_options(config) == dict()

    # no credentials, yes deltalake_storage_options
    config.deltalake_storage_options = {"foo": "bar"}
    assert _deltalake_storage_options(config) == {"foo": "bar"}

    # yes credentials, yes deltalake_storage_options: no shared keys
    creds = AwsCredentials(
        aws_access_key_id="dummy_key_id",
        aws_secret_access_key="dummy_acces_key",  # type: ignore[arg-type]
        aws_session_token="dummy_session_token",  # type: ignore[arg-type]
        region_name="dummy_region_name",
    )
    config.credentials = creds
    config.bucket_url = "s3://foo"
    assert _deltalake_storage_options(config).keys() == {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "region",
        "foo",
    }

    # yes credentials, yes deltalake_storage_options: yes shared keys
    config.deltalake_storage_options = {"aws_access_key_id": "i_will_overwrite"}
    assert _deltalake_storage_options(config).keys() == {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "region",
    }
    assert _deltalake_storage_options(config)["aws_access_key_id"] == "i_will_overwrite"


def test_write_delta_table(filesystem_client) -> None:
    if os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"].startswith("memory://"):
        pytest.skip(
            "`deltalake` library does not support `memory` protocol (write works, read doesn't)"
        )

    client, remote_dir = filesystem_client
    client = cast(FilesystemClient, client)

    if client.config.protocol == "s3":
        client.config.deltalake_storage_options = {"AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    storage_options = _deltalake_storage_options(client.config)

    with pytest.raises(Exception):
        # bug in `delta-rs` causes error when writing big decimal values
        # https://github.com/delta-io/delta-rs/issues/2510
        # if this test fails, the bug has been fixed and we should remove this
        # note from the docs:
        write_delta_table(
            remote_dir + "/corrupt_delta_table",
            arrow_table_all_data_types("arrow-table", include_decimal_default_precision=True)[0],
            write_disposition="append",
            storage_options=storage_options,
        )

    arrow_table = arrow_table_all_data_types(
        "arrow-table",
        include_decimal_default_precision=False,
        include_decimal_arrow_max_precision=True,
        num_rows=2,
    )[0]

    # first write should create Delta table with same shape as input Arrow table
    write_delta_table(
        remote_dir, arrow_table, write_disposition="append", storage_options=storage_options
    )
    dt = DeltaTable(remote_dir, storage_options=storage_options)
    assert dt.version() == 0
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (arrow_table.num_rows, arrow_table.num_columns)

    # table contents should be different because "time" column has type `string`
    # in Delta table, but type `time` in Arrow source table
    assert not dt_arrow_table.equals(arrow_table)
    casted_cols = ("null", "time", "decimal_arrow_max_precision")
    assert dt_arrow_table.drop_columns(casted_cols).equals(arrow_table.drop_columns(casted_cols))

    # another `append` should create a new table version with twice the number of rows
    write_delta_table(
        remote_dir, arrow_table, write_disposition="append", storage_options=storage_options
    )
    dt = DeltaTable(remote_dir, storage_options=storage_options)
    assert dt.version() == 1
    assert dt.to_pyarrow_table().shape == (arrow_table.num_rows * 2, arrow_table.num_columns)

    # the `replace` write disposition should trigger a "logical delete"
    write_delta_table(
        remote_dir, arrow_table, write_disposition="replace", storage_options=storage_options
    )
    dt = DeltaTable(remote_dir, storage_options=storage_options)
    assert dt.version() == 2
    assert dt.to_pyarrow_table().shape == (arrow_table.num_rows, arrow_table.num_columns)

    # the previous table version should still exist
    dt.load_version(1)
    assert dt.to_pyarrow_table().shape == (arrow_table.num_rows * 2, arrow_table.num_columns)

    # `merge` should resolve to `append` bevavior
    write_delta_table(
        remote_dir, arrow_table, write_disposition="merge", storage_options=storage_options
    )
    dt = DeltaTable(remote_dir, storage_options=storage_options)
    assert dt.version() == 3
    assert dt.to_pyarrow_table().shape == (arrow_table.num_rows * 2, arrow_table.num_columns)

    # add column in source table
    evolved_arrow_table = arrow_table.append_column(
        "new", pa.array([1 for _ in range(arrow_table.num_rows)])
    )
    assert (
        evolved_arrow_table.num_columns == arrow_table.num_columns + 1
    )  # ensure column was appendend

    # new column should be propagated to Delta table (schema evolution is supported)
    write_delta_table(
        remote_dir, evolved_arrow_table, write_disposition="append", storage_options=storage_options
    )
    dt = DeltaTable(remote_dir, storage_options=storage_options)
    assert dt.version() == 4
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (arrow_table.num_rows * 3, evolved_arrow_table.num_columns)
    assert "new" in dt_arrow_table.schema.names
    assert dt_arrow_table.column("new").to_pylist() == [1, 1, None, None, None, None]

    # providing a subset of columns should lead to missing columns being null-filled
    write_delta_table(
        remote_dir, arrow_table, write_disposition="append", storage_options=storage_options
    )
    dt = DeltaTable(remote_dir, storage_options=storage_options)
    assert dt.version() == 5
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (arrow_table.num_rows * 4, evolved_arrow_table.num_columns)
    assert dt_arrow_table.column("new").to_pylist() == [None, None, 1, 1, None, None, None, None]

    with pytest.raises(ValueError):
        # unsupported value for `write_disposition` should raise ValueError
        write_delta_table(
            remote_dir,
            arrow_table,
            write_disposition="foo",  # type:ignore[arg-type]
            storage_options=storage_options,
        )
