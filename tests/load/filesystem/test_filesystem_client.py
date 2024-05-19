import posixpath
import os
from unittest import mock
from typing import Iterator, Tuple, cast

import pytest

import dlt

from dlt.common.time import ensure_pendulum_datetime
from dlt.common.utils import digest128, uniq_id
from dlt.common.storages import FileStorage, ParsedLoadJobFileName

from dlt.destinations.impl.filesystem.filesystem import (
    FilesystemClient,
    FilesystemDestinationClientConfiguration,
    INIT_FILE_NAME,
)


from dlt.destinations.path_utils import create_path, prepare_datetime_params
from tests.cases import arrow_table_all_data_types
from tests.load.filesystem.utils import perform_load
from tests.utils import clean_test_storage, init_test_logging
from tests.load.utils import TEST_FILE_LAYOUTS

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def storage() -> FileStorage:
    return clean_test_storage(init_normalize=True, init_loader=True)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


NORMALIZED_FILES = [
    "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl",
    "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl",
]


def test_filesystem_destination_configuration() -> None:
    assert FilesystemDestinationClientConfiguration().fingerprint() == ""
    assert FilesystemDestinationClientConfiguration(
        bucket_url="s3://cool"
    ).fingerprint() == digest128("s3://cool")


@pytest.mark.parametrize("write_disposition", ("replace", "append", "merge"))
@pytest.mark.parametrize("layout", TEST_FILE_LAYOUTS)
def test_successful_load(write_disposition: str, layout: str, with_gdrive_buckets_env: str) -> None:
    """Test load is successful with an empty destination dataset"""
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    dataset_name = "test_" + uniq_id()
    timestamp = ensure_pendulum_datetime("2024-04-05T09:16:59.942779Z")
    mocked_timestamp = {"state": {"created_at": timestamp}}
    with mock.patch(
        "dlt.current.load_package",
        return_value=mocked_timestamp,
    ), perform_load(
        dataset_name,
        NORMALIZED_FILES,
        write_disposition=write_disposition,
    ) as load_info:
        client, jobs, _, load_id = load_info
        layout = client.config.layout
        dataset_path = posixpath.join(client.bucket_path, client.config.dataset_name)

        # Assert dataset dir exists
        assert client.fs_client.isdir(dataset_path)

        # Sanity check, there are jobs
        assert jobs
        for job in jobs:
            assert job.state() == "completed"
            job_info = ParsedLoadJobFileName.parse(job.file_name())
            destination_path = posixpath.join(
                dataset_path,
                layout.format(
                    schema_name=client.schema.name,
                    table_name=job_info.table_name,
                    load_id=load_id,
                    file_id=job_info.file_id,
                    ext=job_info.file_format,
                    **prepare_datetime_params(load_package_timestamp=timestamp),
                ),
            )

            # File is created with correct filename and path
            assert client.fs_client.isfile(destination_path)


@pytest.mark.parametrize("layout", TEST_FILE_LAYOUTS)
def test_replace_write_disposition(layout: str, default_buckets_env: str) -> None:
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    dataset_name = "test_" + uniq_id()
    # NOTE: context manager will delete the dataset at the end so keep it open until the end
    # state is typed now
    timestamp = ensure_pendulum_datetime("2024-04-05T09:16:59.942779Z")
    mocked_timestamp = {"state": {"created_at": timestamp}}
    with mock.patch(
        "dlt.current.load_package",
        return_value=mocked_timestamp,
    ), perform_load(
        dataset_name,
        NORMALIZED_FILES,
        write_disposition="replace",
    ) as load_info:
        client, _, root_path, load_id1 = load_info
        layout = client.config.layout
        # this path will be kept after replace
        job_2_load_1_path = posixpath.join(
            root_path,
            create_path(
                layout,
                NORMALIZED_FILES[1],
                client.schema.name,
                load_id1,
                load_package_timestamp=timestamp,
                extra_placeholders=client.config.extra_placeholders,
            ),
        )

        with perform_load(
            dataset_name, [NORMALIZED_FILES[0]], write_disposition="replace"
        ) as load_info:
            client, _, root_path, load_id2 = load_info

            # this one we expect to be replaced with
            job_1_load_2_path = posixpath.join(
                root_path,
                create_path(
                    layout,
                    NORMALIZED_FILES[0],
                    client.schema.name,
                    load_id2,
                    load_package_timestamp=timestamp,
                    extra_placeholders=client.config.extra_placeholders,
                ),
            )

            # First file from load1 remains, second file is replaced by load2
            # assert that only these two files are in the destination folder
            paths = []
            for basedir, _dirs, files in client.fs_client.walk(
                client.dataset_path, detail=False, refresh=True
            ):
                # remove internal paths
                if "_dlt" in basedir:
                    continue
                for f in files:
                    if f == INIT_FILE_NAME:
                        continue
                    paths.append(posixpath.join(basedir, f))

            ls = set(paths)
            assert ls == {job_2_load_1_path, job_1_load_2_path}


@pytest.mark.parametrize("layout", TEST_FILE_LAYOUTS)
def test_append_write_disposition(layout: str, default_buckets_env: str) -> None:
    """Run load twice with append write_disposition and assert that there are two copies of each file in destination"""
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)
    dataset_name = "test_" + uniq_id()
    # NOTE: context manager will delete the dataset at the end so keep it open until the end
    # also we would like to have reliable timestamp for this test so we patch it
    timestamp = ensure_pendulum_datetime("2024-04-05T09:16:59.942779Z")
    mocked_timestamp = {"state": {"created_at": timestamp}}
    with mock.patch(
        "dlt.current.load_package",
        return_value=mocked_timestamp,
    ), perform_load(
        dataset_name,
        NORMALIZED_FILES,
        write_disposition="append",
    ) as load_info:
        client, jobs1, root_path, load_id1 = load_info
        with perform_load(dataset_name, NORMALIZED_FILES, write_disposition="append") as load_info:
            client, jobs2, root_path, load_id2 = load_info
            layout = client.config.layout
            expected_files = [
                create_path(
                    layout,
                    job.file_name(),
                    client.schema.name,
                    load_id1,
                    load_package_timestamp=timestamp,
                    extra_placeholders=client.config.extra_placeholders,
                )
                for job in jobs1
            ] + [
                create_path(
                    layout,
                    job.file_name(),
                    client.schema.name,
                    load_id2,
                    load_package_timestamp=timestamp,
                    extra_placeholders=client.config.extra_placeholders,
                )
                for job in jobs2
            ]
            expected_files = sorted([posixpath.join(root_path, fn) for fn in expected_files])

            paths = []
            for basedir, _dirs, files in client.fs_client.walk(
                client.dataset_path, detail=False, refresh=True
            ):
                # remove internal paths
                if "_dlt" in basedir:
                    continue
                for f in files:
                    if f == INIT_FILE_NAME:
                        continue
                    paths.append(posixpath.join(basedir, f))
            assert list(sorted(paths)) == expected_files


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


def test_write_delta_table(filesystem_client) -> None:
    if os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"].startswith("memory://"):
        pytest.skip(
            "`deltalake` library does not support `memory` protocol (write works, read doesn't)"
        )

    import pyarrow as pa
    from deltalake import DeltaTable

    client, remote_dir = filesystem_client
    client = cast(FilesystemClient, client)

    with pytest.raises(Exception):
        # bug in `delta-rs` causes error when writing big decimal values
        # https://github.com/delta-io/delta-rs/issues/2510
        # if this test fails, the bug has been fixed and we should remove this
        # note from the docs:
        client._write_delta_table(
            remote_dir + "/corrupt_delta_table",
            arrow_table_all_data_types("arrow-table", include_decimal_default_precision=True)[0],
            write_disposition="append",
        )

    arrow_table = arrow_table_all_data_types(
        "arrow-table",
        include_decimal_default_precision=False,
        include_decimal_arrow_max_precision=True,
        num_rows=2,
    )[0]

    # first write should create Delta table with same shape as input Arrow table
    client._write_delta_table(remote_dir, arrow_table, write_disposition="append")
    dt = DeltaTable(remote_dir, storage_options=client._deltalake_storage_options)
    assert dt.version() == 0
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (arrow_table.num_rows, arrow_table.num_columns)

    # table contents should be different because "time" column has type `string`
    # in Delta table, but type `time` in Arrow source table
    assert not dt_arrow_table.equals(arrow_table)
    casted_cols = ("null", "time", "decimal_arrow_max_precision")
    assert dt_arrow_table.drop_columns(casted_cols).equals(arrow_table.drop_columns(casted_cols))

    # another `append` should create a new table version with twice the number of rows
    client._write_delta_table(remote_dir, arrow_table, write_disposition="append")
    dt = DeltaTable(remote_dir, storage_options=client._deltalake_storage_options)
    assert dt.version() == 1
    assert dt.to_pyarrow_table().shape == (arrow_table.num_rows * 2, arrow_table.num_columns)

    # the `replace` write disposition should trigger a "logical delete"
    client._write_delta_table(remote_dir, arrow_table, write_disposition="replace")
    dt = DeltaTable(remote_dir, storage_options=client._deltalake_storage_options)
    assert dt.version() == 2
    assert dt.to_pyarrow_table().shape == (arrow_table.num_rows, arrow_table.num_columns)

    # the previous table version should still exist
    dt.load_version(1)
    assert dt.to_pyarrow_table().shape == (arrow_table.num_rows * 2, arrow_table.num_columns)

    # `merge` should resolve to `append` bevavior
    client._write_delta_table(remote_dir, arrow_table, write_disposition="merge")
    dt = DeltaTable(remote_dir, storage_options=client._deltalake_storage_options)
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
    client._write_delta_table(remote_dir, evolved_arrow_table, write_disposition="append")
    dt = DeltaTable(remote_dir, storage_options=client._deltalake_storage_options)
    assert dt.version() == 4
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (arrow_table.num_rows * 3, evolved_arrow_table.num_columns)
    assert "new" in dt_arrow_table.schema.names
    assert dt_arrow_table.column("new").to_pylist() == [1, 1, None, None, None, None]

    # providing a subset of columns should lead to missing columns being null-filled
    client._write_delta_table(remote_dir, arrow_table, write_disposition="append")
    dt = DeltaTable(remote_dir, storage_options=client._deltalake_storage_options)
    assert dt.version() == 5
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (arrow_table.num_rows * 4, evolved_arrow_table.num_columns)
    assert dt_arrow_table.column("new").to_pylist() == [None, None, 1, 1, None, None, None, None]

    with pytest.raises(ValueError):
        # unsupported value for `write_disposition` should raise ValueError
        client._write_delta_table(remote_dir, arrow_table, write_disposition="foo")
