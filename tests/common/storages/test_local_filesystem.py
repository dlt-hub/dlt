import os
import itertools
import pytest
import pathlib
import platform

from dlt.common.storages import fsspec_from_config, FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import FileItemDict, glob_files

from tests.common.storages.utils import assert_sample_files

TEST_SAMPLE_FILES = "tests/common/storages/samples"


@pytest.mark.parametrize(
    "bucket_url,load_content", itertools.product(["file:///", "/", ""], [True, False])
)
def test_filesystem_dict_local(bucket_url: str, load_content: bool) -> None:
    if bucket_url in [""]:
        # relative paths
        bucket_url = TEST_SAMPLE_FILES
    else:
        if bucket_url == "/":
            bucket_url = os.path.abspath(TEST_SAMPLE_FILES)
        else:
            bucket_url = pathlib.Path(TEST_SAMPLE_FILES).absolute().as_uri()

    config = FilesystemConfiguration(bucket_url=bucket_url)
    filesystem, _ = fsspec_from_config(config)
    # use glob to get data
    try:
        all_file_items = list(glob_files(filesystem, bucket_url))
        assert_sample_files(all_file_items, filesystem, config, load_content)
    except NotImplementedError as ex:
        pytest.skip("Skipping due to " + str(ex))


def test_filesystem_decompress() -> None:
    config = FilesystemConfiguration(bucket_url=TEST_SAMPLE_FILES)
    filesystem, _ = fsspec_from_config(config)
    gzip_files = list(glob_files(filesystem, TEST_SAMPLE_FILES, "**/*.gz"))
    assert len(gzip_files) > 0
    for file in gzip_files:
        assert file["encoding"] == "gzip"
        assert file["file_name"].endswith(".gz")
        file_dict = FileItemDict(file, filesystem)
        # read as is (compressed gzip)
        with file_dict.open(compression="disable") as f:
            assert f.read() == file_dict.read_bytes()
        # read as uncompressed text
        with file_dict.open(mode="tr") as f:
            lines = f.readlines()
            assert len(lines) > 1
            assert lines[0].startswith('"1200864931","2015-07-01 00:00:13"')
        # read as uncompressed binary
        with file_dict.open(compression="enable") as f:
            assert f.read().startswith(b'"1200864931","2015-07-01 00:00:13"')


@pytest.mark.skipif(platform.system() != "Windows", reason="Test it only on Windows")
def test_windows_unc_path() -> None:
    config = FilesystemConfiguration(bucket_url=TEST_SAMPLE_FILES)
    config.read_only = True

    unc_path = r"\\localhost\\" + os.path.abspath("tests/common/storages/samples").replace(":", "$")
    abs_path = os.path.abspath(r"tests/common/storages/samples")

    for bucket_url in [
        unc_path,
        "file://" + unc_path,
        abs_path,
        abs_path.replace(r"\\", "/"),
    ]:
        filesystem, _ = fsspec_from_config(config)

        try:
            all_file_items = list(glob_files(filesystem, bucket_url))
            expected_files = [
                "freshman_kgs.csv",
                "freshman_lbs.csv",
                "mlb_players.csv",
                "mlb_teams_2012.csv",
                "mlb_players.jsonl",
                "A881_20230920.csv",
                "A803_20230919.csv",
                "A803_20230920.csv",
                "mlb_players.parquet",
                "taxi.csv.gz",
                "sample.txt",
            ]
            for file in all_file_items:
                file_name = file["file_name"].split("\\")[-1].split("/")[-1]
                assert file_name in expected_files
        except NotImplementedError as ex:
            pytest.skip(f"Skipping due to {str(ex)}")
