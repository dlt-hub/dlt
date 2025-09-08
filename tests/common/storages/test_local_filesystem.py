import os
import pytest
import pathlib
from urllib.parse import quote
from typing import Tuple

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.storages import fsspec_from_config, FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import FileItemDict, glob_files

from tests.common.storages.utils import assert_sample_files, TEST_SAMPLE_FILES
from tests.utils import skipifnotwindows, skipifwindows

UNC_LOCAL_PATH = r"\\localhost\c$\tests\common\test.csv"
UNC_LOCAL_EXT_PATH = r"\\?\UNC\localhost\c$\tests\common\test.csv"
UNC_WSL_PATH = r"\\wsl.localhost\Ubuntu-18.04\home\rudolfix\ .dlt"


@skipifnotwindows
@pytest.mark.parametrize(
    "bucket_url,file_url",
    (
        (UNC_LOCAL_PATH, pathlib.PureWindowsPath(UNC_LOCAL_PATH).as_uri()),
        (UNC_LOCAL_EXT_PATH, pathlib.PureWindowsPath(UNC_LOCAL_EXT_PATH).as_uri()),
        (UNC_WSL_PATH, pathlib.PureWindowsPath(UNC_WSL_PATH).as_uri()),
        (r"C:\hello", "file:///C:/hello"),
        (r"\\?\C:\hello", "file://%3F/C%3A/hello"),
        (r"a\b $\b", "file:///" + pathlib.Path(r"a\\" + quote("b $") + r"\b").resolve().as_posix()),
        # same paths but with POSIX separators
        (
            UNC_LOCAL_PATH.replace("\\", "/"),
            pathlib.PureWindowsPath(UNC_LOCAL_PATH).as_uri(),
        ),
        (
            UNC_WSL_PATH.replace("\\", "/"),
            pathlib.PureWindowsPath(UNC_WSL_PATH).as_uri(),
        ),
        (r"C:\hello".replace("\\", "/"), "file:///C:/hello"),
        (
            r"a\b $\b".replace("\\", "/"),
            "file:///" + pathlib.Path(r"a\\" + quote("b $") + r"\b").resolve().as_posix(),
        ),
    ),
)
def test_local_path_win_configuration(bucket_url: str, file_url: str) -> None:
    assert FilesystemConfiguration.is_local_path(bucket_url) is True
    assert FilesystemConfiguration.make_file_url(bucket_url) == file_url

    c = resolve_configuration(FilesystemConfiguration(bucket_url))
    assert c.protocol == "file"
    assert c.bucket_url == file_url
    assert FilesystemConfiguration.make_local_path(c.bucket_url) == str(
        pathlib.Path(bucket_url).resolve()
    )


@skipifnotwindows
@pytest.mark.parametrize(
    "bucket_url",
    (
        r"~\Documents\csv_files\a",
        r"~\Documents\csv_files\a".replace("\\", "/"),
    ),
)
def test_local_user_win_path_configuration(bucket_url: str) -> None:
    file_url = "file:///" + pathlib.Path(bucket_url).expanduser().as_posix().lstrip("/")
    assert FilesystemConfiguration.is_local_path(bucket_url) is True
    assert FilesystemConfiguration.make_file_url(bucket_url) == file_url

    c = resolve_configuration(FilesystemConfiguration(bucket_url))
    assert c.protocol == "file"
    assert c.bucket_url == file_url
    assert FilesystemConfiguration.make_local_path(c.bucket_url) == str(
        pathlib.Path(bucket_url).expanduser()
    )


@skipifnotwindows
def test_file_win_configuration() -> None:
    assert (
        FilesystemConfiguration.make_local_path("file://localhost/c$/samples")
        == r"\\localhost\c$\samples"
    )
    assert (
        FilesystemConfiguration.make_local_path("file://///localhost/c$/samples")
        == r"\\localhost\c$\samples"
    )
    assert FilesystemConfiguration.make_local_path("file:///C:/samples") == r"C:\samples"


@skipifwindows
@pytest.mark.parametrize(
    "bucket_url,file_url",
    (
        (r"/src/local/app", "file:///src/local/app"),
        (r"_storage", "file://" + pathlib.Path("_storage").resolve().as_posix()),
    ),
)
def test_file_posix_configuration(bucket_url: str, file_url: str) -> None:
    assert FilesystemConfiguration.is_local_path(bucket_url) is True
    assert FilesystemConfiguration.make_file_url(bucket_url) == file_url

    c = resolve_configuration(FilesystemConfiguration(bucket_url))
    assert c.protocol == "file"
    assert c.bucket_url == file_url
    assert FilesystemConfiguration.make_local_path(c.bucket_url) == str(
        pathlib.Path(bucket_url).resolve()
    )


@skipifwindows
@pytest.mark.parametrize(
    "bucket_url",
    ("~/docs",),
)
def test_local_user_posix_path_configuration(bucket_url: str) -> None:
    file_url = "file:///" + pathlib.Path(bucket_url).expanduser().as_posix().lstrip("/")
    assert FilesystemConfiguration.is_local_path(bucket_url) is True
    assert FilesystemConfiguration.make_file_url(bucket_url) == file_url

    c = resolve_configuration(FilesystemConfiguration(bucket_url))
    assert c.protocol == "file"
    assert c.bucket_url == file_url
    assert FilesystemConfiguration.make_local_path(c.bucket_url) == str(
        pathlib.Path(bucket_url).expanduser()
    )


@pytest.mark.parametrize(
    "bucket_url,local_path",
    (
        ("file://_storage", "_storage"),
        ("file://_storage/a/b/c.txt", "_storage/a/b/c.txt"),
    ),
)
def test_file_host_configuration(bucket_url: str, local_path: str) -> None:
    # recognized as UNC path on windows. on POSIX the path does not make sense and start with "//"
    assert FilesystemConfiguration.is_local_path(bucket_url) is False
    assert FilesystemConfiguration.make_local_path(bucket_url) == str(
        pathlib.Path("//" + local_path)
    )

    c = resolve_configuration(FilesystemConfiguration(bucket_url))
    assert c.protocol == "file"
    assert c.bucket_url == bucket_url


@pytest.mark.parametrize(
    "bucket_url,local_path,norm_bucket_url",
    (
        ("file:", "", ""),
        ("file://", "", ""),
        ("file:/", "/", "file:///" + pathlib.Path("/").resolve().as_posix().lstrip("/")),
    ),
)
def test_file_filesystem_configuration(
    bucket_url: str, local_path: str, norm_bucket_url: str
) -> None:
    assert FilesystemConfiguration.is_local_path(bucket_url) is False
    if local_path == "":
        # those paths are invalid
        with pytest.raises(ConfigurationValueError):
            FilesystemConfiguration.make_local_path(bucket_url)
    else:
        assert FilesystemConfiguration.make_local_path(bucket_url) == str(
            pathlib.Path(local_path).resolve()
        )
        assert FilesystemConfiguration.make_file_url(local_path) == norm_bucket_url

    if local_path == "":
        with pytest.raises(ConfigurationValueError):
            resolve_configuration(FilesystemConfiguration(bucket_url))
    else:
        c = resolve_configuration(FilesystemConfiguration(bucket_url))
        assert c.protocol == "file"
        assert c.bucket_url == bucket_url
        assert FilesystemConfiguration.make_local_path(c.bucket_url) == str(
            pathlib.Path(local_path).resolve()
        )


@pytest.mark.parametrize(
    "bucket_url",
    (
        "s3://dlt-ci-test-bucket/",
        "gdrive://Xaie902-1/a/c",
    ),
)
def test_filesystem_bucket_configuration(bucket_url: str) -> None:
    assert FilesystemConfiguration.is_local_path(bucket_url) is False


@pytest.mark.parametrize("bucket_url", ("file:/", "file:///", "/", ""))
@pytest.mark.parametrize("load_content", (True, False))
@pytest.mark.parametrize("glob_filter", ("**", "**/*.csv", "*.txt", "met_csv/A803/*.csv"))
def test_filesystem_dict_local(bucket_url: str, load_content: bool, glob_filter: str) -> None:
    if bucket_url in [""]:
        # relative paths
        bucket_url = str(pathlib.Path(TEST_SAMPLE_FILES))
    else:
        if bucket_url == "/":
            bucket_url = os.path.abspath(TEST_SAMPLE_FILES)
        else:
            bucket_url = bucket_url + pathlib.Path(TEST_SAMPLE_FILES).resolve().as_posix()

    config = FilesystemConfiguration(bucket_url=bucket_url)
    filesystem, _ = fsspec_from_config(config)
    # use glob to get data
    all_file_items = list(glob_files(filesystem, bucket_url, file_glob=glob_filter))
    assert_sample_files(all_file_items, filesystem, config, load_content, glob_filter)


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


# create windows UNC paths, on POSIX systems they are not used
WIN_ABS_PATH = os.path.abspath(TEST_SAMPLE_FILES)
WIN_ABS_EXT_PATH = "\\\\?\\" + os.path.abspath(TEST_SAMPLE_FILES)
WIN_UNC_PATH = "\\\\localhost\\" + WIN_ABS_PATH.replace(":", "$").lower()
WIN_UNC_EXT_PATH = "\\\\?\\UNC\\localhost\\" + WIN_ABS_PATH.replace(":", "$").lower()


if os.name == "nt":
    windows_local_files: Tuple[str, ...] = (
        WIN_UNC_PATH,
        "file:///" + pathlib.Path(WIN_UNC_PATH).as_posix(),
        "file://localhost/" + pathlib.Path(WIN_ABS_PATH).as_posix().replace(":", "$"),
        # WIN_UNC_EXT_PATH,
        # "file:///" + pathlib.Path(WIN_UNC_EXT_PATH).as_posix(),
        # "file://localhost/" + pathlib.Path(WIN_UNC_EXT_PATH).as_posix().replace(":", "$"),
        WIN_ABS_PATH,
        WIN_ABS_EXT_PATH,
        pathlib.Path(WIN_ABS_PATH).as_uri(),
        pathlib.Path(WIN_ABS_EXT_PATH).as_uri(),
        # r"\\wsl.localhost\Ubuntu-18.04\home\rudolfix\src\dlt\tests\common\storages\samples"
    )
else:
    windows_local_files = ()


@skipifnotwindows
@pytest.mark.parametrize("bucket_url", windows_local_files)
@pytest.mark.parametrize("load_content", [True, False])
@pytest.mark.parametrize("glob_filter", ("**", "**/*.csv", "*.txt", "met_csv/A803/*.csv"))
def test_windows_unc_path(load_content: bool, bucket_url: str, glob_filter: str) -> None:
    config = FilesystemConfiguration(bucket_url=TEST_SAMPLE_FILES)
    config.read_only = True
    fs_client, _ = fsspec_from_config(config)
    all_file_items = list(glob_files(fs_client, bucket_url, file_glob=glob_filter))
    assert_sample_files(all_file_items, fs_client, config, load_content, glob_filter)
