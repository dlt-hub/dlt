from datetime import datetime, timezone  # noqa: I251
from typing import Dict, List

import fsspec
import pytest
from fsspec import AbstractFileSystem

from dlt.common import pendulum
from dlt.common.storages.fsspec_filesystem import FileItem, glob_files

from tests.utils import autoindex_http_server

AUTOINDEX_BUCKET_URL = "http://localhost:8190"

# sizes of the csv sample files served by `autoindex_http_server`, keyed by relative path
CSV_SAMPLE_SIZES = {
    "csv/freshman_kgs.csv": 1455,
    "csv/freshman_lbs.csv": 1528,
    "csv/mlb_players.csv": 45498,
    "csv/mlb_teams_2012.csv": 541,
}


@pytest.fixture
def http_fs() -> AbstractFileSystem:
    # a cached listing would mask what each glob actually requests
    return fsspec.filesystem("http", use_listings_cache=False)


def _by_relative_path(items: List[FileItem]) -> Dict[str, FileItem]:
    return {item["relative_path"]: item for item in items}


@pytest.mark.serial
def test_glob_http_without_file_info(autoindex_http_server, http_fs: AbstractFileSystem) -> None:
    """fsspec scrapes http listings from an html index which reports no size, so files list
    without one instead of failing."""
    items = _by_relative_path(list(glob_files(http_fs, AUTOINDEX_BUCKET_URL, "csv/*.csv")))

    assert set(items) == set(CSV_SAMPLE_SIZES)
    for item in items.values():
        assert item["size_in_bytes"] is None
        # no Last-Modified in a listing, mtime falls back to now
        assert item["modification_date"].tzinfo is not None
        assert (datetime.now(timezone.utc) - item["modification_date"]).total_seconds() < 60


@pytest.mark.serial
def test_glob_http_with_file_info(autoindex_http_server, http_fs: AbstractFileSystem) -> None:
    """`fetch_file_info` reads size and mtime per file, which is the only place http reports them."""
    items = _by_relative_path(
        list(glob_files(http_fs, AUTOINDEX_BUCKET_URL, "csv/*.csv", fetch_file_info=True))
    )

    assert set(items) == set(CSV_SAMPLE_SIZES)
    for rel_path, item in items.items():
        assert item["size_in_bytes"] == CSV_SAMPLE_SIZES[rel_path]
        # a real Last-Modified is the file's mtime on disk, never a fresh `now`
        assert (datetime.now(timezone.utc) - item["modification_date"]).total_seconds() > 60
        assert isinstance(item["modification_date"], datetime)
        assert not isinstance(item["modification_date"], pendulum.DateTime)


@pytest.mark.serial
@pytest.mark.parametrize("fetch_file_info", (True, False), ids=("with_info", "without_info"))
def test_glob_http_recursive(
    autoindex_http_server, http_fs: AbstractFileSystem, fetch_file_info: bool
) -> None:
    items = _by_relative_path(
        list(glob_files(http_fs, AUTOINDEX_BUCKET_URL, "**/*.csv", fetch_file_info=fetch_file_info))
    )

    assert set(items) == set(CSV_SAMPLE_SIZES) | {
        "met_csv/A801/A881_20230920.csv",
        "met_csv/A803/A803_20230919.csv",
        "met_csv/A803/A803_20230920.csv",
    }


@pytest.mark.serial
@pytest.mark.parametrize("fetch_file_info", (True, False), ids=("with_info", "without_info"))
def test_glob_http_single_file(
    autoindex_http_server, http_fs: AbstractFileSystem, fetch_file_info: bool
) -> None:
    """A glob without wildcards resolves through `info` so it reports size either way."""
    items = list(
        glob_files(
            http_fs, AUTOINDEX_BUCKET_URL, "csv/mlb_players.csv", fetch_file_info=fetch_file_info
        )
    )

    assert len(items) == 1
    assert items[0]["file_name"] == "mlb_players.csv"
    assert items[0]["size_in_bytes"] == CSV_SAMPLE_SIZES["csv/mlb_players.csv"]
