import os
import posixpath
import pytest
from dlt.common.configuration.inject import with_config, last_config

from dlt.common import pendulum
from dlt.common.storages import filesystem_from_config, FilesystemConfiguration
from dlt.common.storages.filesystem import MTIME_DISPATCH
from dlt.common.utils import uniq_id

from tests.utils import preserve_environ, autouse_test_storage


@with_config(spec=FilesystemConfiguration, sections=("destination", "filesystem"))
def get_config(config: FilesystemConfiguration = None) -> FilesystemConfiguration:
    return config


def test_filesystem_instance(all_buckets_env: str) -> None:
    bucket_url = os.environ['DESTINATION__FILESYSTEM__BUCKET_URL']
    config = get_config()
    assert bucket_url.startswith(config.protocol)
    filesystem, url = filesystem_from_config(config)
    if config.protocol != "file":
        assert bucket_url.endswith(url)
    # do a few file ops
    now = pendulum.now()
    filename = "filesystem_common_" + uniq_id()
    file_url = posixpath.join(url, filename)
    try:
        filesystem.pipe(file_url, b"test bytes")
        files = filesystem.ls(url, detail=True)
        details = next(d for d in files if d["name"] == file_url)
        # print(details)
        # print(MTIME_DISPATCH[config.protocol](details))
        assert (MTIME_DISPATCH[config.protocol](details) - now).seconds < 60
    finally:
        filesystem.rm(file_url)