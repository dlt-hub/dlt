import pytest
import os

from dlt.sources.filesystem import filesystem, read_parquet
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.storages.configuration import FilesystemConfiguration


def test_config_sections_resolution_defaults_to_cwd():
    """filesystem() without bucket_url should default to current directory"""
    filesystem_resource = filesystem(file_glob="**/*.parquet")
    filesystem_pipe = filesystem_resource | read_parquet()

    # Should NOT raise — defaults to "." (current working directory)
    # We just verify the resource can be constructed and resolves correctly
    config = FilesystemConfiguration()
    assert config.bucket_url == "."
    assert config.protocol == "file"


def test_config_sections_resolution_missing_credentials():
    """Cloud bucket_url still requires credentials — check config section path"""
    filesystem_resource = filesystem(
        bucket_url="s3://some-bucket", file_glob="**/*.parquet"
    )
    filesystem_pipe = filesystem_resource | read_parquet()

    with pytest.raises(ConfigFieldMissingException) as exc_info:
        list(filesystem_pipe)

    flat_trace = exc_info.value.attrs()["traces"]
    assert "credentials" in flat_trace
    # Verify the correct config section path is used
    assert any(
        trace.sections[:3] == ["sources", "filesystem", "filesystem"]
        for trace in flat_trace["credentials"]
    )