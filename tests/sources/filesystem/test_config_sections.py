import pytest

from dlt.sources.filesystem import filesystem, read_parquet
from dlt.common.configuration.exceptions import ConfigFieldMissingException


def test_config_sections_resolution():
    filesystem_resource = filesystem(file_glob="**/*.parquet")
    filesystem_pipe = filesystem_resource | read_parquet()

    with pytest.raises(ConfigFieldMissingException) as exc_info:
        list(filesystem_pipe)

    # NOTE: we check that the first trace related to filesystem has the correct
    # sections set
    flat_trace = exc_info.value.attrs()["traces"]["bucket_url"]
    assert flat_trace[0].key.startswith("SOURCES__FILESYSTEM__FILESYSTEM__")
