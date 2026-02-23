from dlt.common.utils import custom_environ
from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.configuration import (
    FilesystemDestinationClientConfiguration,
    HfFilesystemDestinationClientConfiguration,
)
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient, HfFilesystemClient
from tests.load.utils import AWS_BUCKET, FILE_BUCKET, HF_BUCKET, MEMORY_BUCKET


def test_filesystem_factory_is_hf():
    # NOTE: we don't test TOML configs explicitly; testing env vars indirectly tests TOML configs
    # because `is_hf` relies on `resolve_configuration` function

    # bucket url not provided
    assert not filesystem().is_hf

    # bucket url provided via constructor
    assert filesystem(HF_BUCKET).is_hf
    assert not filesystem(FILE_BUCKET).is_hf

    # bucket url provided via environment variable
    is_hf_envs = [
        {"BUCKET_URL": HF_BUCKET},
        {"DESTINATION__BUCKET_URL": HF_BUCKET},
        {"DESTINATION__FILESYSTEM__BUCKET_URL": HF_BUCKET},
    ]
    for env in is_hf_envs:
        with custom_environ(env):
            assert filesystem().is_hf
    not_is_hf_envs = [
        {"BUCKET_URL": FILE_BUCKET},
        {"DESTINATION__BUCKET_URL": AWS_BUCKET},
        {"DESTINATION__FILESYSTEM__BUCKET_URL": MEMORY_BUCKET},
    ]
    for env in not_is_hf_envs:
        with custom_environ(env):
            assert not filesystem().is_hf

    # with destination name
    with custom_environ({"DESTINATION__MY_DESTINATION__BUCKET_URL": HF_BUCKET}):
        assert filesystem(destination_name="my_destination").is_hf
    with custom_environ({"DESTINATION__MY_DESTINATION__BUCKET_URL": FILE_BUCKET}):
        assert not filesystem(destination_name="my_destination").is_hf

    # assert adjustments are made when protocol is `hf`
    hf_filesystem = filesystem(HF_BUCKET)
    assert hf_filesystem.is_hf
    assert hf_filesystem.spec == HfFilesystemDestinationClientConfiguration
    assert hf_filesystem.client_class == HfFilesystemClient
    assert hf_filesystem.capabilities().preferred_loader_file_format == "parquet"
    assert hf_filesystem.capabilities().parquet_format.write_page_index is True
    assert hf_filesystem.capabilities().parquet_format.use_content_defined_chunking is True
    non_hf_filesystem = filesystem(FILE_BUCKET)
    assert not non_hf_filesystem.is_hf
    assert non_hf_filesystem.spec == FilesystemDestinationClientConfiguration
    assert non_hf_filesystem.client_class == FilesystemClient
    assert non_hf_filesystem.capabilities().preferred_loader_file_format == "jsonl"
    assert non_hf_filesystem.capabilities().parquet_format is None
