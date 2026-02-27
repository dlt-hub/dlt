from unittest.mock import MagicMock, patch
import pytest

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
    assert hf_filesystem.capabilities().supported_table_formats == []
    assert hf_filesystem.capabilities().supported_merge_strategies == []
    non_hf_filesystem = filesystem(FILE_BUCKET)
    assert not non_hf_filesystem.is_hf
    assert non_hf_filesystem.spec == FilesystemDestinationClientConfiguration
    assert non_hf_filesystem.client_class == FilesystemClient
    assert non_hf_filesystem.capabilities().preferred_loader_file_format == "jsonl"
    assert non_hf_filesystem.capabilities().parquet_format is None


def test_resolve_bucket_url_ignores_foreign_credentials():
    """_resolve_bucket_url must not fail when unrelated CREDENTIALS env var is set.

    This happens when other destinations (e.g. athena) set CREDENTIALS to a postgres
    connection string — the filesystem factory must still resolve bucket_url without
    trying to parse those credentials.
    """
    # bare CREDENTIALS env var with incompatible value must not break protocol detection
    with custom_environ(
        {
            "DESTINATION__FILESYSTEM__BUCKET_URL": HF_BUCKET,
            "CREDENTIALS": "postgres://loader:password@localhost:5432/dlt_data",
        }
    ):
        fs = filesystem()
        assert fs.is_hf

    # without bucket_url, _resolve_bucket_url returns None and is_hf is False
    with custom_environ({"CREDENTIALS": "postgres://loader:password@localhost:5432/dlt_data"}):
        fs = filesystem()
        assert not fs.is_hf


@pytest.mark.parametrize("hf_dataset_card", [True, False])
def test_hf_dataset_card_flag(hf_dataset_card: bool) -> None:
    """Card operations are called only when hf_dataset_card is True."""
    client = MagicMock(spec=HfFilesystemClient)
    client.config = MagicMock(spec=HfFilesystemDestinationClientConfiguration)
    client.config.hf_dataset_card = hf_dataset_card
    client.repo_id = "org/dataset"
    client.hf_api = MagicMock()
    client.fs_client = MagicMock()

    # test create_dataset
    HfFilesystemClient.create_dataset(client)
    client.hf_api.create_repo.assert_called_once()
    if hf_dataset_card:
        client._safe_card_operation.assert_called_once()
    else:
        client._safe_card_operation.assert_not_called()

    client.reset_mock()

    # test complete_load
    with patch.object(FilesystemClient, "complete_load"):
        HfFilesystemClient.complete_load(client, "1234567890.123")
    if hf_dataset_card:
        client._safe_card_operation.assert_called_once()
        args = client._safe_card_operation.call_args
        assert "1234567890.123" in args[0]
    else:
        client._safe_card_operation.assert_not_called()
