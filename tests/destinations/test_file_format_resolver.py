from typing import List, TYPE_CHECKING
import pytest

from dlt.common.destination.exceptions import DestinationIncompatibleLoaderFileFormatException
from dlt.common.destination.capabilities import (
    DestinationCapabilitiesContext,
    merge_caps_file_formats,
)


def test_file_format_resolution() -> None:
    if TYPE_CHECKING:
        cp = DestinationCapabilitiesContext

    class cp:  # type: ignore[no-redef]
        def __init__(self) -> None:
            self.preferred_loader_file_format: str = None
            self.supported_loader_file_formats: List[str] = []
            self.preferred_staging_file_format: str = None
            self.supported_staging_file_formats: List[str] = []

    destcp = cp()
    stagecp = cp()

    # check regular resolution
    destcp.preferred_loader_file_format = "jsonl"
    destcp.supported_loader_file_formats = ["jsonl", "insert_values", "parquet"]
    assert merge_caps_file_formats("some", "some_s", destcp, None) == (
        "jsonl",
        ["jsonl", "insert_values", "parquet"],
    )

    # check staging resolution with clear preference
    destcp.supported_staging_file_formats = ["jsonl", "insert_values", "parquet"]
    destcp.preferred_staging_file_format = "insert_values"
    stagecp.supported_loader_file_formats = ["jsonl", "insert_values", "parquet"]
    assert merge_caps_file_formats("some", "some_s", destcp, stagecp) == (
        "insert_values",
        ["jsonl", "insert_values", "parquet"],
    )

    # check staging resolution where preference does not match
    destcp.supported_staging_file_formats = ["insert_values", "parquet"]
    destcp.preferred_staging_file_format = "tsv"  # type: ignore[assignment]
    stagecp.supported_loader_file_formats = ["jsonl", "insert_values", "parquet"]
    assert merge_caps_file_formats("some", "some_s", destcp, stagecp) == (
        "insert_values",
        ["insert_values", "parquet"],
    )

    # check incompatible staging
    destcp.supported_staging_file_formats = ["insert_values", "tsv"]  # type: ignore[list-item]
    destcp.preferred_staging_file_format = "tsv"  # type: ignore[assignment]
    stagecp.supported_loader_file_formats = ["jsonl", "parquet"]
    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        merge_caps_file_formats("some", "some_s", destcp, stagecp)
