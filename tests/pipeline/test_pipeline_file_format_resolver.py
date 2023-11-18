from typing import List, TYPE_CHECKING

import dlt
import pytest

from dlt.common.exceptions import DestinationIncompatibleLoaderFileFormatException, DestinationLoadingViaStagingNotSupported, DestinationNoStagingMode
from dlt.common.destination.capabilities import DestinationCapabilitiesContext

def test_file_format_resolution()  -> None:
    # raise on destinations that does not support staging
    with pytest.raises(DestinationLoadingViaStagingNotSupported):
        p = dlt.pipeline(pipeline_name="managed_state_pipeline", destination="postgres", staging="filesystem")

    # raise on staging that does not support staging interface
    with pytest.raises(DestinationNoStagingMode):
        p = dlt.pipeline(pipeline_name="managed_state_pipeline", staging="postgres")

    p = dlt.pipeline(pipeline_name="managed_state_pipeline")

    if TYPE_CHECKING:
        cp = DestinationCapabilitiesContext

    class cp():  # type: ignore[no-redef]
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
    assert p._resolve_loader_file_format("some", "some", destcp, None, None) == "jsonl"

    # check resolution with input
    assert p._resolve_loader_file_format("some", "some", destcp, None, "parquet") == "parquet"

    # check invalid input
    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        assert p._resolve_loader_file_format("some", "some", destcp, None, "csv")  # type: ignore[arg-type]

    # check staging resolution with clear preference
    destcp.supported_staging_file_formats = ["jsonl", "insert_values", "parquet"]
    destcp.preferred_staging_file_format = "insert_values"
    stagecp.supported_loader_file_formats = ["jsonl", "insert_values", "parquet"]
    assert p._resolve_loader_file_format("some", "some", destcp, stagecp, None) == "insert_values"

    # check invalid input
    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        p._resolve_loader_file_format("some", "some", destcp, stagecp, "csv")  # type: ignore[arg-type]

    # check staging resolution where preference does not match
    destcp.supported_staging_file_formats = ["insert_values", "parquet"]
    destcp.preferred_staging_file_format = "csv"  # type: ignore[assignment]
    stagecp.supported_loader_file_formats = ["jsonl", "insert_values", "parquet"]
    assert p._resolve_loader_file_format("some", "some", destcp, stagecp, None) == "insert_values"
    assert p._resolve_loader_file_format("some", "some", destcp, stagecp, "parquet") == "parquet"

    # check incompatible staging
    destcp.supported_staging_file_formats = ["insert_values", "csv"]  # type: ignore[list-item]
    destcp.preferred_staging_file_format = "csv"  # type: ignore[assignment]
    stagecp.supported_loader_file_formats = ["jsonl", "parquet"]
    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        p._resolve_loader_file_format("some", "some", destcp, stagecp, None)
