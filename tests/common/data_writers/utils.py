import os
from typing import Set, Literal


from dlt.common.data_writers.buffered import BufferedDataWriter, DataWriter
from dlt.common.destination import TLoaderFileFormat, DestinationCapabilitiesContext

from tests.utils import TEST_STORAGE_ROOT

ALL_WRITERS: Set[Literal[TLoaderFileFormat]] = {
    "insert_values",
    "jsonl",
    "parquet",
    "arrow",
    "puae-jsonl",
}


def get_writer(
    _format: TLoaderFileFormat = "insert_values",
    buffer_max_items: int = 10,
    file_max_items: int = 5000,
    disable_compression: bool = False,
) -> BufferedDataWriter[DataWriter]:
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.preferred_loader_file_format = _format
    file_template = os.path.join(TEST_STORAGE_ROOT, f"{_format}.%s")
    return BufferedDataWriter(
        _format,
        file_template,
        buffer_max_items=buffer_max_items,
        file_max_items=file_max_items,
        disable_compression=disable_compression,
        _caps=caps,
    )
