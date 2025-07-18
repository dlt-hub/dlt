import os
from typing import Type, Optional

from dlt.common.data_writers.buffered import BufferedDataWriter
from dlt.common.data_writers.writers import TWriter, ALL_WRITERS
from dlt.common.destination import DestinationCapabilitiesContext

from tests.utils import TEST_STORAGE_ROOT

ALL_OBJECT_WRITERS = [
    writer for writer in ALL_WRITERS if writer.writer_spec().data_item_format == "object"
]
ALL_ARROW_WRITERS = [
    writer for writer in ALL_WRITERS if writer.writer_spec().data_item_format == "arrow"
]


def get_writer(
    writer: Type[TWriter],
    buffer_max_items: int = 10,
    file_max_items: Optional[int] = 10,
    file_max_bytes: Optional[int] = None,
    disable_compression: bool = False,
    caps: DestinationCapabilitiesContext = None,
) -> BufferedDataWriter[TWriter]:
    caps = caps or DestinationCapabilitiesContext.generic_capabilities()
    writer_spec = writer.writer_spec()
    caps.preferred_loader_file_format = writer_spec.file_format
    file_template = os.path.join(TEST_STORAGE_ROOT, f"{writer_spec.file_format}.%s")
    return BufferedDataWriter(
        writer_spec,
        file_template,
        buffer_max_items=buffer_max_items,
        file_max_items=file_max_items,
        file_max_bytes=file_max_bytes,
        disable_compression=disable_compression,
        _caps=caps,
    )
