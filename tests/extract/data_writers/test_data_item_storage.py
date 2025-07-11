import os
from typing import Type
import pytest

from dlt.common.configuration.container import Container
from dlt.common.data_writers.writers import DataWriter
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.metrics import DataWriterMetrics
from dlt.common.schema.utils import new_column
from dlt.common.storages.data_item_storage import DataItemStorage

from tests.utils import TEST_STORAGE_ROOT
from tests.common.data_writers.utils import ALL_OBJECT_WRITERS


class ItemTestStorage(DataItemStorage):
    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        return os.path.join(TEST_STORAGE_ROOT, f"{load_id}.{schema_name}.{table_name}.%s")


@pytest.mark.parametrize("writer_type", ALL_OBJECT_WRITERS)
def test_write_items(writer_type: Type[DataWriter]) -> None:
    writer_spec = writer_type.writer_spec()
    with Container().injectable_context(
        DestinationCapabilitiesContext.generic_capabilities(writer_spec.file_format)
    ):
        item_storage = ItemTestStorage(writer_spec)
        c1 = new_column("col1", "bigint")
        t1 = {"col1": c1}
        count = item_storage.write_data_item(
            "load_1", "schema", "t1", [{"col1": 182812}, {"col1": -1}], t1
        )
        assert count == 2
        assert item_storage.closed_files("load_1") == []
        # write empty file
        metrics = item_storage.write_empty_items_file("load_2", "schema", "t1", t1)
        assert item_storage.closed_files("load_2")[0] == metrics
        # force closing file by writing empty file
        metrics = item_storage.import_items_file(
            "load_1",
            "schema",
            "t1",
            "tests/extract/cases/imported.any",
            DataWriterMetrics("", 1, 231, 0, 0),
        )
        assert item_storage.closed_files("load_1")[1] == metrics

        # closed files are separate
        item_storage.write_data_item("load_1", "schema", "t1", [{"col1": 182812}, {"col1": -1}], t1)
        item_storage.write_data_item("load_2", "schema", "t1", [{"col1": 182812}, {"col1": -1}], t1)
        item_storage.close_writers("load_1")
        assert len(item_storage.closed_files("load_1")) == 3
        assert len(item_storage.closed_files("load_2")) == 1
        item_storage.close_writers("load_2")
        assert len(item_storage.closed_files("load_2")) == 2
