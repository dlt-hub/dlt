import os
import pytest

from dlt.common.configuration.container import Container
from dlt.common.data_writers.writers import DataWriterMetrics
from dlt.common.destination.capabilities import TLoaderFileFormat, DestinationCapabilitiesContext
from dlt.common.schema.utils import new_column
from tests.common.data_writers.utils import ALL_WRITERS
from dlt.common.storages.data_item_storage import DataItemStorage

from tests.utils import TEST_STORAGE_ROOT


class TestItemStorage(DataItemStorage):
    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        return os.path.join(TEST_STORAGE_ROOT, f"{load_id}.{schema_name}.{table_name}.%s")


@pytest.mark.parametrize("format_", ALL_WRITERS - {"arrow"})
def test_write_items(format_: TLoaderFileFormat) -> None:
    with Container().injectable_context(
        DestinationCapabilitiesContext.generic_capabilities(format_)
    ):
        item_storage = TestItemStorage(format_)
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
