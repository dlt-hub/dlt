from typing import List

from dlt.common import logger
from dlt.common.metrics import DataWriterMetrics
from dlt.common.schema import TSchemaUpdate
from dlt.common.storages.load_package import ParsedLoadJobFileName

from dlt.normalize.items_normalizers.base import ItemsNormalizer


class FileImportNormalizer(ItemsNormalizer):
    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]:
        self._maybe_cancel()
        logger.info(
            f"Table {root_table_name} {self.item_storage.writer_spec.file_format} file"
            f" {extracted_items_file} will be directly imported without normalization"
        )
        completed_columns = self.schema.get_table_columns(root_table_name)
        if not completed_columns:
            logger.warning(
                f"Table {root_table_name} has no completed columns for imported file"
                f" {extracted_items_file} and will not be created! Pass column hints to the"
                " resource or with dlt.mark.with_hints or create the destination table yourself."
            )
        with self.normalize_storage.extracted_packages.storage.open_file(
            extracted_items_file, "rb"
        ) as f:
            # TODO: sniff the schema depending on a file type
            file_metrics = DataWriterMetrics(extracted_items_file, 0, f.tell(), 0, 0)
        parts = ParsedLoadJobFileName.parse(extracted_items_file)
        self.item_storage.import_items_file(
            self.load_id,
            self.schema.name,
            parts.table_name,
            self.normalize_storage.extracted_packages.storage.make_full_path(extracted_items_file),
            file_metrics,
        )
        return []
