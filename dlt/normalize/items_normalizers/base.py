from typing import List, Dict, Optional, Set

from abc import abstractmethod

from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.storages.load_storage import LoadStorage
from dlt.common.storages import NormalizeStorage
from dlt.common.storages.data_item_storage import DataItemStorage

from dlt.normalize.configuration import NormalizeConfiguration

PROGRESS_ITEMS_FLUSH_THRESHOLD = 1000
"""Flush progress when collected number of items exceeds or equals this value"""


class ItemsNormalizer:
    def __init__(
        self,
        item_storage: DataItemStorage,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
        report_progress: bool = False,
        collector: Collector = NULL_COLLECTOR,
    ) -> None:
        self.item_storage = item_storage
        self.load_storage = load_storage
        self.normalize_storage = normalize_storage
        self.schema = schema
        self.load_id = load_id
        self.config = config
        self.naming = self.schema.naming
        self._report_to_file = report_progress
        self._collector = collector
        self._pending_progress: Dict[str, int] = {}
        self._total_pending: int = 0

    def _report_progress(self, table_name: str, items_count: int) -> None:
        """Reports progress for processed items, either directly via collector or buffered to file."""
        if self._collector is not NULL_COLLECTOR:
            self._collector.update(table_name, items_count)
            return
        if self._report_to_file:
            self._pending_progress[table_name] = (
                self._pending_progress.get(table_name, 0) + items_count
            )
            self._total_pending += items_count
            if self._total_pending >= PROGRESS_ITEMS_FLUSH_THRESHOLD:
                self._flush_progress()

    def _flush_progress(self) -> None:
        """Writes buffered progress to the package progress directory."""
        if not self._report_to_file or not self._pending_progress:
            return
        self.load_storage.new_packages.write_progress(self.load_id, self._pending_progress)
        self._pending_progress.clear()
        self._total_pending = 0

    def _maybe_cancel(self) -> None:
        self.load_storage.new_packages.raise_if_cancelled(self.load_id)

    @property
    def null_only_columns(self) -> Dict[str, Set[str]]:
        return {}

    @abstractmethod
    def __call__(self, extracted_items_file: str, root_table_name: str) -> List[TSchemaUpdate]: ...

    def close(self) -> None:
        """Called when normalizer finishes processing. Override to clean up resources."""
        pass
