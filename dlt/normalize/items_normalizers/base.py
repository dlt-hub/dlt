from typing import List, Dict, Set
from abc import abstractmethod

from dlt.common.schema import TSchemaUpdate, Schema
from dlt.common.storages.load_storage import LoadStorage
from dlt.common.storages import NormalizeStorage
from dlt.common.storages.data_item_storage import DataItemStorage

from dlt.normalize.configuration import NormalizeConfiguration


class ItemsNormalizer:
    def __init__(
        self,
        item_storage: DataItemStorage,
        load_storage: LoadStorage,
        normalize_storage: NormalizeStorage,
        schema: Schema,
        load_id: str,
        config: NormalizeConfiguration,
    ) -> None:
        self.item_storage = item_storage
        self.load_storage = load_storage
        self.normalize_storage = normalize_storage
        self.schema = schema
        self.load_id = load_id
        self.config = config
        self.naming = self.schema.naming

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
