from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.runners.configuration import PoolRunnerConfiguration, TPoolType
from dlt.common.storages import (
    LoadStorageConfiguration,
    NormalizeStorageConfiguration,
    SchemaStorageConfiguration,
)


@configspec
class ItemsNormalizerConfiguration(BaseConfiguration):
    add_dlt_id: bool = False
    """When true, items to be normalized will have `_dlt_id` column added with an ID for each row:
    random or a hash of the primary key or of the row, depending on the merge strategy."""
    add_dlt_load_id: bool = False
    """When true, items to be normalized will have `_dlt_load_id` column added with the current load ID."""


@configspec
class NormalizeConfiguration(PoolRunnerConfiguration):
    pool_type: TPoolType = "process"
    destination_capabilities: DestinationCapabilitiesContext = None  # injectable
    _schema_storage_config: SchemaStorageConfiguration = None
    _normalize_storage_config: NormalizeStorageConfiguration = None
    _load_storage_config: LoadStorageConfiguration = None

    json_normalizer: ItemsNormalizerConfiguration = ItemsNormalizerConfiguration(
        add_dlt_id=True, add_dlt_load_id=True
    )

    parquet_normalizer: ItemsNormalizerConfiguration = ItemsNormalizerConfiguration(
        add_dlt_id=False, add_dlt_load_id=False
    )

    model_normalizer: ItemsNormalizerConfiguration = ItemsNormalizerConfiguration(
        add_dlt_id=False, add_dlt_load_id=True
    )

    def on_resolved(self) -> None:
        self.pool_type = "none" if self.workers == 1 else "process"
