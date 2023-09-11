from typing import TYPE_CHECKING

from dlt.common.configuration import configspec
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.runners.configuration import PoolRunnerConfiguration, TPoolType
from dlt.common.storages import LoadStorageConfiguration, NormalizeStorageConfiguration, SchemaStorageConfiguration


@configspec
class NormalizeConfiguration(PoolRunnerConfiguration):
    pool_type: TPoolType = "process"
    destination_capabilities: DestinationCapabilitiesContext = None  # injectable
    _schema_storage_config: SchemaStorageConfiguration
    _normalize_storage_config: NormalizeStorageConfiguration
    _load_storage_config: LoadStorageConfiguration

    def on_resolved(self) -> None:
        self.pool_type = "none" if self.workers == 1 else "process"

    if TYPE_CHECKING:
        def __init__(
            self,
            pool_type: TPoolType = "process",
            workers: int = None,
            _schema_storage_config: SchemaStorageConfiguration = None,
            _normalize_storage_config: NormalizeStorageConfiguration = None,
            _load_storage_config: LoadStorageConfiguration = None
        ) -> None:
            ...
