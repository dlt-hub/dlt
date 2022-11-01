from typing import TYPE_CHECKING

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import LoadVolumeConfiguration, NormalizeVolumeConfiguration, SchemaVolumeConfiguration, PoolRunnerConfiguration, TPoolType
from dlt.common.destination import DestinationCapabilitiesContext


@configspec(init=True)
class NormalizeConfiguration(PoolRunnerConfiguration):
    pool_type: TPoolType = "process"
    destination_capabilities: DestinationCapabilitiesContext = None  # injectable
    _schema_storage_config: SchemaVolumeConfiguration
    _normalize_storage_config: NormalizeVolumeConfiguration
    _load_storage_config: LoadVolumeConfiguration

    if TYPE_CHECKING:
        def __init__(
            self,
            pool_type: TPoolType = None,
            workers: int = None,
            exit_on_exception: bool = None,
            is_single_run: bool = None,
            _schema_storage_config: SchemaVolumeConfiguration = None,
            _normalize_storage_config: NormalizeVolumeConfiguration = None,
            _load_storage_config: LoadVolumeConfiguration = None
        ) -> None:
            ...
