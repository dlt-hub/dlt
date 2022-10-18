from dlt.common.configuration import configspec
from dlt.common.configuration.specs import LoadVolumeConfiguration, NormalizeVolumeConfiguration, SchemaVolumeConfiguration, PoolRunnerConfiguration, DestinationCapabilitiesContext, TPoolType


@configspec(init=True)
class NormalizeConfiguration(PoolRunnerConfiguration):
    pool_type: TPoolType = "process"
    destination_capabilities: DestinationCapabilitiesContext = None  # injectable
    schema_storage_config: SchemaVolumeConfiguration
    normalize_storage_config: NormalizeVolumeConfiguration
    load_storage_config: LoadVolumeConfiguration
