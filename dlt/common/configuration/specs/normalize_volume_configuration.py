from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec


@configspec
class NormalizeVolumeConfiguration(BaseConfiguration):
    normalize_volume_path: str = None  # path to volume where normalized loader files will be stored
