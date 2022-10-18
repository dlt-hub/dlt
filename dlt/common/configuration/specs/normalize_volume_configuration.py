from typing import TYPE_CHECKING

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec


@configspec(init=True)
class NormalizeVolumeConfiguration(BaseConfiguration):
    normalize_volume_path: str = None  # path to volume where normalized loader files will be stored

    if TYPE_CHECKING:
        def __init__(self, normalize_volume_path: str = None) -> None:
            ...
