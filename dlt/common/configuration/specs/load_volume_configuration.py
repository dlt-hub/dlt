from typing import TYPE_CHECKING

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec


@configspec(init=True)
class LoadVolumeConfiguration(BaseConfiguration):
    load_volume_path: str = None  # path to volume where files to be loaded to analytical storage are stored
    delete_completed_jobs: bool = False  # if set to true the folder with completed jobs will be deleted

    if TYPE_CHECKING:
        def __init__(self, load_volume_path: str = None, delete_completed_jobs: bool = None) -> None:
            ...
