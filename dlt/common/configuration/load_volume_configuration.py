import os

from dlt.common.configuration.run_configuration import BaseConfiguration


class LoadVolumeConfiguration(BaseConfiguration):
    LOAD_VOLUME_PATH: str = os.path.join("_storage", "load")  # path to volume where files to be loaded to analytical storage are stored
    DELETE_COMPLETED_JOBS: bool = False  # if set to true the folder with completed jobs will be deleted

class ProductionLoadVolumeConfiguration(LoadVolumeConfiguration):
    LOAD_VOLUME_PATH: str = None
