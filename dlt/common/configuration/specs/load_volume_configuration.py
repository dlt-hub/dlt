from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec


@configspec
class LoadVolumeConfiguration(BaseConfiguration):
    load_volume_path: str = None  # path to volume where files to be loaded to analytical storage are stored
    delete_completed_jobs: bool = False  # if set to true the folder with completed jobs will be deleted
