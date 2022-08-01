from dlt.common.storages.normalize_storage import NormalizeStorage
from dlt.common.configuration import NormalizeVolumeConfiguration
from dlt.common.storages.load_storage import LoadStorage
from dlt.common.configuration import LoadVolumeConfiguration
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.configuration import SchemaVolumeConfiguration


NormalizeStorage(True, NormalizeVolumeConfiguration)
LoadStorage(True, LoadVolumeConfiguration, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
SchemaStorage(SchemaVolumeConfiguration.SCHEMA_VOLUME_PATH, makedirs=True)
