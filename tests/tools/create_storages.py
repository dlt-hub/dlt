from dlt.common.storages import NormalizeStorage, LoadStorage, SchemaStorage
from dlt.common.configuration import NormalizeVolumeConfiguration, LoadVolumeConfiguration, SchemaVolumeConfiguration


NormalizeStorage(True, NormalizeVolumeConfiguration)
LoadStorage(True, LoadVolumeConfiguration, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
SchemaStorage(SchemaVolumeConfiguration, makedirs=True)
