from dlt.common.storages.unpacker_storage import UnpackerStorage
from dlt.common.configuration import UnpackingVolumeConfiguration
from dlt.common.storages.loader_storage import LoaderStorage
from dlt.common.configuration import LoadingVolumeConfiguration
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.configuration import SchemaVolumeConfiguration


UnpackerStorage(True, UnpackingVolumeConfiguration)
LoaderStorage(True, LoadingVolumeConfiguration, "jsonl")
SchemaStorage(SchemaVolumeConfiguration.SCHEMA_VOLUME_PATH, makedirs=True)
