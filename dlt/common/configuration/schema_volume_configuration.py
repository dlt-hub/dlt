from typing import Optional, Literal

from dlt.common.configuration import BaseConfiguration

TSchemaFileFormat = Literal["json", "yaml"]


class SchemaVolumeConfiguration(BaseConfiguration):
    SCHEMA_VOLUME_PATH: str = "_storage/schemas"  # path to volume with default schemas
    IMPORT_SCHEMA_PATH: Optional[str] = None  # import schema from external location
    EXPORT_SCHEMA_PATH: Optional[str] = None  # export schema to external location
    EXTERNAL_SCHEMA_FORMAT: TSchemaFileFormat = "yaml"  # format in which to expect external schema
    EXTERNAL_SCHEMA_FORMAT_REMOVE_DEFAULTS: bool = True  # remove default values when exporting schema


class ProductionSchemaVolumeConfiguration(SchemaVolumeConfiguration):
    SCHEMA_VOLUME_PATH: str = None
