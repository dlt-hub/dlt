from typing import TYPE_CHECKING, Literal, Optional, get_args

from dlt.common.configuration.specs import BaseConfiguration, configspec

TSchemaFileFormat = Literal["json", "yaml"]
SchemaFileExtensions = get_args(TSchemaFileFormat)


@configspec(init=True)
class SchemaStorageConfiguration(BaseConfiguration):
    schema_volume_path: str = None  # path to volume with default schemas
    import_schema_path: Optional[str] = None  # import schema from external location
    export_schema_path: Optional[str] = None  # export schema to external location
    external_schema_format: TSchemaFileFormat = "yaml"  # format in which to expect external schema
    external_schema_format_remove_defaults: bool = True  # remove default values when exporting schema

    if TYPE_CHECKING:
        def __init__(self, schema_volume_path: str = None, import_schema_path: str = None, export_schema_path: str = None) -> None:
            ...


@configspec(init=True)
class NormalizeStorageConfiguration(BaseConfiguration):
    normalize_volume_path: str = None  # path to volume where normalized loader files will be stored

    if TYPE_CHECKING:
        def __init__(self, normalize_volume_path: str = None) -> None:
            ...


@configspec(init=True)
class LoadStorageConfiguration(BaseConfiguration):
    load_volume_path: str = None  # path to volume where files to be loaded to analytical storage are stored
    delete_completed_jobs: bool = False  # if set to true the folder with completed jobs will be deleted

    if TYPE_CHECKING:
        def __init__(self, load_volume_path: str = None, delete_completed_jobs: bool = None) -> None:
            ...
