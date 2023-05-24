from typing import ClassVar, Sequence, NamedTuple
from itertools import groupby
from pathlib import Path

from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.configuration import NormalizeStorageConfiguration
from dlt.common.storages.versioned_storage import VersionedStorage

class TParsedNormalizeFileName(NamedTuple):
    schema_name: str
    table_name: str
    file_id: str


class NormalizeStorage(VersionedStorage):

    STORAGE_VERSION: ClassVar[str] = "1.0.0"
    EXTRACTED_FOLDER: ClassVar[str] = "extracted"  # folder within the volume where extracted files to be normalized are stored

    @with_config(spec=NormalizeStorageConfiguration, sections=(known_sections.NORMALIZE,))
    def __init__(self, is_owner: bool, config: NormalizeStorageConfiguration = config.value) -> None:
        super().__init__(NormalizeStorage.STORAGE_VERSION, is_owner, FileStorage(config.normalize_volume_path, "t", makedirs=is_owner))
        self.config = config
        if is_owner:
            self.initialize_storage()

    def initialize_storage(self) -> None:
        self.storage.create_folder(NormalizeStorage.EXTRACTED_FOLDER, exists_ok=True)

    def list_files_to_normalize_sorted(self) -> Sequence[str]:
        return sorted(self.storage.list_folder_files(NormalizeStorage.EXTRACTED_FOLDER))

    def group_by_schema(self, files: Sequence[str]) -> "groupby[str, str]":
        return groupby(files, NormalizeStorage.get_schema_name)

    @staticmethod
    def get_schema_name(file_name: str) -> str:
        return NormalizeStorage.parse_normalize_file_name(file_name).schema_name

    @staticmethod
    def build_extracted_file_stem(schema_name: str, table_name: str, file_id: str) -> str:
        # builds file name with the extracted data to be passed to normalize
        return f"{schema_name}.{table_name}.{file_id}"

    @staticmethod
    def parse_normalize_file_name(file_name: str) -> TParsedNormalizeFileName:
        # parse extracted file name and returns (events found, load id, schema_name)
        if not file_name.endswith("jsonl"):
            raise ValueError(file_name)

        parts = Path(file_name).stem.split(".")
        if len(parts) != 3:
            raise ValueError(file_name)
        return TParsedNormalizeFileName(*parts)
