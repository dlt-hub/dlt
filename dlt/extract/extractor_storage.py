import semver

from dlt.common.json import json_typed_dumps
from dlt.common.typing import Any
from dlt.common.utils import uniq_id
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages import VersionedStorage, NormalizeStorage


class ExtractorStorageBase(VersionedStorage):
    def __init__(self, version: semver.VersionInfo, is_owner: bool, storage: FileStorage, normalize_storage: NormalizeStorage) -> None:
        self.normalize_storage = normalize_storage
        super().__init__(version, is_owner, storage)

    def create_temp_folder(self) -> str:
        tf_name = uniq_id()
        self.storage.create_folder(tf_name)
        return tf_name

    def save_json(self, name: str, d: Any) -> None:
        # saves json using typed encoder
        self.storage.save(name, json_typed_dumps(d))

    def commit_events(self, schema_name: str, processed_file_path: str, dest_file_stem: str, no_processed_events: int, load_id: str, with_delete: bool = True) -> str:
        raise NotImplementedError()
        # schema name cannot contain underscores
        # FileStorage.validate_file_name_component(schema_name)

        # dest_name = NormalizeStorage.build_extracted_file_stem(schema_name, dest_file_stem, no_processed_events, load_id)
        # # if no events extracted from tracker, file is not saved
        # if no_processed_events > 0:
        #     # moves file to possibly external storage and place in the dest folder atomically
        #     self.storage.copy_cross_storage_atomically(
        #         self.normalize_storage.storage.storage_path, NormalizeStorage.EXTRACTED_FOLDER, processed_file_path, dest_name)

        # if with_delete:
        #     self.storage.delete(processed_file_path)

        # return dest_name
