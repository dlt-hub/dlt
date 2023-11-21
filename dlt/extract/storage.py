import os
from typing import ClassVar, Dict

from dlt.common.data_writers import TLoaderFileFormat

from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItems
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.storages import NormalizeStorageConfiguration, NormalizeStorage, DataItemStorage, FileStorage


class ExtractorItemStorage(DataItemStorage):
    load_file_type: TLoaderFileFormat

    def __init__(self, storage: FileStorage, extract_folder: str="extract") -> None:
        # data item storage with jsonl with pua encoding
        super().__init__(self.load_file_type)
        self.extract_folder = extract_folder
        self.storage = storage


    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        template = NormalizeStorage.build_extracted_file_stem(schema_name, table_name, "%s")
        return self.storage.make_full_path(os.path.join(self._get_extract_path(load_id), template))

    def _get_extract_path(self, extract_id: str) -> str:
        return os.path.join(self.extract_folder, extract_id)


class JsonLExtractorStorage(ExtractorItemStorage):
    load_file_type: TLoaderFileFormat = "puae-jsonl"


class ArrowExtractorStorage(ExtractorItemStorage):
    load_file_type: TLoaderFileFormat = "arrow"


class ExtractorStorage(NormalizeStorage):
    EXTRACT_FOLDER: ClassVar[str] = "extract"

    """Wrapper around multiple extractor storages with different file formats"""
    def __init__(self, C: NormalizeStorageConfiguration) -> None:
        super().__init__(True, C)
        self._item_storages: Dict[TLoaderFileFormat, ExtractorItemStorage] = {
            "puae-jsonl": JsonLExtractorStorage(self.storage, extract_folder=self.EXTRACT_FOLDER),
            "arrow": ArrowExtractorStorage(self.storage, extract_folder=self.EXTRACT_FOLDER)
        }

    def _get_extract_path(self, extract_id: str) -> str:
        return os.path.join(self.EXTRACT_FOLDER, extract_id)

    def create_extract_id(self) -> str:
        extract_id = uniq_id()
        self.storage.create_folder(self._get_extract_path(extract_id))
        return extract_id

    def get_storage(self, loader_file_format: TLoaderFileFormat) -> ExtractorItemStorage:
        return self._item_storages[loader_file_format]

    def close_writers(self, extract_id: str) -> None:
        for storage in self._item_storages.values():
            storage.close_writers(extract_id)

    def commit_extract_files(self, extract_id: str, with_delete: bool = True) -> None:
        extract_path = self._get_extract_path(extract_id)
        for file in self.storage.list_folder_files(extract_path, to_root=False):
            from_file = os.path.join(extract_path, file)
            to_file = os.path.join(NormalizeStorage.EXTRACTED_FOLDER, file)
            if with_delete:
                self.storage.atomic_rename(from_file, to_file)
            else:
                # create hardlink which will act as a copy
                self.storage.link_hard(from_file, to_file)
        if with_delete:
            self.storage.delete_folder(extract_path, recursively=True)

    def write_data_item(self, file_format: TLoaderFileFormat, load_id: str, schema_name: str, table_name: str, item: TDataItems, columns: TTableSchemaColumns) -> None:
        self.get_storage(file_format).write_data_item(load_id, schema_name, table_name, item, columns)
