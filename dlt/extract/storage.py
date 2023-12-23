import os
from typing import Dict, List

from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.data_writers.writers import DataWriterMetrics
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.storages import (
    NormalizeStorageConfiguration,
    NormalizeStorage,
    DataItemStorage,
    FileStorage,
    PackageStorage,
    LoadPackageInfo,
)
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.typing import TDataItems
from dlt.common.time import precise_time
from dlt.common.utils import uniq_id


class ExtractorItemStorage(DataItemStorage):
    load_file_type: TLoaderFileFormat

    def __init__(self, package_storage: PackageStorage) -> None:
        """Data item storage using `storage` to manage load packages"""
        super().__init__(self.load_file_type)
        self.package_storage = package_storage

    def _get_data_item_path_template(self, load_id: str, _: str, table_name: str) -> str:
        file_name = PackageStorage.build_job_file_name(table_name, "%s")
        file_path = self.package_storage.get_job_file_path(
            load_id, PackageStorage.NEW_JOBS_FOLDER, file_name
        )
        return self.package_storage.storage.make_full_path(file_path)


class JsonLExtractorStorage(ExtractorItemStorage):
    load_file_type: TLoaderFileFormat = "puae-jsonl"


class ArrowExtractorStorage(ExtractorItemStorage):
    load_file_type: TLoaderFileFormat = "arrow"


class ExtractStorage(NormalizeStorage):
    """Wrapper around multiple extractor storages with different file formats"""

    def __init__(self, config: NormalizeStorageConfiguration) -> None:
        super().__init__(True, config)
        # always create new packages in an unique folder for each instance so
        # extracts are isolated ie. if they fail
        self.new_packages_folder = uniq_id(8)
        self.storage.create_folder(self.new_packages_folder, exists_ok=True)
        self.new_packages = PackageStorage(
            FileStorage(os.path.join(self.storage.storage_path, self.new_packages_folder)), "new"
        )
        self._item_storages: Dict[TLoaderFileFormat, ExtractorItemStorage] = {
            "puae-jsonl": JsonLExtractorStorage(self.new_packages),
            "arrow": ArrowExtractorStorage(self.new_packages),
        }

    def create_load_package(self, schema: Schema, reuse_exiting_package: bool = True) -> str:
        """Creates a new load package for given `schema` or returns if such package already exists.

        You can prevent reuse of the existing package by setting `reuse_exiting_package` to False
        """
        load_id: str = None
        if reuse_exiting_package:
            # look for existing package with the same schema name
            # TODO: we may cache this mapping but fallback to files is required if pipeline restarts
            load_ids = self.new_packages.list_packages()
            for load_id in load_ids:
                if self.new_packages.schema_name(load_id) == schema.name:
                    break
                load_id = None
        if not load_id:
            load_id = str(precise_time())
            self.new_packages.create_package(load_id)
        # always save schema
        self.new_packages.save_schema(load_id, schema)
        return load_id

    def get_storage(self, loader_file_format: TLoaderFileFormat) -> ExtractorItemStorage:
        return self._item_storages[loader_file_format]

    def close_writers(self, load_id: str) -> None:
        for storage in self._item_storages.values():
            storage.close_writers(load_id)

    def closed_files(self, load_id: str) -> List[DataWriterMetrics]:
        files = []
        for storage in self._item_storages.values():
            files.extend(storage.closed_files(load_id))
        return files

    def remove_closed_files(self, load_id: str) -> None:
        for storage in self._item_storages.values():
            storage.remove_closed_files(load_id)

    def commit_new_load_package(self, load_id: str, schema: Schema) -> None:
        self.new_packages.save_schema(load_id, schema)
        self.storage.rename_tree(
            os.path.join(self.new_packages_folder, self.new_packages.get_package_path(load_id)),
            os.path.join(
                NormalizeStorage.EXTRACTED_FOLDER, self.new_packages.get_package_path(load_id)
            ),
        )

    def delete_empty_extract_folder(self) -> None:
        """Deletes temporary extract folder if empty"""
        self.storage.delete_folder(self.new_packages_folder, recursively=False)

    def get_load_package_info(self, load_id: str) -> LoadPackageInfo:
        """Returns information on temp and extracted packages"""
        try:
            return self.new_packages.get_load_package_info(load_id)
        except LoadPackageNotFound:
            return self.extracted_packages.get_load_package_info(load_id)

    def write_data_item(
        self,
        file_format: TLoaderFileFormat,
        load_id: str,
        schema_name: str,
        table_name: str,
        item: TDataItems,
        columns: TTableSchemaColumns,
    ) -> None:
        self.get_storage(file_format).write_data_item(
            load_id, schema_name, table_name, item, columns
        )
