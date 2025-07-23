import os
from typing import Dict, List

from dlt.common.data_writers import TDataItemFormat, DataWriter, FileWriterSpec
from dlt.common.metrics import DataWriterMetrics
from dlt.common.schema import Schema
from dlt.common.storages import (
    NormalizeStorageConfiguration,
    NormalizeStorage,
    DataItemStorage,
    FileStorage,
    PackageStorage,
    LoadPackageInfo,
    create_load_id,
)
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.utils import uniq_id


class ExtractorItemStorage(DataItemStorage):
    def __init__(self, package_storage: PackageStorage, writer_spec: FileWriterSpec) -> None:
        """Data item storage using `storage` to manage load packages"""
        super().__init__(writer_spec)
        self.package_storage = package_storage

    def _get_data_item_path_template(self, load_id: str, _: str, table_name: str) -> str:
        file_name = PackageStorage.build_job_file_name(table_name, "%s")
        file_path = self.package_storage.get_job_file_path(
            load_id, PackageStorage.NEW_JOBS_FOLDER, file_name
        )
        return self.package_storage.storage.make_full_path(file_path)


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
        self.item_storages: Dict[TDataItemFormat, ExtractorItemStorage] = {
            "object": ExtractorItemStorage(
                self.new_packages, DataWriter.writer_spec_from_file_format("typed-jsonl", "object")
            ),
            "arrow": ExtractorItemStorage(
                self.new_packages, DataWriter.writer_spec_from_file_format("parquet", "arrow")
            ),
            "model": ExtractorItemStorage(
                self.new_packages, DataWriter.writer_spec_from_file_format("model", "model")
            ),
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
            load_id = create_load_id()
            self.new_packages.create_package(load_id)
        # always save schema
        self.new_packages.save_schema(load_id, schema)
        return load_id

    def close_writers(self, load_id: str, skip_flush: bool = False) -> None:
        for storage in self.item_storages.values():
            storage.close_writers(load_id, skip_flush=skip_flush)

    def closed_files(self, load_id: str) -> List[DataWriterMetrics]:
        files = []
        for storage in self.item_storages.values():
            files.extend(storage.closed_files(load_id))
        return files

    def remove_closed_files(self, load_id: str) -> None:
        for storage in self.item_storages.values():
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
