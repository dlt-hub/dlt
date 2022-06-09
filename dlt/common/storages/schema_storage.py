import os
from typing import Optional

from dlt.common import json
from dlt.common.file_storage import FileStorage
from dlt.common.schema import Schema, StoredSchema


class SchemaStorage:

    STORE_SCHEMA_FILE_PATTERN = "%s_schema.json"
    FOLDER_SCHEMA_FILE = "schema.json"

    def __init__(self, schema_storage_root: str, makedirs: bool = False) -> None:
        self.storage = FileStorage(schema_storage_root, makedirs=makedirs)

    def load_store_schema(self, name: str) -> Schema:
        # loads a schema from a store holding many schemas
        schema_file = self._get_file_by_name(name)
        stored_schema: StoredSchema = json.loads(self.storage.load(schema_file))
        return Schema.from_dict(stored_schema)

    def load_folder_schema(self, from_folder: str) -> Schema:
        # loads schema from a folder containing one default schema
        schema_path = self._get_file_in_folder(from_folder)
        stored_schema: StoredSchema = json.loads(self.storage.load(schema_path))
        return Schema.from_dict(stored_schema)

    def save_store_schema(self, schema: Schema) -> str:
        # save a schema to schema store
        dump = json.dumps(schema.to_dict(), indent=2)
        schema_file = self._get_file_by_name(schema.schema_name)
        return self.storage.save(schema_file, dump)

    def remove_store_schema(self, name: str) -> None:
        schema_file = self._get_file_by_name(name)
        self.storage.delete(schema_file)

    def save_folder_schema(self, schema: Schema, in_folder: str) -> str:
        # save a schema to a folder holding one schema
        dump = json.dumps(schema.to_dict())
        schema_file = self._get_file_in_folder(in_folder)
        return self.storage.save(schema_file, dump)

    def has_store_schema(self, name: str) -> bool:
        schema_file = self._get_file_by_name(name)
        return self.storage.has_file(schema_file)

    def _get_file_by_name(self, name: str) -> str:
        return SchemaStorage.STORE_SCHEMA_FILE_PATTERN % name

    def _get_file_in_folder(self, folder: str) -> str:
        return os.path.join(folder, SchemaStorage.FOLDER_SCHEMA_FILE)   # if folder is None else os.path.join(folder, SchemaStorage.SCHEMA_FILE)
