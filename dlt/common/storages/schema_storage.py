import os

from dlt.common import json
from dlt.common.file_storage import FileStorage
from dlt.common.schema import Schema
from dlt.common.typing import DictStrAny


class SchemaStorage:

    FOLDER_SCHEMA_FILE = "schema.json"
    STORE_SCHEMA_FILE_PATTERN = f"%s_{FOLDER_SCHEMA_FILE}"

    def __init__(self, schema_storage_root: str, makedirs: bool = False) -> None:
        self.storage = FileStorage(schema_storage_root, makedirs=makedirs)

    def load_store_schema(self, name: str) -> Schema:
        # loads a schema from a store holding many schemas
        schema_file = self._file_name_in_store(name)
        stored_schema: DictStrAny = json.loads(self.storage.load(schema_file))
        return Schema.from_dict(stored_schema)

    def load_folder_schema(self, from_folder: str) -> Schema:
        # loads schema from a folder containing one default schema
        schema_path = self._file_name_in_folder(from_folder)
        stored_schema: DictStrAny = json.loads(self.storage.load(schema_path))
        return Schema.from_dict(stored_schema)

    def save_store_schema(self, schema: Schema) -> str:
        # save a schema to schema store
        dump = json.dumps(schema.to_dict(), indent=2)
        schema_file = self._file_name_in_store(schema.schema_name)
        return self.storage.save(schema_file, dump)

    def remove_store_schema(self, name: str) -> None:
        schema_file = self._file_name_in_store(name)
        self.storage.delete(schema_file)

    def save_folder_schema(self, schema: Schema, in_folder: str) -> str:
        # save a schema to a folder holding one schema
        dump = json.dumps(schema.to_dict())
        schema_file = self._file_name_in_folder(in_folder)
        return self.storage.save(schema_file, dump)

    def has_store_schema(self, name: str) -> bool:
        schema_file = self._file_name_in_store(name)
        return self.storage.has_file(schema_file)

    def _file_name_in_store(self, name: str) -> str:
        if name:
            return SchemaStorage.STORE_SCHEMA_FILE_PATTERN % name
        else:
            return SchemaStorage.FOLDER_SCHEMA_FILE

    def _file_name_in_folder(self, folder: str) -> str:
        return os.path.join(folder, SchemaStorage.FOLDER_SCHEMA_FILE)   # if folder is None else os.path.join(folder, SchemaStorage.SCHEMA_FILE)
