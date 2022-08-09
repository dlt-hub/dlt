import os
import re
import yaml
from typing import Iterator, List, Type, Mapping

from dlt.common import json, logger
from dlt.common.configuration.schema_volume_configuration import TSchemaFileFormat
from dlt.common.file_storage import FileStorage
from dlt.common.schema import Schema, verify_schema_hash
from dlt.common.schema.typing import TStoredSchema
from dlt.common.typing import DictStrAny
from dlt.common.configuration import SchemaVolumeConfiguration

from dlt.common.storages.exceptions import InStorageSchemaModified, SchemaNotFoundError


class SchemaStorage(Mapping[str, Schema]):

    SCHEMA_FILE_NAME = "schema.%s"
    NAMED_SCHEMA_FILE_PATTERN = f"%s_{SCHEMA_FILE_NAME}"

    def __init__(self, C: Type[SchemaVolumeConfiguration], makedirs: bool = False) -> None:
        self.C = C
        self.storage = FileStorage(C.SCHEMA_VOLUME_PATH, makedirs=makedirs)

    def load_schema(self, name: str) -> Schema:
        # loads a schema from a store holding many schemas
        schema_file = self._file_name_in_store(name, "json")
        storage_schema: DictStrAny = None
        try:
            storage_schema = json.loads(self.storage.load(schema_file))
            # prevent external modifications of schemas kept in storage
            if not verify_schema_hash(storage_schema, empty_hash_verifies=True):
                raise InStorageSchemaModified(name, self.C.SCHEMA_VOLUME_PATH)
        except FileNotFoundError:
            # maybe we can import from external storage
            pass

        # try to import from external storage
        if self.C.IMPORT_SCHEMA_PATH:
            return self._maybe_import_schema(name, storage_schema)
        if storage_schema is None:
            raise SchemaNotFoundError(name, self.C.SCHEMA_VOLUME_PATH)
        return Schema.from_dict(storage_schema)

    def save_schema(self, schema: Schema) -> str:
        # check if there's schema to import
        if self.C.IMPORT_SCHEMA_PATH:
            try:
                imported_schema = Schema.from_dict(self._load_import_schema(schema.name))
                # link schema being saved to current imported schema so it will not overwrite this save when loaded
                schema._imported_version_hash = imported_schema.stored_version_hash
            except FileNotFoundError:
                # just save the schema
                pass
        path = self._save_schema(schema)
        if self.C.EXPORT_SCHEMA_PATH:
            self._export_schema(schema, self.C.EXPORT_SCHEMA_PATH)
        return path

    def remove_schema(self, name: str) -> None:
        schema_file = self._file_name_in_store(name, "json")
        self.storage.delete(schema_file)

    def has_schema(self, name: str) -> bool:
        schema_file = self._file_name_in_store(name, "json")
        return self.storage.has_file(schema_file)

    def list_schemas(self) -> List[str]:
        files = self.storage.list_folder_files(".", to_root=False)
        # extract names
        return [re.split("_|schema", f)[0] for f in files]

    def __getitem__(self, name: str) -> Schema:
        return self.load_schema(name)

    def __len__(self) -> int:
        return len(self.list_schemas())

    def __iter__(self) -> Iterator[str]:
        for name in self.list_schemas():
            yield name

    def __contains__(self, name: str) -> bool:  # type: ignore
        return name in self.list_schemas()

    def _maybe_import_schema(self, name: str, storage_schema: DictStrAny = None) -> Schema:
        rv_schema: Schema = None
        try:
            imported_schema = self._load_import_schema(name)
            if storage_schema is None:
                # import schema when no schema in storage
                rv_schema = Schema.from_dict(imported_schema)
                # if schema was imported, overwrite storage schema
                rv_schema._imported_version_hash = rv_schema.version_hash
                self._save_schema(rv_schema)
                logger.info(f"Schema {name} not present in {self.storage.storage_path} and got imported with version {rv_schema.stored_version} and imported hash {rv_schema._imported_version_hash}")
            else:
                # import schema when imported schema was modified from the last import
                sc = Schema.from_dict(storage_schema)
                rv_schema = Schema.from_dict(imported_schema)
                if rv_schema.version_hash != sc._imported_version_hash:
                    # use imported schema but version must be bumped and imported hash set
                    rv_schema._stored_version = sc.stored_version + 1
                    rv_schema._imported_version_hash = rv_schema.version_hash
                    # if schema was imported, overwrite storage schema
                    self._save_schema(rv_schema)
                    logger.info(f"Schema {name} was present in {self.storage.storage_path} but is overwritten with imported schema version {rv_schema.stored_version} and imported hash {rv_schema._imported_version_hash}")
                else:
                    # use storage schema as nothing changed
                    rv_schema = sc
        except FileNotFoundError:
            # no schema to import -> skip silently and return the original
            if storage_schema is None:
                raise SchemaNotFoundError(name, self.C.SCHEMA_VOLUME_PATH, self.C.IMPORT_SCHEMA_PATH, self.C.EXTERNAL_SCHEMA_FORMAT)
            rv_schema = Schema.from_dict(storage_schema)

        assert rv_schema is not None
        return rv_schema

    def _load_import_schema(self, name: str) -> DictStrAny:
        import_storage = FileStorage(self.C.IMPORT_SCHEMA_PATH, makedirs=False)
        schema_file = self._file_name_in_store(name, self.C.EXTERNAL_SCHEMA_FORMAT)
        imported_schema: DictStrAny = None
        imported_schema_s = import_storage.load(schema_file)
        if self.C.EXTERNAL_SCHEMA_FORMAT == "json":
            imported_schema = json.loads(imported_schema_s)
        elif self.C.EXTERNAL_SCHEMA_FORMAT == "yaml":
            imported_schema = yaml.safe_load(imported_schema_s)
        else:
            raise ValueError(self.C.EXTERNAL_SCHEMA_FORMAT)
        return imported_schema

    def _export_schema(self, schema: Schema, export_path: str) -> None:
        if self.C.EXTERNAL_SCHEMA_FORMAT == "json":
            exported_schema_s = schema.to_pretty_json(remove_defaults=self.C.EXTERNAL_SCHEMA_FORMAT_REMOVE_DEFAULTS)
        elif self.C.EXTERNAL_SCHEMA_FORMAT == "yaml":
            exported_schema_s = schema.to_pretty_yaml(remove_defaults=self.C.EXTERNAL_SCHEMA_FORMAT_REMOVE_DEFAULTS)
        else:
            raise ValueError(self.C.EXTERNAL_SCHEMA_FORMAT)

        export_storage = FileStorage(export_path, makedirs=True)
        schema_file = self._file_name_in_store(schema.name, self.C.EXTERNAL_SCHEMA_FORMAT)
        export_storage.save(schema_file, exported_schema_s)
        logger.info(f"Schema {schema.name} exported to {export_path} with version {schema.stored_version} as {self.C.EXTERNAL_SCHEMA_FORMAT}")

    def _save_schema(self, schema: Schema) -> str:
        # save a schema to schema store
        schema_file = self._file_name_in_store(schema.name, "json")
        return self.storage.save(schema_file, schema.to_pretty_json(remove_defaults=False))

    def _file_name_in_store(self, name: str, fmt: TSchemaFileFormat) -> str:
        if name:
            return SchemaStorage.NAMED_SCHEMA_FILE_PATTERN % (name, fmt)
        else:
            return SchemaStorage.SCHEMA_FILE_NAME % fmt
