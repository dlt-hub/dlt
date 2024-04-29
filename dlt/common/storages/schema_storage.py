import yaml
from typing import Iterator, List, Mapping, Tuple, cast

from dlt.common import logger
from dlt.common.json import json
from dlt.common.configuration import with_config
from dlt.common.configuration.accessors import config
from dlt.common.schema.utils import to_pretty_json, to_pretty_yaml
from dlt.common.storages.configuration import (
    SchemaStorageConfiguration,
    TSchemaFileFormat,
    SchemaFileExtensions,
)
from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema import Schema, verify_schema_hash
from dlt.common.typing import DictStrAny

from dlt.common.storages.exceptions import (
    InStorageSchemaModified,
    SchemaNotFoundError,
    UnexpectedSchemaName,
)


class SchemaStorage(Mapping[str, Schema]):
    SCHEMA_FILE_NAME = "schema.%s"
    NAMED_SCHEMA_FILE_PATTERN = f"%s.{SCHEMA_FILE_NAME}"

    @with_config(spec=SchemaStorageConfiguration, sections=("schema",))
    def __init__(
        self, config: SchemaStorageConfiguration = config.value, makedirs: bool = False
    ) -> None:
        self.config = config
        self.storage = FileStorage(config.schema_volume_path, makedirs=makedirs)

    def _load_schema_json(self, name: str) -> DictStrAny:
        schema_file = self._file_name_in_store(name, "json")
        return cast(DictStrAny, json.loads(self.storage.load(schema_file)))

    def load_schema(self, name: str) -> Schema:
        # loads a schema from a store holding many schemas
        storage_schema: DictStrAny = None
        try:
            storage_schema = self._load_schema_json(name)
            # prevent external modifications of schemas kept in storage
            if not verify_schema_hash(storage_schema, verifies_if_not_migrated=True):
                raise InStorageSchemaModified(name, self.config.schema_volume_path)
        except FileNotFoundError:
            # maybe we can import from external storage
            pass

        # try to import from external storage
        if self.config.import_schema_path:
            return self._maybe_import_schema(name, storage_schema)
        if storage_schema is None:
            raise SchemaNotFoundError(name, self.config.schema_volume_path)
        return Schema.from_dict(storage_schema)

    def save_schema(self, schema: Schema) -> str:
        # check if there's schema to import
        if self.config.import_schema_path:
            try:
                imported_schema = Schema.from_dict(self._load_import_schema(schema.name))
                # link schema being saved to current imported schema so it will not overwrite this save when loaded
                schema._imported_version_hash = imported_schema.stored_version_hash
            except FileNotFoundError:
                # just save the schema
                pass
        path = self._save_schema(schema)
        if self.config.export_schema_path:
            self._export_schema(schema, self.config.export_schema_path)
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
        return [f.split(".")[0] for f in files]

    def clear_storage(self) -> None:
        for schema_name in self.list_schemas():
            self.remove_schema(schema_name)

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
                # store import hash to self to track changes
                rv_schema._imported_version_hash = rv_schema.version_hash
                logger.info(
                    f"Schema {name} not present in {self.storage.storage_path} and got imported"
                    f" with version {rv_schema.stored_version} and imported hash"
                    f" {rv_schema._imported_version_hash}"
                )
                # if schema was imported, overwrite storage schema
                self._save_schema(rv_schema)
                if self.config.export_schema_path:
                    self._export_schema(rv_schema, self.config.export_schema_path)
            else:
                # import schema when imported schema was modified from the last import
                rv_schema = Schema.from_dict(storage_schema)
                i_s = Schema.from_dict(imported_schema)
                if i_s.version_hash != rv_schema._imported_version_hash:
                    rv_schema.replace_schema_content(i_s, link_to_replaced_schema=True)
                    rv_schema._imported_version_hash = i_s.version_hash
                    logger.info(
                        f"Schema {name} was present in {self.storage.storage_path} but is"
                        f" overwritten with imported schema version {i_s.version} and"
                        f" imported hash {i_s.version_hash}"
                    )
                    # if schema was imported, overwrite storage schema
                    self._save_schema(rv_schema)
                    if self.config.export_schema_path:
                        self._export_schema(rv_schema, self.config.export_schema_path)
        except FileNotFoundError:
            # no schema to import -> skip silently and return the original
            if storage_schema is None:
                raise SchemaNotFoundError(
                    name,
                    self.config.schema_volume_path,
                    self.config.import_schema_path,
                    self.config.external_schema_format,
                )
            rv_schema = Schema.from_dict(storage_schema)

        assert rv_schema is not None
        return rv_schema

    def _load_import_schema(self, name: str) -> DictStrAny:
        import_storage = FileStorage(self.config.import_schema_path, makedirs=False)
        schema_file = self._file_name_in_store(name, self.config.external_schema_format)
        return self._parse_schema_str(
            import_storage.load(schema_file), self.config.external_schema_format
        )

    def _export_schema(self, schema: Schema, export_path: str) -> None:
        stored_schema = schema.to_dict(remove_defaults=True)
        if self.config.external_schema_format == "json":
            exported_schema_s = to_pretty_json(stored_schema)
        elif self.config.external_schema_format == "yaml":
            exported_schema_s = to_pretty_yaml(stored_schema)
        else:
            raise ValueError(self.config.external_schema_format)

        export_storage = FileStorage(export_path, makedirs=True)
        schema_file = self._file_name_in_store(schema.name, self.config.external_schema_format)
        export_storage.save(schema_file, exported_schema_s)
        logger.info(
            f"Schema {schema.name} exported to {export_path} with version"
            f" {stored_schema['version']}:{stored_schema['version_hash']} as"
            f" {self.config.external_schema_format}"
        )

    def _save_schema(self, schema: Schema) -> str:
        # save a schema to schema store
        schema_file = self._file_name_in_store(schema.name, "json")
        stored_schema = schema.to_dict()
        saved_path = self.storage.save(schema_file, to_pretty_json(stored_schema))
        # this should be the only place where this function is called. we bump a version and
        # clean modified status
        schema._bump_version()
        return saved_path

    @staticmethod
    def load_schema_file(
        path: str, name: str, extensions: Tuple[TSchemaFileFormat, ...] = SchemaFileExtensions
    ) -> Schema:
        storage = FileStorage(path)
        for extension in extensions:
            file = SchemaStorage._file_name_in_store(name, extension)
            if storage.has_file(file):
                parsed_schema = SchemaStorage._parse_schema_str(storage.load(file), extension)
                schema = Schema.from_dict(parsed_schema)
                if schema.name != name:
                    raise UnexpectedSchemaName(name, path, schema.name)
                return schema
        raise SchemaNotFoundError(name, path)

    @staticmethod
    def _parse_schema_str(schema_str: str, extension: TSchemaFileFormat) -> DictStrAny:
        if extension == "json":
            imported_schema: DictStrAny = json.loads(schema_str)
        elif extension == "yaml":
            imported_schema = yaml.safe_load(schema_str)
        else:
            raise ValueError(extension)
        return imported_schema

    @staticmethod
    def _file_name_in_store(name: str, fmt: TSchemaFileFormat) -> str:
        if name:
            return SchemaStorage.NAMED_SCHEMA_FILE_PATTERN % (name, fmt)
        else:
            return SchemaStorage.SCHEMA_FILE_NAME % fmt
