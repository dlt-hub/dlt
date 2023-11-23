from typing import Dict, List

from dlt.common.schema.schema import Schema
from dlt.common.configuration.accessors import config
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.storages.configuration import SchemaStorageConfiguration


class LiveSchemaStorage(SchemaStorage):
    def __init__(
        self, config: SchemaStorageConfiguration = config.value, makedirs: bool = False
    ) -> None:
        self.live_schemas: Dict[str, Schema] = {}
        super().__init__(config, makedirs)

    def __getitem__(self, name: str) -> Schema:
        if name in self.live_schemas:
            schema = self.live_schemas[name]
        else:
            # return new schema instance
            schema = super().load_schema(name)
            self.update_live_schema(schema)

        return schema

    def load_schema(self, name: str) -> Schema:
        self.commit_live_schema(name)
        # now live schema is saved so we can load it with the changes
        return super().load_schema(name)

    def save_schema(self, schema: Schema) -> str:
        rv = super().save_schema(schema)
        # update the live schema with schema being saved, if no live schema exist, create one to be available for a getter
        self.update_live_schema(schema)
        return rv

    def remove_schema(self, name: str) -> None:
        super().remove_schema(name)
        # also remove the live schema
        self.live_schemas.pop(name, None)

    def save_import_schema_if_not_exists(self, schema: Schema) -> None:
        if self.config.import_schema_path:
            try:
                self._load_import_schema(schema.name)
            except FileNotFoundError:
                # save import schema only if it not exist
                self._export_schema(schema, self.config.import_schema_path)

    def commit_live_schema(self, name: str) -> Schema:
        # if live schema exists and is modified then it must be used as an import schema
        live_schema = self.live_schemas.get(name)
        if live_schema and live_schema.stored_version_hash != live_schema.version_hash:
            live_schema.bump_version()
            self._save_schema(live_schema)
        return live_schema

    def update_live_schema(self, schema: Schema, can_create_new: bool = True) -> None:
        """Will update live schema content without writing to storage. Optionally allows to create a new live schema"""
        live_schema = self.live_schemas.get(schema.name)
        if live_schema:
            if id(live_schema) != id(schema):
                # replace content without replacing instance
                # print(f"live schema {live_schema} updated in place")
                live_schema.replace_schema_content(schema)
        elif can_create_new:
            # print(f"live schema {schema.name} created from schema")
            self.live_schemas[schema.name] = schema

    def list_schemas(self) -> List[str]:
        names = list(set(super().list_schemas()) | set(self.live_schemas.keys()))
        return names
