from typing import Dict

from dlt.common.schema.schema import Schema
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.configuration.specs import SchemaVolumeConfiguration
from dlt.common.configuration.accessors import config


class LiveSchemaStorage(SchemaStorage):

    def __init__(self, config: SchemaVolumeConfiguration = config.value, makedirs: bool = False) -> None:
        self.live_schemas: Dict[str, Schema] = {}
        super().__init__(config, makedirs)

    def __getitem__(self, name: str) -> Schema:
        if name in self.live_schemas:
            schema = self.live_schemas[name]
        else:
            # return new schema instance
            schema = super().load_schema(name)
            self._update_live_schema(schema, True)

        return schema

    def load_schema(self, name: str) -> Schema:
        self.commit_live_schema(name)
        # now live schema is saved so we can load it with the changes
        return super().load_schema(name)

    def save_schema(self, schema: Schema) -> str:
        rv = super().save_schema(schema)
        # -- update the live schema with schema being saved but do not create live instance if not already present
        # no, cre
        self._update_live_schema(schema, True)
        return rv

    def initialize_import_schema(self, schema: Schema) -> None:
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

    def _update_live_schema(self, schema: Schema, can_create_new: bool) -> None:
        live_schema = self.live_schemas.get(schema.name)
        if live_schema:
            # replace content without replacing instance
            # print(f"live schema {live_schema} updated in place")
            live_schema.replace_schema_content(schema)
        elif can_create_new:
            # print(f"live schema {schema.name} created from schema")
            self.live_schemas[schema.name] = schema
