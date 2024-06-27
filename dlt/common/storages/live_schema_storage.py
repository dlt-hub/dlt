from typing import Dict, List, cast

from dlt.common.schema.schema import Schema
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.storages.configuration import SchemaStorageConfiguration


class LiveSchemaStorage(SchemaStorage):
    def __init__(self, config: SchemaStorageConfiguration, makedirs: bool = False) -> None:
        self.live_schemas: Dict[str, Schema] = {}
        super().__init__(config, makedirs)

    def __getitem__(self, name: str) -> Schema:
        if name in self.live_schemas:
            schema = self.live_schemas[name]
            if not self.is_live_schema_committed(name):
                return schema
        # return new schema instance
        schema = self.load_schema(name)
        schema = self.set_live_schema(schema)
        return schema

    def save_schema(self, schema: Schema) -> str:
        # update the live schema with schema being saved, if no live schema exist, create one to be available for a getter
        schema = self.set_live_schema(schema)
        rv = super().save_schema(schema)
        return rv

    def remove_schema(self, name: str) -> None:
        super().remove_schema(name)
        # also remove the live schema
        self.live_schemas.pop(name, None)

    def commit_live_schema(self, name: str) -> str:
        """Saves live schema in storage if it was modified"""
        if not self.is_live_schema_committed(name):
            live_schema = self.live_schemas[name]
            return self._save_schema(live_schema)
        # not saved
        return None

    def is_live_schema_committed(self, name: str) -> bool:
        """Checks if live schema is present in storage and have same hash"""
        live_schema = self.live_schemas.get(name)
        if live_schema is None:
            raise SchemaNotFoundError(name, f"live-schema://{name}")
        return not live_schema.is_modified

    def set_live_schema(self, schema: Schema) -> Schema:
        """Will add or update live schema content without writing to storage."""
        live_schema = self.live_schemas.get(schema.name)
        if live_schema:
            if id(live_schema) != id(schema):
                # replace content without replacing instance
                # print(f"live schema {live_schema} updated in place")
                live_schema.replace_schema_content(schema, link_to_replaced_schema=True)
        else:
            # print(f"live schema {schema.name} created from schema")
            live_schema = self.live_schemas[schema.name] = schema
        return live_schema

    def list_schemas(self) -> List[str]:
        names = list(set(super().list_schemas()) | set(self.live_schemas.keys()))
        return names
