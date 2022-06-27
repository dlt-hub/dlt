from typing import Any

from dlt.common.exceptions import DltException
from dlt.common.schema.typing import TDataType


class SchemaException(DltException):
    pass


class InvalidSchemaName(SchemaException):
    def __init__(self, name: str, normalized_name: str) -> None:
        self.name = name
        super().__init__(f"{name} is invalid schema name. Only lowercase letters are allowed. Try {normalized_name} instead")


class CannotCoerceColumnException(SchemaException):
    def __init__(self, table_name: str, column_name: str, from_type: TDataType, to_type: TDataType, value: Any) -> None:
        super().__init__(f"Cannot coerce type in table {table_name} column {column_name} existing type {from_type} coerced type {to_type} value: {value}")


class TablePropertiesClashException(SchemaException):
    def __init__(self, table_name: str, prop_name: str, val1: str, val2: str):
        super().__init__(f"Cannot merge partial tables for {table_name} due to property {prop_name}: {val1} != {val2}")


class ParentTableNotFoundException(SchemaException):
    def __init__(self, table_name: str, parent_table_name: str, explanation: str = "") -> None:
        self.table_name = table_name
        self.parent_table_name = parent_table_name
        super().__init__(f"Parent table {parent_table_name} for {table_name} was not found in the schema.{explanation}")


class CannotCoerceNullException(SchemaException):
    def __init__(self, table_name: str, column_name: str) -> None:
        super().__init__(f"Cannot coerce NULL in table {table_name} column {column_name} which is not nullable")


class SchemaCorruptedException(SchemaException):
    pass


class SchemaEngineNoUpgradePathException(SchemaException):
    def __init__(self, schema_name: str, init_engine: int, from_engine: int, to_engine: int) -> None:
        self.schema_name = schema_name
        self.init_engine = init_engine
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(f"No engine upgrade path in schema {schema_name} from {init_engine} to {to_engine}, stopped at {from_engine}")
