from typing import Any

from dlt.common.exceptions import DltException
from dlt.common.data_types import TDataType


class SchemaException(DltException):
    pass


class InvalidSchemaName(ValueError, SchemaException):
    MAXIMUM_SCHEMA_NAME_LENGTH = 64

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"{name} is an invalid schema/source name. The source or schema name must be a valid Python identifier ie. a snake case function name and have maximum {self.MAXIMUM_SCHEMA_NAME_LENGTH} characters. Ideally should contain only small letters, numbers and underscores.")


# class InvalidDatasetName(ValueError, SchemaException):
#     def __init__(self, name: str, normalized_name: str) -> None:
#         self.name = name
#         super().__init__(f"{name} is an invalid dataset name. The dataset name must conform to wide range of destinations and ideally should contain only small letters, numbers and underscores. Try {normalized_name} instead as suggested by current naming module.")

class InvalidDatasetName(ValueError, SchemaException):
    def __init__(self, destination_name: str) -> None:
        self.destination_name = destination_name
        super().__init__(f"Destination {destination_name} does not accept empty datasets. Please pass the dataset name to the destination configuration ie. via dlt pipeline.")


class CannotCoerceColumnException(SchemaException):
    def __init__(self, table_name: str, column_name: str, from_type: TDataType, to_type: TDataType, coerced_value: Any) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.from_type = from_type
        self.to_type = to_type
        self.coerced_value = coerced_value
        super().__init__(f"Cannot coerce type in table {table_name} column {column_name} existing type {from_type} coerced type {to_type} value: {coerced_value}")


class TablePropertiesConflictException(SchemaException):
    def __init__(self, table_name: str, prop_name: str, val1: str, val2: str):
        self.table_name = table_name
        self.prop_name = prop_name
        self.val1 = val1
        self.val2 = val2
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
