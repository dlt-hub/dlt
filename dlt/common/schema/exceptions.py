from typing import Any, List

from dlt.common.exceptions import DltException
from dlt.common.data_types import TDataType
from dlt.common.schema.typing import (
    TSchemaContractDict,
    TSchemaContractEntities,
    TSchemaEvolutionMode,
)
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TColumnSchema, TColumnSchemaBase


class SchemaException(DltException):
    def __init__(self, schema_name: str, msg: str) -> None:
        self.schema_name = schema_name
        if schema_name:
            msg = f"In schema `{schema_name}`: " + msg
        super().__init__(msg)


class InvalidSchemaName(ValueError, SchemaException):
    MAXIMUM_SCHEMA_NAME_LENGTH = 64

    def __init__(self, schema_name: str) -> None:
        self.name = schema_name
        super().__init__(
            schema_name,
            f"`{schema_name}` is an invalid schema/source name. The source or schema name must be a"
            " valid Python identifier ie. a snake case function name and have maximum"
            f" {self.MAXIMUM_SCHEMA_NAME_LENGTH} characters. Ideally should contain only small"
            " letters, numbers and underscores.",
        )


# TODO: does not look like a SchemaException
# class InvalidDatasetName(ValueError, SchemaException):
#     def __init__(self, destination_name: str) -> None:
#         self.destination_name = destination_name
#         super().__init__(
#             f"Destination {destination_name} does not accept empty datasets. Please pass the"
#             " dataset name to the destination configuration ie. via dlt pipeline."
#         )


class CannotCoerceColumnException(SchemaException):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        from_type: TDataType,
        to_type: TDataType,
        coerced_value: Any,
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.from_type = from_type
        self.to_type = to_type
        self.coerced_value = coerced_value
        super().__init__(
            schema_name,
            f"Cannot coerce type `{from_type}` to `{to_type}` for value `{coerced_value}` "
            f"in table `{table_name}` column `{column_name}`",
        )


class TablePropertiesConflictException(SchemaException):
    def __init__(self, schema_name: str, table_name: str, prop_name: str, val1: str, val2: str):
        self.table_name = table_name
        self.prop_name = prop_name
        self.val1 = val1
        self.val2 = val2
        super().__init__(
            schema_name,
            f"Cannot merge partial tables into table `{table_name}` due to property `{prop_name}`"
            f" with different values: `{val1} != {val2}`",
        )


class ParentTableNotFoundException(SchemaException):
    def __init__(
        self, schema_name: str, table_name: str, parent_table_name: str, explanation: str = ""
    ) -> None:
        self.table_name = table_name
        self.parent_table_name = parent_table_name
        super().__init__(
            schema_name,
            f"Parent table `{parent_table_name}` for `{table_name}` was not found in "
            f" `schema.{explanation}`",
        )


class CannotCoerceNullException(SchemaException):
    def __init__(self, schema_name: str, table_name: str, column_name: str) -> None:
        super().__init__(
            schema_name,
            f"Cannot coerce NULL in table `{table_name}` column `{column_name}` which is not"
            " nullable",
        )


class SchemaCorruptedException(SchemaException):
    pass


class SchemaIdentifierNormalizationCollision(SchemaCorruptedException):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        identifier_type: str,
        identifier_name: str,
        conflict_identifier_name: str,
        naming_name: str,
        collision_msg: str,
    ) -> None:
        if identifier_type == "column":
            table_info = f" in table `{table_name}`"
        else:
            table_info = ""
        msg = (
            f"A `{identifier_type}` name `{identifier_name}`{table_info} collides with"
            f" `{conflict_identifier_name}` after normalization with `{naming_name}` naming"
            " convention. "
            + collision_msg
        )
        self.table_name = table_name
        self.identifier_type = identifier_type
        self.identifier_name = identifier_name
        self.conflict_identifier_name = conflict_identifier_name
        self.naming_name = naming_name
        super().__init__(schema_name, msg)


class SchemaEngineNoUpgradePathException(SchemaException):
    def __init__(
        self, schema_name: str, init_engine: int, from_engine: int, to_engine: int
    ) -> None:
        self.init_engine = init_engine
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(
            schema_name,
            f"No engine upgrade path in schema `{schema_name}` from engine `{init_engine}` to"
            f" `{to_engine}`, stopped at `{from_engine}`. You possibly tried to run an older dlt"
            " version against a destination you have previously loaded data to with a newer dlt"
            " version.",
        )


class DataValidationError(SchemaException):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        schema_entity: TSchemaContractEntities,
        contract_mode: TSchemaEvolutionMode,
        table_schema: Any,
        schema_contract: TSchemaContractDict,
        data_item: Any = None,
        extended_info: str = None,
    ) -> None:
        """Raised when `data_item` violates `contract_mode` on a `schema_entity` as defined by `table_schema`

        Schema, table and column names are given as a context and full `schema_contract` and causing `data_item` as an evidence.
        """
        msg = f"Table: `{table_name}` "
        if column_name:
            msg += f"Column: `{column_name}`"
        msg = (
            "In "
            + msg
            + f" . Contract on `{schema_entity}` with `{contract_mode=:}` is violated. "
            + (extended_info or "")
        )
        data_item_str = ""
        if (
            data_item
            and hasattr(data_item, "get")
            and table_schema
            and hasattr(table_schema, "get")
        ):
            identifier_columns = [
                x.get("name")
                for x in table_schema.get("columns", {}).values()
                if x.get("primary_key") or x.get("merge_key") or x.get("unique")
            ]
            if identifier_columns:
                data_item_str += "Offending data item: "
                data_item_keys = [
                    f"{column}: {data_item.get(column)}" for column in identifier_columns
                ]
                data_item_str += ", ".join(data_item_keys)
                msg += f" {data_item_str}"
        super().__init__(schema_name, msg)
        self.table_name = table_name
        self.column_name = column_name

        # violated contract
        self.schema_entity = schema_entity
        self.contract_mode = contract_mode

        # some evidence
        self.table_schema = table_schema
        self.schema_contract = schema_contract
        self.data_item = data_item


class TableNotFound(KeyError, SchemaException):
    def __init__(self, schema_name: str, table_name: str) -> None:
        self.table_name = table_name
        super().__init__(schema_name, f"Table not found: `{table_name}`")


class TableIdentifiersFrozen(SchemaException):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        to_naming: NamingConvention,
        from_naming: NamingConvention,
        details: str,
    ) -> None:
        self.table_name = table_name
        self.to_naming = to_naming
        self.from_naming = from_naming
        msg = (
            f"Attempt to normalize identifiers for a table `{table_name}` from naming"
            f" `{from_naming.name()}` to `{to_naming.name()}` changed one or more identifiers. "
        )
        msg += (
            " This table already received data and tables were created at the destination. By"
            " default changing the identifiers is not allowed. "
        )
        msg += (
            " Such changes may result in creation of a new table or a new columns while the old"
            " columns with data will still be kept. "
        )
        msg += (
            " You may disable this behavior by setting"
            " `schema.allow_identifier_change_on_table_with_data` to True or removing"
            " `x-normalizer` hints from particular tables. "
        )
        msg += f" Details: {details}"
        super().__init__(schema_name, msg)


class ColumnNameConflictException(SchemaException):
    pass


class UnboundColumnException(SchemaException):
    def __init__(self, schema_name: str, table_name: str, columns: List[TColumnSchemaBase]) -> None:
        self.columns = columns
        self.schema_name = schema_name
        self.table_name = table_name

        col_infos: List[str] = []
        for column in columns:
            key_type: str = ""
            if column.get("merge_key"):
                key_type = "merge key"
            elif column.get("primary_key"):
                key_type = "primary key"

            line = f"  - {column['name']}"
            if key_type or not column.get("nullable", True):
                suffix = " (marked as non-nullable"
                if key_type:
                    suffix += f" {key_type}"
                suffix += " and must have values)"
                line += suffix
            col_infos.append(line)

        msg = (
            f"The following columns in table `{table_name}` did not receive any data during"
            " this load:\n"
            + "\n".join(col_info for col_info in col_infos)
        )
        msg += (
            "\n\nThis can happen if you specify columns manually, for example, using the"
            " `merge_key`, `primary_key` or `columns` argument but they do not exist in the data.\n"
        )
        super().__init__(schema_name, msg)


class UnboundColumnWithoutTypeException(SchemaException):
    def __init__(self, schema_name: str, table_name: str, columns: List[TColumnSchemaBase]) -> None:
        self.columns = columns
        self.schema_name = schema_name
        self.table_name = table_name

        column_names = [col["name"] for col in columns]

        msg = (
            f"The following columns in table '{table_name}' did not receive any data during this"
            " load and therefore could not have their types inferred:\n"
            + "\n".join(f"  - {name}" for name in column_names)
            + "\n\nUnless type hints are provided, these columns will not be materialized in the"
            " destination.\nOne way to provide type hints is to use the 'columns' argument in"
            " the '@dlt.resource' decorator.  For"
            f" example:\n\n@dlt.resource(columns={{{repr(column_names[0])}: {{'data_type':"
            " 'text'}})\n"
        )

        super().__init__(schema_name, msg)
