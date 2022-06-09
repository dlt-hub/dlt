import base64
import binascii
import yaml
import datetime
import re
from re import Pattern
from copy import deepcopy
from dateutil.parser import isoparse
from typing import Dict, List, Set, Mapping, Optional, Sequence, Tuple, Type, TypedDict, Literal, Any, cast

from dlt.common import pendulum, json, Decimal
from dlt.common.names import normalize_schema_name
from dlt.common.typing import DictStrAny, StrAny, StrStr
from dlt.common.arithmetics import ConversionSyntax
from dlt.common.exceptions import DltException

DataType = Literal["text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei"]
HintType = Literal["not_null", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]
ColumnProp = Literal["name", "data_type", "nullable", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]

DATA_TYPES: Set[DataType] = set(["text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei"])
COLUMN_PROPS: Set[ColumnProp] = set(["name", "data_type", "nullable", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"])
COLUMN_HINTS: Set[HintType] = set(["partition", "cluster", "primary_key", "foreign_key", "sort", "unique"])

class ColumnBase(TypedDict, total=True):
    name: str
    data_type: DataType
    nullable: bool

class Column(ColumnBase, total=True):
    partition: bool
    cluster: bool
    unique: bool
    sort: bool
    primary_key: bool
    foreign_key: bool

Table = Dict[str, Column]
SchemaTables = Dict[str, Table]
SchemaUpdate = Dict[str, List[Column]]


class StoredSchema(TypedDict, total=True):
    version: int
    engine_version: int
    name: str
    tables: SchemaTables
    preferred_types: Mapping[str, DataType]
    hints: Mapping[HintType, Sequence[str]]
    excludes: Sequence[str]
    includes: Sequence[str]


class Schema:

    VERSION_TABLE_NAME = "_version"
    VERSION_COLUMN_NAME = "version"
    LOADS_TABLE_NAME = "_loads"
    ENGINE_VERSION = 2

    def __init__(self, name: str) -> None:
        # verify schema name
        if name != normalize_schema_name(name):
            raise InvalidSchemaName(name)
        self._schema_tables: SchemaTables = {}
        self._schema_name: str = name
        self._version = 1
        # list of preferred types: map regex on columns into types
        self._preferred_types: Mapping[str, DataType] = {}
        # compiled regexes
        self._compiled_preferred_types: List[Tuple[Pattern[str], DataType]] = []
        # table hints
        self._hints: Mapping[HintType, Sequence[str]] = {}
        self._compiled_hints: Dict[HintType, Sequence[Pattern[str]]] = {}
        # excluded paths
        self._excludes: Sequence[str] = []
        self._compiled_excludes: Sequence[Pattern[str]] = []
        # included paths
        self._includes: Sequence[str] = []
        self._compiled_includes: Sequence[Pattern[str]] = []
        # add version table
        self._add_standard_tables()
        # add standard hints
        self._add_standard_hints()
        # compile hints
        self._compile_regexes()

    @classmethod
    def from_dict(cls, stored_schema: StoredSchema) -> "Schema":
        # upgrade engine if needed
        cls._upgrade_engine_version(stored_schema, stored_schema["engine_version"], cls.ENGINE_VERSION)
        # create new instance from dict
        self: Schema = cls(stored_schema["name"])
        self._schema_tables = stored_schema["tables"]
        # TODO: generate difference if STANDARD SCHEMAS are different than those and increase schema version
        if Schema.VERSION_TABLE_NAME not in self._schema_tables:
            raise SchemaCorruptedException(f"Schema must contain table {Schema.VERSION_TABLE_NAME}")
        if Schema.LOADS_TABLE_NAME not in self._schema_tables:
            raise SchemaCorruptedException(f"Schema must contain table {Schema.LOADS_TABLE_NAME}")
        # verify table schemas
        for table_name, table in self._schema_tables.items():
            for column_name in table:
                # add default hints to tables
                column = self._add_missing_hints(table[column_name])
                # overwrite column name
                column["name"] = column_name
                # verify column
                self._verify_column(table_name, column_name, column)
                table[column_name] = column
        self._version = stored_schema["version"]
        self._preferred_types = stored_schema["preferred_types"]
        self._hints = stored_schema["hints"]
        self._excludes = stored_schema["excludes"]
        self._includes = stored_schema["includes"]
        # compile regexes
        self._compile_regexes()

        return self

    def filter_row(self, table_name: str, row: StrAny, path_separator: str) -> StrAny:
        # include and exclude paths follow the naming convention of the unpacker and correspond to json document nesting
        # current version of the unpacker separates json elements with __

        def _exclude(path: str) -> bool:
            is_included = False
            is_excluded = any(exclude.search(path) for exclude in self._compiled_excludes)
            if is_excluded:
                # we may have exception if explicitely included
                is_included = any(include.search(path) for include in self._compiled_includes)
            return is_excluded and not is_included

        # check if any of the rows is excluded
        for field_name in list(row.keys()):
            path = f"{table_name}{path_separator}{field_name}"
            # excluded if any rule matches
            if _exclude(path):
                # TODO: copy to new instance
                del row[field_name]  # type: ignore
        return row

    def coerce_row(self, table_name: str, row: StrAny) -> Tuple[StrAny, List[Column]]:
        table_schema: Table = self._schema_tables.get(table_name, {})
        new_columns: List[Column] = []
        new_row: DictStrAny = {}
        for col_name, v in row.items():
            # skip None values, we should infer the types later
            if v is None:
                # just check if column is nullable if exists
                self._coerce_null_value(table_schema, table_name, col_name)
            else:
                new_col_name, new_col_def, new_v = self._coerce_non_null_value(table_schema, table_name, col_name, v)
                new_row[new_col_name] = new_v
                if new_col_def:
                    new_columns.append(new_col_def)

        return new_row, new_columns

    def filter_hints_in_row(self, table_name: str, hint_type: HintType, row: StrAny) -> StrAny:
        rv_row: DictStrAny = {}
        column_prop: ColumnProp = self._hint_to_column_prop(hint_type)
        try:
            table = self.get_table(table_name)
            for column_name in table:
                if column_name in row:
                    hint_value = table[column_name][column_prop]
                    if (hint_value and column_prop != "nullable") or (column_prop == "nullable" and not hint_value):
                        rv_row[column_name] = row[column_name]
        except KeyError:
            for k, v in row.items():
                if self._infer_hint(hint_type, v, k):
                    rv_row[k] = v

        # dicts are ordered and we will return the rows with hints in the same order as they appear in the columns
        return rv_row

    def update_schema(self, table_name: str, updated_columns: List[Column]) -> None:
        # all tables in the schema must start with the schema name
        # if not table_name.startswith(f"{self._schema_name}"):
        #     raise InvalidTableNameException(self._schema_name, table_name)

        if table_name not in self._schema_tables:
            # add the whole new table to SchemaTables
            self._schema_tables[table_name] = {c["name"]: c for c in updated_columns}
        else:
            # add several columns to existing table
            table_schema = self._schema_tables[table_name]
            for column in updated_columns:
                column_name = column["name"]
                if column_name in table_schema:
                    # we do not support changing existing columns
                    if not Schema._compare_columns(table_schema[column_name], column):
                        # attempt to update to incompatible columns
                        raise CannotCoerceColumnException(table_name, column_name, table_schema[column_name]["data_type"], column["data_type"], None)
                else:
                    table_schema[column_name] = column
        # bump schema version
        self._version += 1

    def get_schema_update_for(self, table_name: str, t: Table) -> List[Column]:
        # gets new columns to be added to "t" to bring up to date with stored schema
        diff_c: List[Column] = []
        s_t = self.get_table(table_name)
        for c in s_t.values():
            if c["name"] not in t:
                diff_c.append(c)
        return diff_c

    def get_table(self, table_name: str) -> Table:
        return self._schema_tables[table_name]

    def to_dict(self) -> StoredSchema:
        return  {
            "tables": self._schema_tables,
            "name": self._schema_name,
            "version": self._version,
            "preferred_types": self._preferred_types,
            "hints": self._hints,
            "excludes": self._excludes,
            "includes": self._includes,
            "engine_version": Schema.ENGINE_VERSION
        }

    @property
    def schema_version(self) -> int:
        return self._version

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def schema_tables(self) -> SchemaTables:
        return self._schema_tables

    def as_yaml(self, remove_default_hints: bool = False) -> str:
        d = self.to_dict()
        clean_tables = deepcopy(d["tables"])

        for t in clean_tables.values():
            for c in t.values():
                # do not save names
                del c["name"]  # type: ignore
                # remove hints with default values
                if remove_default_hints:
                    for h in list(c.keys()):
                        if type(c[h]) is bool and c[h] is False and h != "nullable":  # type: ignore
                            del c[h]  # type: ignore

        d["tables"] = clean_tables

        return cast(str, yaml.dump(d, allow_unicode=True, default_flow_style=False, sort_keys=False))

    def _infer_column(self, k: str, v: Any) -> Column:
        return Column(
            name=k,
            data_type=self._map_value_to_column_type(v, k),
            nullable=not self._infer_hint("not_null", v, k),
            partition=self._infer_hint("partition", v, k),
            cluster=self._infer_hint("cluster", v, k),
            sort=self._infer_hint("sort", v, k),
            unique=self._infer_hint("unique", v, k),
            primary_key=self._infer_hint("primary_key", v, k),
            foreign_key=self._infer_hint("foreign_key", v, k)
        )

    def _coerce_null_value(self, table_schema: Table, table_name: str, col_name: str) -> None:
        if col_name in table_schema:
            existing_column = table_schema[col_name]
            if not existing_column["nullable"]:
                raise CannotCoerceNullException(table_name, col_name)

    def _coerce_non_null_value(self, table_schema: Table, table_name: str, col_name: str, v: Any) -> Tuple[str, Column, Any]:
        new_column: Column = None
        rv = v
        variant_col_name = col_name

        if col_name in table_schema:
            existing_column = table_schema[col_name]
            # existing columns cannot be changed so we must update row
            py_data_type = Schema._py_type_to_sc_type(type(v))
            if existing_column["data_type"] != py_data_type:
                # first try to coerce existing value into destination type
                try:
                    rv = Schema._coerce_type(existing_column["data_type"], py_data_type, v)
                except (ValueError, SyntaxError):
                    # for complex types we must coerce to text
                    if py_data_type == "complex":
                        py_data_type = "text"
                        rv = Schema._coerce_type("text", "complex", v)
                    # if that does not work we must create variant extension to the table
                    variant_col_name = f"{col_name}_v_{py_data_type}"
                    # if variant exists check type, coercions are not required
                    if variant_col_name in table_schema:
                        if table_schema[variant_col_name]["data_type"] != py_data_type:
                            raise CannotCoerceColumnException(table_name, variant_col_name, table_schema[variant_col_name]["data_type"], py_data_type, v)
                    else:
                        # new column
                        # add new column
                        new_column = self._infer_column(variant_col_name, v)
                        # must have variant type, not preferred or coerced type
                        new_column["data_type"] = py_data_type
            else:
                # just copy row: types match
                pass
        else:
            # infer new column
            new_column = self._infer_column(col_name, v)
            # and coerce type if inference changed the python type
            py_type = Schema._py_type_to_sc_type(type(v))
            rv = Schema._coerce_type(new_column["data_type"], py_type, v)

        return variant_col_name, new_column, rv

    def _map_value_to_column_type(self, v: Any, k: str) -> DataType:
        mapped_type = Schema._py_type_to_sc_type(type(v))
         # if complex type was detected we must coerce to string
        if mapped_type == "complex":
            mapped_type = "text"
        # get preferred type based on column name
        preferred_type = self._get_preferred_type(k)
        # try to match python type to preferred
        if preferred_type:
            # try to coerce to destination type
            try:
                Schema._coerce_type(preferred_type, mapped_type, v)
                # coercion possible so preferred type may be used
                mapped_type = preferred_type
            except ValueError:
                # coercion not possible
                pass
        print(f"{v} {k} -> {mapped_type}")
        return mapped_type

    def _get_preferred_type(self, col_name: str) -> Optional[DataType]:
        return next((m[1] for m in self._compiled_preferred_types if m[0].search(col_name)), None)

    def _infer_hint(self, hint_type: HintType, _: Any, k: str) -> bool:
        if hint_type in self._compiled_hints:
            return any(h.search(k) for h in self._compiled_hints[hint_type])
        else:
            return False

    def _add_standard_tables(self) -> None:
        version_table: Table = {
            "version": self._add_missing_hints({
                "name": "version",
                "data_type": "bigint",
                "nullable": False,
            }),
            "engine_version": self._add_missing_hints({
                "name": "engine_version",
                "data_type": "bigint",
                "nullable": False
            }),
            "inserted_at": self._add_missing_hints({
                "name": "inserted_at",
                "data_type": "timestamp",
                "nullable": False
            })

        }
        self._schema_tables[Schema.VERSION_TABLE_NAME] = version_table
        load_table: Table = {
            "load_id": self._add_missing_hints({
                "name": "load_id",
                "data_type": "text",
                "nullable": False
            }),
            "status": self._add_missing_hints({
                "name": "status",
                "data_type": "bigint",
                "nullable": False
            }),
            "inserted_at": self._add_missing_hints({
                "name": "inserted_at",
                "data_type": "timestamp",
                "nullable": False
            })
        }
        self._schema_tables[Schema.LOADS_TABLE_NAME] = load_table

    def _add_standard_hints(self) -> None:
        self._hints = {
            "not_null": ["^_record_hash$", "^_root_hash$", "^_parent_hash$", "^_pos$", "_load_id"],
            "foreign_key": ["^_parent_hash$"],
            "unique": ["^_record_hash$"]
        }

    def _compile_regexes(self) -> None:
        for pattern, dt in self._preferred_types.items():
            # add tuples to be searched in coercions
            self._compiled_preferred_types.append((re.compile(pattern), dt))
        for hint_name, hint_list in self._hints.items():
            # compile hints which are column matching regexes
            self._compiled_hints[hint_name] = list(map(lambda hint: re.compile(hint), hint_list))
        self._compiled_excludes = list(map(lambda exclude: re.compile(exclude), self._excludes))
        self._compiled_includes = list(map(lambda include: re.compile(include), self._includes))

    @staticmethod
    def _verify_column(table_name: str, column_name: str, column: Column) -> None:
        existing_props = set(column.keys())
        missing_props = COLUMN_PROPS.difference(existing_props)
        if len(missing_props) > 0:
            raise SchemaCorruptedException(f"In table {table_name} column {column_name}: Column definition is missing following properties {missing_props}")
        data_type = column["data_type"]
        if data_type not in DATA_TYPES:
            raise SchemaCorruptedException(f"In table {table_name} column {column_name}: {data_type} is not one of available types: {DATA_TYPES}")
        for p, v in column.items():
            if p in COLUMN_HINTS and not type(v) is bool:
                 raise SchemaCorruptedException(f"In table {table_name} column {column_name}: hint {p} is not boolean.")

    @staticmethod
    def _upgrade_engine_version(schema_dict: StoredSchema, from_engine: int, to_engine: int) -> None:
        if from_engine == 1:
            schema_dict["engine_version"] = 2
            schema_dict["includes"] = []
            schema_dict["excludes"] = []
            from_engine = 2
        if from_engine == 2:
            pass
        if from_engine != to_engine:
            raise SchemaEngineNoUpgradePathException(schema_dict["name"], schema_dict["engine_version"], from_engine, to_engine)

    @staticmethod
    def _add_missing_hints(column: ColumnBase) -> Column:
        return {
            **{  # type:ignore
                "partition": False,
                "cluster": False,
                "unique": False,
                "sort": False,
                "primary_key": False,
                "foreign_key": False,
            },
            **column
        }


    @staticmethod
    def _py_type_to_sc_type(t: Type[Any]) -> DataType:
        if t is float:
            return "double"
        elif t is int:
            return "bigint"
        elif t is bool:
            return "bool"
        elif t is bytes:
            return "binary"
        elif t in [dict, list]:
            return "complex"
        elif issubclass(t, Decimal):
            return "decimal"
        elif issubclass(t, datetime.datetime):
            return "timestamp"
        else:
            return "text"

    @staticmethod
    def _coerce_type(to_type: DataType, from_type: DataType, value: Any) -> Any:
        if to_type == from_type:
            return value

        if to_type == "text":
            if from_type == "complex":
                return json.dumps(value)
            else:
                return str(value)

        if to_type == "binary":
            if from_type == "text":
                if value.startswith("0x"):
                    return bytes.fromhex(value[2:])
                try:
                    return base64.b64decode(value, validate=True)
                except binascii.Error:
                    raise ValueError(value)
            if from_type == "bigint":
                return value.to_bytes((value.bit_length() + 7) // 8, 'little')

        if to_type in ["wei", "bigint"]:
            if from_type == "bigint":
                return value
            if from_type in ["decimal", "double"]:
                if value % 1 != 0:
                    # only integer decimals and floats can be coerced
                    raise ValueError(value)
                return int(value)
            if from_type == "text":
                trim_value = value.strip()
                if trim_value.startswith("0x"):
                    return int(trim_value[2:], 16)
                else:
                    return int(trim_value)

        if to_type == "double":
             if from_type in ["bigint", "wei", "decimal"]:
                return float(value)
             if from_type == "text":
                trim_value = value.strip()
                if trim_value.startswith("0x"):
                    return float(int(trim_value[2:], 16))
                else:
                    return float(trim_value)

        if to_type == "decimal":
            if from_type in ["bigint", "wei"]:
                return value
            if from_type == "double":
                return Decimal(value)
            if from_type == "text":
                trim_value = value.strip()
                if trim_value.startswith("0x"):
                    return int(trim_value[2:], 16)
                elif "." not in trim_value and "e" not in trim_value:
                    return int(trim_value)
                else:
                    try:
                        return Decimal(trim_value)
                    except ConversionSyntax:
                        raise ValueError(trim_value)

        if to_type == "timestamp":
            if from_type in ["bigint", "double"]:
                # returns ISO datetime with timezone
                return str(pendulum.from_timestamp(value))

            if from_type == "text":
                # if parses as ISO date then pass it
                try:
                    isoparse(value)
                    return value
                except ValueError:
                    # try to convert string to integer, or float
                    try:
                        value = int(value)
                    except ValueError:
                        # raises ValueError if not parsing correctly
                        value = float(value)
                    return str(pendulum.from_timestamp(value))

        raise ValueError(value)

    @staticmethod
    def _compare_columns(a: Column, b: Column) -> bool:
        return a["data_type"] == b["data_type"] and a["nullable"] == b["nullable"]

    @staticmethod
    def _hint_to_column_prop(h: HintType) -> ColumnProp:
        if h == "not_null":
            return "nullable"
        return h

class SchemaException(DltException):
    pass


class InvalidSchemaName(SchemaException):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"{name} is invalid schema name. Only lowercase letters are allowed. Try {normalize_schema_name(name)} instead")


class CannotCoerceColumnException(SchemaException):
    def __init__(self, table_name: str, column_name: str, from_type: DataType, to_type: DataType, value: Any) -> None:
        super().__init__(f"Cannot coerce type in table {table_name} column {column_name} existing type {from_type} coerced type {to_type} value: {value}")


class CannotCoerceNullException(SchemaException):
    def __init__(self, table_name: str, column_name: str) -> None:
        super().__init__(f"Cannot coerce NULL in table {table_name} column {column_name} which is not nullable")


class InvalidTableNameException(SchemaException):
    def __init__(self, schema_name: str, table_name: str) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        super().__init__(f"All table names must start with '{schema_name}' so {table_name} is invalid")

class SchemaCorruptedException(SchemaException):
    pass


class SchemaEngineNoUpgradePathException(SchemaException):
    def __init__(self, schema_name: str, init_engine: int, from_engine: int, to_engine: int) -> None:
        self.schema_name = schema_name
        self.init_engine = init_engine
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(f"No engine upgrade path in schema {schema_name} from {init_engine} to {to_engine}, stopped at {from_engine}")
