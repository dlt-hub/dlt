from importlib import import_module
import yaml
import re
from re import Pattern
from copy import deepcopy
from typing import Dict, List, Set, Mapping, Optional, Sequence, Tuple, Any, cast

from dlt.common.typing import DictStrAny, StrAny, TEvent
from dlt.common.normalizers.names import TNormalizeNameFunc
from dlt.common.normalizers.json import TNormalizeJSONFunc
from dlt.common.schema.typing import NormalizersConfig, StoredSchema, SchemaTables, Table, Column, ColumnProp, DataType, HintType
from dlt.common.schema import utils
from dlt.common.schema.exceptions import CannotCoerceColumnException, CannotCoerceNullException, InvalidSchemaName, SchemaCorruptedException

class Schema:

    VERSION_TABLE_NAME = "_version"
    VERSION_COLUMN_NAME = "version"
    LOADS_TABLE_NAME = "_loads"
    ENGINE_VERSION = 3

    def __init__(self, name: str, normalizers: NormalizersConfig = None) -> None:
        # verify schema name
        if name != utils.normalize_schema_name(name):
            raise InvalidSchemaName(name, utils.normalize_schema_name(name))
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
        # normalizers config
        self._normalizers_config: NormalizersConfig = normalizers
        # name normalization functions
        self.normalize_table_name: TNormalizeNameFunc = None
        self.normalize_column_name: TNormalizeNameFunc = None
        # json normalization function
        self.json_normalize: TNormalizeJSONFunc = None
        # add version table
        self._add_standard_tables()
        # add standard hints
        self._add_standard_hints()
        # configure normalizers, including custom config if present
        self._configure_normalizers()
        # compile hints
        self._compile_regexes()

    @classmethod
    def from_dict(cls, stored_schema: StoredSchema) -> "Schema":
        # upgrade engine if needed
        utils.upgrade_engine_version(stored_schema, stored_schema["engine_version"], cls.ENGINE_VERSION)
        # create new instance from dict
        self: Schema = cls(stored_schema["name"], normalizers=stored_schema.get("normalizers", None))
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
                column = utils.add_missing_hints(table[column_name])
                # overwrite column name
                column["name"] = column_name
                # verify column
                utils.verify_column(table_name, column_name, column)
                table[column_name] = column
        self._version = stored_schema["version"]
        self._preferred_types = stored_schema["preferred_types"]
        self._hints = stored_schema["hints"]
        self._excludes = stored_schema.get("excludes", [])
        self._includes = stored_schema.get("includes", [])
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
                # we may have exception if explicitly included
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
        column_prop: ColumnProp = utils.hint_to_column_prop(hint_type)
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
                    if not utils.compare_columns(table_schema[column_name], column):
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
            "version": self._version,
            "engine_version": Schema.ENGINE_VERSION,
            "name": self._schema_name,
            "tables": self._schema_tables,
            "normalizers": self._normalizers_config,
            "preferred_types": self._preferred_types,
            "hints": self._hints,
            "excludes": self._excludes,
            "includes": self._includes
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
                        if isinstance(c[h], bool) and c[h] is False and h != "nullable":  # type: ignore
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
            py_data_type = utils.py_type_to_sc_type(type(v))
            if existing_column["data_type"] != py_data_type:
                # first try to coerce existing value into destination type
                try:
                    rv = utils.coerce_type(existing_column["data_type"], py_data_type, v)
                except (ValueError, SyntaxError):
                    # for complex types we must coerce to text
                    if py_data_type == "complex":
                        py_data_type = "text"
                        rv = utils.coerce_type("text", "complex", v)
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
            py_type = utils.py_type_to_sc_type(type(v))
            rv = utils.coerce_type(new_column["data_type"], py_type, v)

        return variant_col_name, new_column, rv

    def _map_value_to_column_type(self, v: Any, k: str) -> DataType:
        mapped_type = utils.py_type_to_sc_type(type(v))
         # if complex type was detected we must coerce to string
        if mapped_type == "complex":
            mapped_type = "text"
        # get preferred type based on column name
        preferred_type = self._get_preferred_type(k)
        # try to match python type to preferred
        if preferred_type:
            # try to coerce to destination type
            try:
                utils.coerce_type(preferred_type, mapped_type, v)
                # coercion possible so preferred type may be used
                mapped_type = preferred_type
            except ValueError:
                # coercion not possible
                pass
        return mapped_type

    def _get_preferred_type(self, col_name: str) -> Optional[DataType]:
        return next((m[1] for m in self._compiled_preferred_types if m[0].search(col_name)), None)

    def _infer_hint(self, hint_type: HintType, _: Any, k: str) -> bool:
        if hint_type in self._compiled_hints:
            return any(h.search(k) for h in self._compiled_hints[hint_type])
        else:
            return False

    def _add_standard_tables(self) -> None:
        self._schema_tables[Schema.VERSION_TABLE_NAME] = utils.version_table()
        self._schema_tables[Schema.LOADS_TABLE_NAME] = utils.load_table()

    def _add_standard_hints(self) -> None:
        self._hints = utils.standard_hints()

    def _configure_normalizers(self) -> None:
        if not self._normalizers_config:
            # create default normalizer config
            self._normalizers_config = {
                "names": "dlt.common.normalizers.names.snake_case",
                "json": {
                    "module": "dlt.common.normalizers.json.relational"
                }
            }
        # import desired modules
        naming_module = import_module(self._normalizers_config["names"])
        json_module = import_module(self._normalizers_config["json"]["module"])
        # name normalization functions
        self.normalize_table_name = naming_module.normalize_table_name
        self.normalize_column_name = naming_module.normalize_column_name
        # json normalization function
        self.json_normalize = json_module.normalize
        json_module.extend_schema(self)

    def _compile_regexes(self) -> None:
        for pattern, dt in self._preferred_types.items():
            # add tuples to be searched in coercions
            self._compiled_preferred_types.append((re.compile(pattern), dt))
        for hint_name, hint_list in self._hints.items():
            # compile hints which are column matching regexes
            self._compiled_hints[hint_name] = list(map(lambda hint: re.compile(hint), hint_list))
        self._compiled_excludes = list(map(lambda exclude: re.compile(exclude), self._excludes))
        self._compiled_includes = list(map(lambda include: re.compile(include), self._includes))
