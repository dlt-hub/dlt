import yaml
from copy import copy, deepcopy
from typing import ClassVar, Dict, List, Mapping, Optional, Sequence, Tuple, Any, cast
from dlt.common import json

from dlt.common.typing import DictStrAny, StrAny, REPattern, SupportsVariant, VARIANT_FIELD_FORMAT, TDataItem
from dlt.common.normalizers import TNormalizersConfig, default_normalizers, import_normalizers
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.json import DataItemNormalizer, TNormalizedRowIterator
from dlt.common.schema import utils
from dlt.common.data_types import py_type_to_sc_type, coerce_value, TDataType
from dlt.common.schema.typing import (COLUMN_HINTS, SCHEMA_ENGINE_VERSION, LOADS_TABLE_NAME, VERSION_TABLE_NAME, TColumnSchemaBase, TPartialTableSchema, TSchemaSettings, TSimpleRegex, TStoredSchema,
                                      TSchemaTables, TTableSchema, TTableSchemaColumns, TColumnSchema, TColumnProp, TColumnHint, TTypeDetections, TWriteDisposition)
from dlt.common.schema.exceptions import (CannotCoerceColumnException, CannotCoerceNullException, InvalidSchemaName,
                                          ParentTableNotFoundException, SchemaCorruptedException)
from dlt.common.validation import validate_dict


class Schema:
    ENGINE_VERSION: ClassVar[int] = SCHEMA_ENGINE_VERSION

    naming: NamingConvention
    """Naming convention used by the schema to normalize identifiers"""
    data_item_normalizer: DataItemNormalizer[Any]
    """Data item normalizer used by the schema to create tables"""


    _schema_name: str
    _stored_version: int  # version at load/creation time
    _stored_version_hash: str  # version hash at load/creation time
    _imported_version_hash: str  # version hash of recently imported schema
    _schema_description: str  # optional schema description
    _schema_tables: TSchemaTables
    _settings: TSchemaSettings # schema settings to hold default hints, preferred types and other settings

    # list of preferred types: map regex on columns into types
    _compiled_preferred_types: List[Tuple[REPattern, TDataType]]
    # compiled default hints
    _compiled_hints: Dict[TColumnHint, Sequence[REPattern]]
    # compiled exclude filters per table
    _compiled_excludes: Dict[str, Sequence[REPattern]]
    # compiled include filters per table
    _compiled_includes: Dict[str, Sequence[REPattern]]
    # type detections
    _type_detections: Sequence[TTypeDetections]

    # normalizers config
    _normalizers_config: TNormalizersConfig

    def __init__(self, name: str, normalizers: TNormalizersConfig = None, normalize_name: bool = False) -> None:
        self._reset_schema(name, normalizers, normalize_name)

    @classmethod
    def from_dict(cls, d: DictStrAny) -> "Schema":
        # upgrade engine if needed
        stored_schema = utils.migrate_schema(d, d["engine_version"], cls.ENGINE_VERSION)
        # verify schema
        utils.validate_stored_schema(stored_schema)
        # add defaults
        utils.apply_defaults(stored_schema)

        # bump version if modified
        utils.bump_version_if_modified(stored_schema)
        return cls.from_stored_schema(stored_schema)

    @classmethod
    def from_stored_schema(cls, stored_schema: TStoredSchema) -> "Schema":
        # create new instance from dict
        self: Schema = cls(stored_schema["name"], normalizers=stored_schema.get("normalizers", None))
        self._from_stored_schema(stored_schema)
        return self

    def replace_schema_content(self, schema: "Schema") -> None:
        self._reset_schema(schema.name, schema._normalizers_config)
        self._from_stored_schema(schema.to_dict())

    def to_dict(self, remove_defaults: bool = False) -> TStoredSchema:
        stored_schema: TStoredSchema = {
            "version": self._stored_version,
            "version_hash": self._stored_version_hash,
            "engine_version": Schema.ENGINE_VERSION,
            "name": self._schema_name,
            "tables": self._schema_tables,
            "settings": self._settings,
            "normalizers": self._normalizers_config
        }
        if self._imported_version_hash and not remove_defaults:
            stored_schema["imported_version_hash"] = self._imported_version_hash
        if self._schema_description:
            stored_schema["description"] = self._schema_description

        # bump version if modified
        utils.bump_version_if_modified(stored_schema)
        # remove defaults after bumping version
        if remove_defaults:
            utils.remove_defaults(stored_schema)
        return stored_schema

    def normalize_data_item(self, item: TDataItem, load_id: str, table_name: str) -> TNormalizedRowIterator:
        return self.data_item_normalizer.normalize_data_item(item, load_id, table_name)

    def filter_row(self, table_name: str, row: StrAny) -> StrAny:
        # TODO: remove this. move to extract stage
        # exclude row elements according to the rules in `filter` elements of the table
        # include rules have precedence and are used to make exceptions to exclude rules
        # the procedure will apply rules from the table_name and it's all parent tables up until root
        # parent tables are computed by `normalize_break_path` function so they do not need to exist in the schema
        # note: the above is not very clean. the `parent` element of each table should be used but as the rules
        #  are typically used to prevent not only table fields but whole tables from being created it is not possible

        if not self._compiled_excludes:
            # if there are no excludes in the whole schema, no modification to a row can be made
            # most of the schema do not use them
            return row

        def _exclude(path: str, excludes: Sequence[REPattern], includes: Sequence[REPattern]) -> bool:
            is_included = False
            is_excluded = any(exclude.search(path) for exclude in excludes)
            if is_excluded:
                # we may have exception if explicitly included
                is_included = any(include.search(path) for include in includes)
            return is_excluded and not is_included

        # break table name in components
        branch = self.naming.break_path(table_name)

        # check if any of the rows is excluded by rules in any of the tables
        for i in range(len(branch), 0, -1):  # stop is exclusive in `range`
            # start at the top level table
            c_t = self.naming.make_path(*branch[:i])
            excludes = self._compiled_excludes.get(c_t)
            # only if there's possibility to exclude, continue
            if excludes:
                includes = self._compiled_includes.get(c_t) or []
                for field_name in list(row.keys()):
                    path = self.naming.make_path(*branch[i:], field_name)
                    if _exclude(path, excludes, includes):
                        # TODO: copy to new instance
                        del row[field_name]  # type: ignore
            # if row is empty, do not process further
            if not row:
                break
        return row

    def coerce_row(self, table_name: str, parent_table: str, row: StrAny) -> Tuple[DictStrAny, TPartialTableSchema]:
        # get existing or create a new table
        updated_table_partial: TPartialTableSchema = None
        table = self._schema_tables.get(table_name)
        if not table:
            table = utils.new_table(table_name, parent_table)
        table_columns = table["columns"]

        new_row: DictStrAny = {}
        for col_name, v in row.items():
            # skip None values, we should infer the types later
            if v is None:
                # just check if column is nullable if it exists
                self._coerce_null_value(table_columns, table_name, col_name)
            else:
                new_col_name, new_col_def, new_v = self._coerce_non_null_value(table_columns, table_name, col_name, v)
                new_row[new_col_name] = new_v
                if new_col_def:
                    if not updated_table_partial:
                        # create partial table with only the new columns
                        updated_table_partial = copy(table)
                        updated_table_partial["columns"] = {}
                    updated_table_partial["columns"][new_col_name] = new_col_def

        return new_row, updated_table_partial

    def update_schema(self, partial_table: TPartialTableSchema) -> TPartialTableSchema:
        table_name = partial_table["name"]
        parent_table_name = partial_table.get("parent")
        # check if parent table present
        if parent_table_name is not None:
            if self._schema_tables.get(parent_table_name) is None:
                raise ParentTableNotFoundException(
                    table_name, parent_table_name,
                    f" This may be due to misconfigured excludes filter that fully deletes content of the {parent_table_name}. Add includes that will preserve the parent table."
                    )
        table = self._schema_tables.get(table_name)
        if table is None:
            # add the whole new table to SchemaTables
            self._schema_tables[table_name] = partial_table
        else:
            # merge tables performing additional checks
            partial_table = utils.merge_tables(table, partial_table)
        return partial_table

    def bump_version(self) -> Tuple[int, str]:
        """Computes schema hash in order to check if schema content was modified. In such case the schema ``stored_version`` and ``stored_version_hash`` are updated.

        Should not be used in production code. The method ``to_dict`` will generate TStoredSchema with correct value, only once before persisting schema to storage.

        Returns:
            Tuple[int, str]: Current (``stored_version``, ``stored_version_hash``) tuple
        """
        version = utils.bump_version_if_modified(self.to_dict())
        self._stored_version, self._stored_version_hash = version
        return version

    def filter_row_with_hint(self, table_name: str, hint_type: TColumnHint, row: StrAny) -> StrAny:
        rv_row: DictStrAny = {}
        column_prop: TColumnProp = utils.hint_to_column_prop(hint_type)
        try:
            table = self.get_table_columns(table_name)
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

    def merge_hints(self, new_hints: Mapping[TColumnHint, Sequence[TSimpleRegex]]) -> None:
        # validate regexes
        validate_dict(TSchemaSettings, {"default_hints": new_hints}, ".", validator_f=utils.simple_regex_validator)
        # prepare hints to be added
        default_hints = self._settings.setdefault("default_hints", {})
        # add `new_hints` to existing hints
        for h, l in new_hints.items():
            if h in default_hints:
                # merge if hint of this type exist
                default_hints[h] = list(set(default_hints[h] + list(l)))
            else:
                # set new hint type
                default_hints[h] = l  # type: ignore
        self._compile_settings()

    def normalize_table_identifiers(self, table: TTableSchema) -> TTableSchema:
        # normalize all identifiers in table according to name normalizer of the schema
        table["name"] = self.naming.normalize_path(table["name"])
        parent = table.get("parent")
        if parent:
            table["parent"] = self.naming.normalize_path(parent)
        columns = table.get("columns")
        if columns:
            for c in columns.values():
                c["name"] = self.naming.normalize_path(c["name"])
            # re-index columns as the name changed
            table["columns"] = {c["name"]:c for c in columns.values()}
        return table

    def get_new_complete_columns(self, table_name: str, t: TTableSchemaColumns) -> List[TColumnSchema]:
        """Gets new columns to be added to "t" to bring up to date with stored schema. Exclude incomplete columns (without data type)"""
        diff_c: List[TColumnSchema] = []
        s_t = self.get_table_columns(table_name, only_complete=True)
        for c in s_t.values():
            if c["name"] not in t:
                diff_c.append(c)
        return diff_c

    def get_table(self, table_name: str) -> TTableSchema:
        return self._schema_tables[table_name]

    def get_table_columns(self, table_name: str, only_complete: bool = False) -> TTableSchemaColumns:
        """Gets columns of `table_name`"""
        if only_complete:
            return {k:v for k, v in self._schema_tables[table_name]["columns"].items() if utils.is_complete_column(v)}
        else:
            return self._schema_tables[table_name]["columns"]

    def data_tables(self) -> List[TTableSchema]:
        """Gets list of all tables, that hold the loaded data. Excludes dlt tables. Excludes incomplete tables (ie. without columns)"""
        return [t for t in self._schema_tables.values() if not t["name"].startswith("_dlt") and len(t["columns"]) > 0]

    def dlt_tables(self) -> List[TTableSchema]:
        """Gets dlt tables"""
        return [t for t in self._schema_tables.values() if t["name"].startswith("_dlt")]

    def get_preferred_type(self, col_name: str) -> Optional[TDataType]:
        return next((m[1] for m in self._compiled_preferred_types if m[0].search(col_name)), None)

    @property
    def version(self) -> int:
        """Version of the schema content that takes into account changes from the time of schema loading/creation.
        The stored version is increased by one if content was modified

        Returns:
            int: Current schema version
        """
        return utils.bump_version_if_modified(self.to_dict())[0]

    @property
    def stored_version(self) -> int:
        """Version of the schema content form the time of schema loading/creation.

        Returns:
            int: Stored schema version
        """
        return self._stored_version

    @property
    def version_hash(self) -> str:
        return utils.bump_version_if_modified(self.to_dict())[1]

    @property
    def stored_version_hash(self) -> str:
        return self._stored_version_hash

    @property
    def name(self) -> str:
        return self._schema_name

    @property
    def tables(self) -> TSchemaTables:
        """Dictionary of schema tables"""
        return self._schema_tables

    @property
    def settings(self) -> TSchemaSettings:
        return self._settings

    def to_pretty_json(self, remove_defaults: bool = True) -> str:
        d = self.to_dict(remove_defaults=remove_defaults)
        return json.dumps(d, pretty=True)

    def to_pretty_yaml(self, remove_defaults: bool = True) -> str:
        d = self.to_dict(remove_defaults=remove_defaults)
        return yaml.dump(d, allow_unicode=True, default_flow_style=False, sort_keys=False)

    def clone(self) -> "Schema":
        d = deepcopy(self.to_dict())
        return Schema.from_dict(d)  # type: ignore

    def _infer_column(self, k: str, v: Any, data_type: TDataType = None, is_variant: bool = False) -> TColumnSchema:
        column_schema =  TColumnSchema(
            name=k,
            data_type=data_type or self._infer_column_type(v, k),
            nullable=not self._infer_hint("not_null", v, k)
        )
        for hint in COLUMN_HINTS:
            column_schema[utils.hint_to_column_prop(hint)] = self._infer_hint(hint, v, k)

        if is_variant:
            column_schema["variant"] = is_variant
        return column_schema

    def _coerce_null_value(self, table_columns: TTableSchemaColumns, table_name: str, col_name: str) -> None:
        """Raises when column is explicitly not nullable"""
        if col_name in table_columns:
            existing_column = table_columns[col_name]
            if not existing_column.get("nullable", True):
                raise CannotCoerceNullException(table_name, col_name)

    def _coerce_non_null_value(self, table_columns: TTableSchemaColumns, table_name: str, col_name: str, v: Any, is_variant: bool = False) -> Tuple[str, TColumnSchema, Any]:
        new_column: TColumnSchema = None
        existing_column = table_columns.get(col_name)
        # if column exist but is incomplete then keep it as new column
        if existing_column and not utils.is_complete_column(existing_column):
            new_column = existing_column
            existing_column = None

        # infer type or get it from existing table
        col_type = existing_column["data_type"] if existing_column else self._infer_column_type(v, col_name, skip_preferred=is_variant)
        # get data type of value
        py_type = py_type_to_sc_type(type(v))
        # and coerce type if inference changed the python type
        try:
            coerced_v = coerce_value(col_type, py_type, v)
            # print(f"co: {py_type} -> {col_type} {v}")
        except (ValueError, SyntaxError):
            if is_variant:
                # this is final call: we cannot generate any more auto-variants
                raise CannotCoerceColumnException(table_name, col_name, py_type, table_columns[col_name]["data_type"], v)
            # otherwise we must create variant extension to the table
            # pass final=True so no more auto-variants can be created recursively
            # TODO: generate callback so DLT user can decide what to do
            variant_col_name = self.naming.shorten_fragments(col_name, VARIANT_FIELD_FORMAT % py_type)
            return self._coerce_non_null_value(table_columns, table_name, variant_col_name, v, is_variant=True)

        # if coerced value is variant, then extract variant value
        # note: checking runtime protocols with isinstance(coerced_v, SupportsVariant): is extremely slow so we check if callable as every variant is callable
        if callable(coerced_v):  # and isinstance(coerced_v, SupportsVariant):
            coerced_v = coerced_v()
            if isinstance(coerced_v, tuple):
                # variant recovered so call recursively with variant column name and variant value
                variant_col_name = self.naming.shorten_fragments(col_name, VARIANT_FIELD_FORMAT % coerced_v[0])
                return self._coerce_non_null_value(table_columns, table_name, variant_col_name, coerced_v[1], is_variant=True)

        if not existing_column:
            inferred_column = self._infer_column(col_name, v, data_type=col_type, is_variant=is_variant)
            # if there's partial new_column then merge it with inferred column
            if new_column:
                new_column = utils.merge_columns(new_column, inferred_column, merge_defaults=True)
            else:
                new_column = inferred_column

        return col_name, new_column, coerced_v

    def _infer_column_type(self, v: Any, col_name: str, skip_preferred: bool = False) -> TDataType:
        tv = type(v)
        # try to autodetect data type
        mapped_type = utils.autodetect_sc_type(self._type_detections, tv, v)
        # if not try standard type mapping
        if mapped_type is None:
            mapped_type = py_type_to_sc_type(tv)
        # get preferred type based on column name
        preferred_type: TDataType = None
        if not skip_preferred:
            preferred_type = self.get_preferred_type(col_name)
        return preferred_type or mapped_type

    def _infer_hint(self, hint_type: TColumnHint, _: Any, col_name: str) -> bool:
        if hint_type in self._compiled_hints:
            return any(h.search(col_name) for h in self._compiled_hints[hint_type])
        else:
            return False

    def _add_standard_tables(self) -> None:
        self._schema_tables[VERSION_TABLE_NAME] = utils.version_table()
        self._schema_tables[LOADS_TABLE_NAME] = utils.load_table()

    def _add_standard_hints(self) -> None:
        default_hints = utils.standard_hints()
        if default_hints:
            self._settings["default_hints"] = default_hints
        type_detections = utils.standard_type_detections()
        if type_detections:
            self._settings["detections"] = type_detections

    def _configure_normalizers(self) -> None:
        if not self._normalizers_config:
            # create default normalizer config
            self._normalizers_config = default_normalizers()
        # import desired modules
        naming_module, item_normalizer_class = import_normalizers(self._normalizers_config)
        # name normalization functions
        self.naming = naming_module
        # data item normalization function
        self.data_item_normalizer = item_normalizer_class(self)
        self.data_item_normalizer.extend_schema()

    def _reset_schema(self, name: str, normalizers: TNormalizersConfig = None, normalize_name: bool = False) -> None:
        self._schema_tables: TSchemaTables = {}
        self._schema_name: str = None
        self._stored_version = 1
        self._stored_version_hash: str = None
        self._imported_version_hash: str = None
        self._schema_description: str = None

        self._settings: TSchemaSettings = {}
        self._compiled_preferred_types: List[Tuple[REPattern, TDataType]] = []
        self._compiled_hints: Dict[TColumnHint, Sequence[REPattern]] = {}
        self._compiled_excludes: Dict[str, Sequence[REPattern]] = {}
        self._compiled_includes: Dict[str, Sequence[REPattern]] = {}
        self._type_detections: Sequence[TTypeDetections] = None

        self._normalizers_config: TNormalizersConfig = normalizers
        self.naming = None
        self.data_item_normalizer = None

        # add version tables
        self._add_standard_tables()
        # add standard hints
        self._add_standard_hints()
        # configure normalizers, including custom config if present
        self._configure_normalizers()
        # verify schema name after configuring normalizers
        self._set_schema_name(name, normalize_name)
        # compile all known regexes
        self._compile_settings()
        # set initial version hash
        self._stored_version_hash = self.version_hash

    def _from_stored_schema(self, stored_schema: TStoredSchema) -> None:
        self._schema_tables = stored_schema.get("tables") or {}
        if VERSION_TABLE_NAME not in self._schema_tables:
            raise SchemaCorruptedException(f"Schema must contain table {VERSION_TABLE_NAME}")
        if LOADS_TABLE_NAME not in self._schema_tables:
            raise SchemaCorruptedException(f"Schema must contain table {LOADS_TABLE_NAME}")
        self._stored_version = stored_schema["version"]
        self._stored_version_hash = stored_schema["version_hash"]
        self._imported_version_hash = stored_schema.get("imported_version_hash")
        self._schema_description = stored_schema.get("description")
        self._settings = stored_schema.get("settings") or {}
        self._compile_settings()

    def _set_schema_name(self, name: str, normalize_name: bool) -> None:
        normalized_name = self.naming.normalize_identifier(name)
        if name != normalized_name:
            if normalize_name:
                name = normalized_name
            else:
                raise InvalidSchemaName(name, normalized_name)
        self._schema_name = name

    def _compile_settings(self) -> None:
        # if self._settings:
        for pattern, dt in self._settings.get("preferred_types", {}).items():
            # add tuples to be searched in coercions
            self._compiled_preferred_types.append((utils.compile_simple_regex(pattern), dt))
        for hint_name, hint_list in self._settings.get("default_hints", {}).items():
            # compile hints which are column matching regexes
            self._compiled_hints[hint_name] = list(map(utils.compile_simple_regex, hint_list))
        if self._schema_tables:
            for table in self._schema_tables.values():
                if "filters" in table:
                    if "excludes" in table["filters"]:
                        self._compiled_excludes[table["name"]] = list(map(utils.compile_simple_regex, table["filters"]["excludes"]))
                    if "includes" in table["filters"]:
                        self._compiled_includes[table["name"]] = list(map(utils.compile_simple_regex, table["filters"]["includes"]))
        # look for auto-detections in settings and then normalizer
        self._type_detections = self._settings.get("detections") or self._normalizers_config.get("detections") or []  # type: ignore

    def __repr__(self) -> str:
        return f"Schema {self.name} at {id(self)}"
