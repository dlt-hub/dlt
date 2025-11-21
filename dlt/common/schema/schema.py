from __future__ import annotations

from copy import copy, deepcopy
from typing import (
    Callable,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Any,
    cast,
)

from dlt.common.schema.migrations import migrate_schema
from dlt.common.utils import extend_list_deduplicated, simple_repr, without_none
from dlt.common.typing import (
    DictStrAny,
    StrAny,
    REPattern,
    TDataItem,
)
from dlt.common.normalizers import TNormalizersConfig, NamingConvention
from dlt.common.normalizers.json import DataItemNormalizer, TNormalizedRowIterator
from dlt.common.schema import utils
from dlt.common.data_types import TDataType
from dlt.common.schema.typing import (
    DLT_NAME_PREFIX,
    SCHEMA_ENGINE_VERSION,
    LOADS_TABLE_NAME,
    VERSION_TABLE_NAME,
    PIPELINE_STATE_TABLE_NAME,
    TPartialTableSchema,
    TSchemaContractEntities,
    TSchemaEvolutionMode,
    TSchemaSettings,
    TSimpleRegex,
    TTableReferenceStandalone,
    TStoredSchema,
    TSchemaTables,
    TTableReference,
    TTableSchema,
    TTableSchemaColumns,
    TColumnSchema,
    TColumnProp,
    TColumnDefaultHint,
    TTypeDetections,
    TSchemaContractDict,
    TSchemaContract,
)
from dlt.common.schema.exceptions import (
    InvalidSchemaName,
    ParentTableNotFoundException,
    SchemaCorruptedException,
    TableIdentifiersFrozen,
    TableNotFound,
)
from dlt.common.schema.normalizers import import_normalizers, configured_normalizers
from dlt.common.schema.exceptions import DataValidationError
from dlt.common.validation import validate_dict


DEFAULT_SCHEMA_CONTRACT_MODE: TSchemaContractDict = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "evolve",
}


class Schema:
    ENGINE_VERSION: ClassVar[int] = SCHEMA_ENGINE_VERSION

    naming: NamingConvention
    """Naming convention used by the schema to normalize identifiers"""
    data_item_normalizer: DataItemNormalizer[Any]
    """Data item normalizer used by the schema to create tables"""

    version_table_name: str
    """Normalized name of the version table"""
    loads_table_name: str
    """Normalized name of the loads table"""
    state_table_name: str
    """Normalized name of the dlt state table"""

    _schema_name: str
    _dlt_tables_prefix: str
    _stored_version: int  # version at load time
    _stored_version_hash: str  # version hash at load time
    _stored_previous_hashes: Optional[List[str]]  # list of ancestor hashes of the schema
    _imported_version_hash: str  # version hash of recently imported schema
    _schema_description: str  # optional schema description
    _schema_tables: TSchemaTables
    _settings: (
        TSchemaSettings  # schema settings to hold default hints, preferred types and other settings
    )

    # list of preferred types: map regex on columns into types
    _compiled_preferred_types: List[Tuple[REPattern, TDataType]]
    # compiled default hints
    _compiled_hints: Dict[TColumnDefaultHint, Sequence[REPattern]]
    # compiled exclude filters per table
    _compiled_excludes: Dict[str, Sequence[REPattern]]
    # compiled include filters per table
    _compiled_includes: Dict[str, Sequence[REPattern]]
    # type detections
    _type_detections: Sequence[TTypeDetections]

    # normalizers config
    _normalizers_config: TNormalizersConfig

    def __init__(self, name: str, normalizers: TNormalizersConfig = None) -> None:
        self._reset_schema(name, normalizers)

    @classmethod
    def from_dict(
        cls,
        d: DictStrAny,
        remove_processing_hints: bool = False,
        bump_version: bool = True,
        validate_schema: bool = True,
    ) -> "Schema":
        # upgrade engine if needed
        stored_schema = migrate_schema(d, d["engine_version"], cls.ENGINE_VERSION)
        # verify schema
        if validate_schema:
            utils.validate_stored_schema(stored_schema)
        # add defaults
        stored_schema = utils.apply_defaults(stored_schema)
        # remove processing hints that could be created by normalize and load steps
        if remove_processing_hints:
            utils.remove_processing_hints(stored_schema["tables"])

        # bump version if modified
        if bump_version:
            utils.bump_version_if_modified(stored_schema)
        return cls.from_stored_schema(stored_schema)

    @classmethod
    def from_stored_schema(cls, stored_schema: TStoredSchema) -> "Schema":
        # create new instance from dict
        self: Schema = cls(
            stored_schema["name"], normalizers=stored_schema.get("normalizers", None)
        )
        self._from_stored_schema(stored_schema)
        return self

    def replace_schema_content(
        self, schema: "Schema", link_to_replaced_schema: bool = False
    ) -> None:
        """Replaces content of the current schema with `schema` content. Does not compute new schema hash and
        does not increase the numeric version. Optionally will link the replaced schema to incoming schema
        by keeping its hash in prev hashes and setting stored hash to replaced schema hash.
        """
        # do not bump version so hash from `schema` is preserved
        stored_schema = schema.to_dict(bump_version=False)
        if link_to_replaced_schema:
            replaced_version_hash = self.version_hash
            # do not store hash if the replaced schema is identical
            if schema.version_hash != replaced_version_hash:
                utils.store_prev_hash(stored_schema, replaced_version_hash)
        self._reset_schema(schema.name, schema._normalizers_config)
        self._from_stored_schema(stored_schema)

    def normalize_data_item(
        self, item: TDataItem, load_id: str, table_name: str
    ) -> TNormalizedRowIterator:
        return self.data_item_normalizer.normalize_data_item(item, load_id, table_name)

    def apply_schema_contract(
        self,
        schema_contract: TSchemaContractDict,
        partial_table: TPartialTableSchema,
        data_item: TDataItem = None,
        raise_on_freeze: bool = True,
    ) -> Tuple[
        TPartialTableSchema, List[Tuple[TSchemaContractEntities, str, TSchemaEvolutionMode]]
    ]:
        """
        Checks if `schema_contract` allows for the `partial_table` to update the schema. It applies the contract dropping
        the affected columns or the whole `partial_table`. It generates and returns a set of filters that should be applied to incoming data in order to modify it
        so it conforms to the contract. `data_item` is provided only as evidence in case DataValidationError is raised.

        Example `schema_contract`:
        {
            "tables": "freeze",
            "columns": "evolve",
            "data_type": "discard_row"
        }

        Settings for table affects new tables, settings for column affects new columns and settings for data_type affects new variant columns. Each setting can be set to one of:
        * evolve: allow all changes
        * freeze: allow no change and fail the load
        * discard_row: allow no schema change and filter out the row
        * discard_value: allow no schema change and filter out the value but load the rest of the row

        Returns a tuple where a first element is modified partial table and the second is a list of filters. The modified partial may be None in case the
        whole table is not allowed.
        Each filter is a tuple of (table|columns, entity name, freeze | discard_row | discard_value).
        Note: by default `freeze` immediately raises DataValidationError which is convenient in most use cases

        """
        # default settings allow all evolutions, skip all else
        if schema_contract == DEFAULT_SCHEMA_CONTRACT_MODE:
            return partial_table, []

        assert partial_table
        table_name = partial_table["name"]
        existing_table: TTableSchema = self._schema_tables.get(table_name, None)

        # table is new when not yet exist or
        is_new_table = not existing_table or self.is_new_table(table_name)
        # check case where we have a new table
        if is_new_table and schema_contract["tables"] != "evolve":
            if raise_on_freeze and schema_contract["tables"] == "freeze":
                raise DataValidationError(
                    self.name,
                    table_name,
                    None,
                    "tables",
                    "freeze",
                    None,
                    schema_contract,
                    data_item,
                    f"Can't add table `{table_name}` because `tables` are frozen.",
                )
            # filter tables with name below
            return None, [("tables", table_name, schema_contract["tables"])]

        column_mode, data_mode = schema_contract["columns"], schema_contract["data_type"]
        # allow to add new columns when table is new or if columns are allowed to evolve once
        if is_new_table or existing_table.get("x-normalizer", {}).get("evolve-columns-once", False):
            column_mode = "evolve"

        # check if we should filter any columns, partial table below contains only new columns
        filters: List[Tuple[TSchemaContractEntities, str, TSchemaEvolutionMode]] = []
        for column_name, column in list(partial_table["columns"].items()):
            # dlt cols may always be added
            if column_name.startswith(self._dlt_tables_prefix):
                continue
            is_variant = column.get("variant", False)
            # new column and contract prohibits that
            if column_mode != "evolve" and not is_variant:
                if raise_on_freeze and column_mode == "freeze":
                    raise DataValidationError(
                        self.name,
                        table_name,
                        column_name,
                        "columns",
                        "freeze",
                        existing_table,
                        schema_contract,
                        data_item,
                        f"Can't add table column `{column_name}` to table `{table_name}` because"
                        " `columns` are frozen.",
                    )
                # filter column with name below
                filters.append(("columns", column_name, column_mode))
                # pop the column
                partial_table["columns"].pop(column_name)

            # variant (data type evolution) and contract prohibits that
            if data_mode != "evolve" and is_variant:
                if raise_on_freeze and data_mode == "freeze":
                    raise DataValidationError(
                        self.name,
                        table_name,
                        column_name,
                        "data_type",
                        "freeze",
                        existing_table,
                        schema_contract,
                        data_item,
                        f"Can't add variant column `{column_name}` for table `{table_name}`"
                        " because `data_types` are frozen.",
                    )
                # filter column with name below
                filters.append(("columns", column_name, data_mode))
                # pop the column
                partial_table["columns"].pop(column_name)

        return partial_table, filters

    @staticmethod
    def expand_schema_contract_settings(
        settings: TSchemaContract, default: TSchemaContractDict = None
    ) -> TSchemaContractDict:
        """Expand partial or shorthand settings into full settings dictionary using `default` for unset entities"""
        if isinstance(settings, str):
            settings = TSchemaContractDict(tables=settings, columns=settings, data_type=settings)
        return cast(
            TSchemaContractDict, {**(default or DEFAULT_SCHEMA_CONTRACT_MODE), **(settings or {})}
        )

    def resolve_contract_settings_for_table(
        self, table_name: str, new_table_schema: TTableSchema = None
    ) -> TSchemaContractDict:
        """Resolve the exact applicable schema contract settings for the table `table_name`. `new_table_schema` is added to the tree during the resolution."""

        settings: TSchemaContract = {}
        if not table_name.startswith(self._dlt_tables_prefix):
            if new_table_schema:
                tables = copy(self._schema_tables)
                tables[table_name] = new_table_schema
            else:
                tables = self._schema_tables
            try:
                settings = utils.get_inherited_table_hint(tables, table_name, "schema_contract")
            except ValueError:
                settings = self._settings.get("schema_contract", {})

        # expand settings, empty settings will expand into default settings
        return Schema.expand_schema_contract_settings(settings)

    def update_table(
        self,
        partial_table: TPartialTableSchema,
        normalize_identifiers: bool = True,
        from_diff: bool = False,
    ) -> TPartialTableSchema:
        """Adds or merges `partial_table` into the schema. Identifiers are normalized by default.
        if `from_diff` is True, then `partial_table` is assumed to be a diff (contains only differences)
        vs. table in schema. in that case diff will not be created but directly applied
        """
        parent_table_name = partial_table.get("parent")
        if normalize_identifiers:
            partial_table = utils.normalize_table_identifiers(partial_table, self.naming)

        table_name = partial_table["name"]
        # check if parent table present
        if parent_table_name is not None:
            if self._schema_tables.get(parent_table_name) is None:
                raise ParentTableNotFoundException(
                    self.name,
                    table_name,
                    parent_table_name,
                    "If you declared nested hints, make sure you added all intermediate tables,"
                    " including those for which you do not declare any hints.",
                )
        table = self._schema_tables.get(table_name)
        if table is None:
            # add the whole new table to SchemaTables
            assert not from_diff, "Cannot update the whole table from diff"
            self._schema_tables[table_name] = partial_table
        else:
            if from_diff:
                partial_table = utils.merge_diff(table, partial_table)
            else:
                # merge tables performing additional checks
                partial_table = utils.merge_table(self.name, table, partial_table)

        self.data_item_normalizer.extend_table(table_name)
        return partial_table

    def update_schema(self, schema: "Schema") -> None:
        """Updates this schema from an incoming schema. Normalizes identifiers after updating normalizers."""
        # pass normalizer config
        self._settings = deepcopy(schema.settings)
        # make shallow copy of normalizer settings
        self._configure_normalizers(copy(schema._normalizers_config))
        self.data_item_normalizer.extend_schema(extend_tables=False)
        self._compile_settings()
        # update all tables starting for parents and then nested tables in order
        tables = list(schema.tables.values())
        for table in tables:
            if not utils.is_nested_table(table):
                for chain_table in utils.get_nested_tables(schema._schema_tables, table["name"]):
                    self.update_table(chain_table)

    def drop_tables(self, table_names: Sequence[str]) -> List[TTableSchema]:
        """Drops tables from the schema and returns the dropped tables. List of table names
        must contain all nested tables to tables being dropped.
        """
        result = []
        candidates = set()

        for table_name in table_names:
            if self.get_table(table_name) and table_name not in candidates:
                candidates.add(table_name)
                # also add table chain
                candidates.update(
                    t["name"] for t in utils.get_nested_tables(self._schema_tables, table_name)
                )
        # compare extension with original list
        if orphaned := candidates.difference(table_names):
            raise SchemaCorruptedException(
                self._schema_name,
                "A set tables to drop would leave orphaned tables. Please use consistent list of "
                f"table names in `drop_table`. Orphaned tabled: {orphaned}",
            )
        # final drop
        for table_name in table_names:
            if table_name in candidates:
                result.append(self._schema_tables.pop(table_name))
                self.data_item_normalizer.remove_table(table_name)

        return result

    def filter_row_with_hint(
        self, table_name: str, hint_type: TColumnDefaultHint, row: StrAny
    ) -> StrAny:
        rv_row: DictStrAny = {}
        column_prop: TColumnProp = utils.hint_to_column_prop(hint_type)
        try:
            table = self.get_table_columns(table_name, include_incomplete=True)
            for column_name in table:
                if column_name in row:
                    hint_value = table[column_name][column_prop]
                    if not utils.has_default_column_prop_value(column_prop, hint_value):
                        rv_row[column_name] = row[column_name]
        except KeyError:
            for k, v in row.items():
                if self._infer_hint(hint_type, k):
                    rv_row[k] = v

        # dicts are ordered and we will return the rows with hints in the same order as they appear in the columns
        return rv_row

    def merge_hints(
        self,
        new_hints: Mapping[TColumnDefaultHint, Sequence[TSimpleRegex]],
        replace: bool = False,
        normalize_identifiers: bool = True,
    ) -> None:
        """Merges or replace existing default hints with `new_hints`. Normalizes names in column regexes if possible. Compiles setting at the end

        NOTE: you can manipulate default hints collection directly via `Schema.settings` as long as you call Schema._compile_settings() at the end.
        """
        self._merge_hints(new_hints, replace=replace, normalize_identifiers=normalize_identifiers)
        self._compile_settings()

    def update_preferred_types(
        self,
        new_preferred_types: Mapping[TSimpleRegex, TDataType],
        normalize_identifiers: bool = True,
    ) -> None:
        """Updates preferred types dictionary with `new_preferred_types`. Normalizes names in column regexes if possible. Compiles setting at the end

        NOTE: you can manipulate preferred hints collection directly via `Schema.settings` as long as you call Schema._compile_settings() at the end.
        """
        self._update_preferred_types(new_preferred_types, normalize_identifiers)
        self._compile_settings()

    def add_type_detection(self, detection: TTypeDetections) -> None:
        """Add type auto detection to the schema."""
        if detection not in self.settings["detections"]:
            self.settings["detections"].append(detection)
            self._compile_settings()

    def remove_type_detection(self, detection: TTypeDetections) -> None:
        """Adds type auto detection to the schema."""
        if detection in self.settings["detections"]:
            self.settings["detections"].remove(detection)
            self._compile_settings()

    def get_new_table_columns(
        self,
        table_name: str,
        existing_columns: TTableSchemaColumns,
        case_sensitive: bool,
        include_incomplete: bool = False,
    ) -> List[TColumnSchema]:
        """Gets new columns to be added to `existing_columns` to bring them up to date with `table_name` schema.
        Columns names are compared case sensitive by default. `existing_column` names are expected to be normalized.
        Typically they come from the destination schema. Columns that are in `existing_columns` and not in `table_name` columns are ignored.

        Optionally includes incomplete columns (without data type)"""
        casefold_f: Callable[[str], str] = str.casefold if not case_sensitive else str
        casefold_existing = {
            casefold_f(col_name): col for col_name, col in existing_columns.items()
        }
        if len(existing_columns) != len(casefold_existing):
            raise SchemaCorruptedException(
                self.name,
                "A set of existing columns passed to `get_new_table_columns` table"
                f" `{table_name}` has colliding names when case insensitive comparison is used."
                f" Original names: {list(existing_columns.keys())}. Case-folded names:"
                f" {list(casefold_existing.keys())}",
            )
        diff_c: List[TColumnSchema] = []
        updated_columns = self.get_table_columns(table_name, include_incomplete=include_incomplete)
        for c in updated_columns.values():
            if casefold_f(c["name"]) not in casefold_existing:
                diff_c.append(c)
        return diff_c

    def get_table(self, table_name: str) -> TTableSchema:
        try:
            return self._schema_tables[table_name]
        except KeyError as k_exc:
            raise TableNotFound(self._schema_name, table_name) from k_exc

    def get_table_columns(
        self, table_name: str, include_incomplete: bool = False
    ) -> TTableSchemaColumns:
        """Gets columns of `table_name`. Optionally includes incomplete columns"""
        table = self.get_table(table_name)
        if include_incomplete:
            return table["columns"]
        else:
            return {k: v for k, v in table["columns"].items() if utils.is_complete_column(v)}

    def data_tables(
        self, seen_data_only: bool = False, include_incomplete: bool = False
    ) -> List[TTableSchema]:
        """Gets list of all tables, that hold the loaded data. Excludes dlt tables. Excludes incomplete tables (ie. without columns)"""
        return [
            t
            for t in self._schema_tables.values()
            if not t["name"].startswith(self._dlt_tables_prefix)
            and (
                (
                    include_incomplete
                    or len(self.get_table_columns(t["name"], include_incomplete)) > 0
                )
                and (not seen_data_only or utils.has_table_seen_data(t))
            )
        ]

    def data_table_names(
        self, seen_data_only: bool = False, include_incomplete: bool = False
    ) -> List[str]:
        """Returns list of table names. Excludes dlt table names."""
        return [
            t["name"]
            for t in self.data_tables(
                seen_data_only=seen_data_only, include_incomplete=include_incomplete
            )
        ]

    def dlt_tables(self) -> List[TTableSchema]:
        """Gets dlt tables"""
        return [
            t for t in self._schema_tables.values() if t["name"].startswith(self._dlt_tables_prefix)
        ]

    def dlt_table_names(self) -> List[str]:
        """Returns list of dlt table names."""
        return [t["name"] for t in self.dlt_tables()]

    def get_preferred_type(self, col_name: str) -> Optional[TDataType]:
        return next((m[1] for m in self._compiled_preferred_types if m[0].search(col_name)), None)

    def is_new_table(self, table_name: str) -> bool:
        """Returns true if this table does not exist OR is incomplete (has only incomplete columns) and therefore new"""
        return (table_name not in self.tables) or (
            not [
                c
                for c in self._schema_tables[table_name]["columns"].values()
                if utils.is_complete_column(c)
            ]
        )

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
        """Current version hash of the schema, recomputed from the actual content"""
        return utils.bump_version_if_modified(self.to_dict())[1]

    @property
    def previous_hashes(self) -> Sequence[str]:
        """Current version hash of the schema, recomputed from the actual content"""
        return utils.bump_version_if_modified(self.to_dict())[3]

    @property
    def stored_version_hash(self) -> str:
        """Version hash of the schema content form the time of schema loading/creation."""
        return self._stored_version_hash

    @property
    def is_modified(self) -> bool:
        """Checks if schema was modified from the time it was saved or if this is a new schema

        A current version hash is computed and compared with stored version hash
        """
        return self.version_hash != self._stored_version_hash

    @property
    def is_new(self) -> bool:
        """Checks if schema was ever saved"""
        return self._stored_version_hash is None

    @property
    def name(self) -> str:
        return self._schema_name

    @property
    def tables(self) -> TSchemaTables:
        """Dictionary of schema tables"""
        return self._schema_tables

    @property
    def references(self) -> list[TTableReferenceStandalone]:
        """References between tables"""
        all_references: list[TTableReferenceStandalone] = []
        for table_name, table in self.tables.items():
            # TODO more specific error handling than ValueError
            try:
                parent_ref = utils.create_parent_child_reference(self.tables, table_name)
                all_references.append(cast(TTableReferenceStandalone, parent_ref))
            except ValueError:
                pass

            try:
                root_ref = utils.create_root_child_reference(self.tables, table_name)
                all_references.append(cast(TTableReferenceStandalone, root_ref))
            except ValueError:
                pass

            try:
                load_table_ref = utils.create_load_table_reference(
                    self.tables[table_name], naming=self.naming
                )
                all_references.append(cast(TTableReferenceStandalone, load_table_ref))
            except ValueError:
                pass

            refs = table.get("references")
            if not refs:
                continue

            for ref in refs:
                top_level_ref: TTableReference = ref.copy()
                if top_level_ref.get("table") is None:
                    top_level_ref["table"] = table_name

                all_references.append(cast(TTableReferenceStandalone, top_level_ref))

        return all_references

    @property
    def settings(self) -> TSchemaSettings:
        return self._settings

    def __repr__(self) -> str:
        kwargs = {
            "name": self.name,
            "version": self.version,
            "tables": list(self.tables),
            "version_hash": self.version_hash,
        }
        return simple_repr("dlt.Schema", **without_none(kwargs))

    def _repr_html_(self, **kwargs: Any) -> str:
        """Render the Schema has a graphviz graph and display it using HTML

        This method is automatically called by notebooks renderers (IPython, marimo, etc.)
        ref: https://ipython.readthedocs.io/en/stable/config/integrating.html

        `dlt.helpers.graphviz.render_with_html()` has not external Python or system dependencies.
        """
        from dlt.helpers.graphviz import _render_dot_with_html

        return _render_dot_with_html(self.to_dot(**kwargs))

    def to_dict(
        self,
        remove_defaults: bool = False,
        remove_processing_hints: bool = False,
        bump_version: bool = True,
    ) -> TStoredSchema:
        stored_schema: TStoredSchema = {
            "version": self._stored_version,
            "version_hash": self._stored_version_hash,
            "engine_version": Schema.ENGINE_VERSION,
            "name": self._schema_name,
            "tables": self._schema_tables,
            "settings": self._settings,
            "normalizers": self._normalizers_config,
            "previous_hashes": self._stored_previous_hashes,
        }
        if self._imported_version_hash and not remove_defaults:
            stored_schema["imported_version_hash"] = self._imported_version_hash
        if self._schema_description:
            stored_schema["description"] = self._schema_description

        # remove processing hints that could be created by normalize and load steps
        if remove_processing_hints:
            stored_schema["tables"] = utils.remove_processing_hints(
                deepcopy(stored_schema["tables"])
            )

        # bump version if modified
        if bump_version:
            utils.bump_version_if_modified(stored_schema)
        # remove defaults after bumping version
        if remove_defaults:
            utils.remove_defaults(stored_schema)
        return stored_schema

    def to_pretty_json(
        self, remove_defaults: bool = True, remove_processing_hints: bool = False
    ) -> str:
        d = self.to_dict(
            remove_defaults=remove_defaults, remove_processing_hints=remove_processing_hints
        )
        return utils.to_pretty_json(d)

    def to_pretty_yaml(
        self, remove_defaults: bool = True, remove_processing_hints: bool = False
    ) -> str:
        d = self.to_dict(
            remove_defaults=remove_defaults, remove_processing_hints=remove_processing_hints
        )
        return utils.to_pretty_yaml(d)

    def to_dbml(
        self,
        remove_processing_hints: bool = False,
        include_dlt_tables: bool = True,
        include_internal_dlt_ref: bool = True,
        include_parent_child_ref: bool = True,
        include_root_child_ref: bool = True,
        group_by_resource: bool = False,
    ) -> str:
        from dlt.helpers.dbml import schema_to_dbml

        stored_schema = self.to_dict(
            # setting this to `True` removes `name` fields that are used in `schema_to_dbml()`
            # if required, we can refactor `dlt.helpers.dbml` to support this
            remove_defaults=False,
            remove_processing_hints=remove_processing_hints,
        )

        # NOTE `allow_custom_dbml_properties` is not exposed because it produces invalid DBML
        dbml_schema = schema_to_dbml(
            stored_schema,
            include_dlt_tables=include_dlt_tables,
            include_internal_dlt_ref=include_internal_dlt_ref,
            include_parent_child_ref=include_parent_child_ref,
            include_root_child_ref=include_root_child_ref,
            group_by_resource=group_by_resource,
        )
        return str(dbml_schema.dbml)

    def to_dot(
        self,
        remove_processing_hints: bool = False,
        include_dlt_tables: bool = True,
        include_internal_dlt_ref: bool = True,
        include_parent_child_ref: bool = True,
        include_root_child_ref: bool = True,
        group_by_resource: bool = False,
    ) -> str:
        """Convert schema to a Graphviz DOT string.

        Args:
            remove_processing_hints: If True, remove hints used for data processing and redundant information.
                This reduces the size of the schema and improves readability.
            include_dlt_tables: If True, include data tables and internal dlt tables. This will influence table
                references and groups produced.
            include_internal_dlt_ref: If True, include references between tables `_dlt_version`, `_dlt_loads` and `_dlt_pipeline_state`
            include_parent_child_ref: If True, include references from `child._dlt_parent_id` to `parent._dlt_id`
            include_root_child_ref: If True, include references from `child._dlt_root_id` to `root._dlt_id`
            group_by_resource: If True, group tables by resource and create subclusters.

        Returns:
            A DOT string of the schema
        """
        from dlt.helpers.graphviz import schema_to_graphviz

        stored_schema = self.to_dict(
            # setting this to `True` removes `name` fields that are used in `schema_to_dbml()`
            # if required, we can refactor `dlt.helpers.dbml` to support this
            remove_defaults=False,
            remove_processing_hints=remove_processing_hints,
        )

        dot = schema_to_graphviz(
            stored_schema,
            include_dlt_tables=include_dlt_tables,
            include_internal_dlt_ref=include_internal_dlt_ref,
            include_parent_child_ref=include_parent_child_ref,
            include_root_child_ref=include_root_child_ref,
            group_by_resource=group_by_resource,
        )
        return dot

    def to_mermaid(
        self,
        remove_processing_hints: bool = False,
        hide_columns: bool = False,
        hide_descriptions: bool = False,
        include_dlt_tables: bool = True,
    ) -> str:
        """Convert schema to a Mermaid diagram string.
        Args:
            remove_processing_hints: If True, remove hints used for data processing and redundant information.
                This reduces the size of the schema and improves readability.
            hide_columns: If True, the diagram hides columns details. This helps readability of large diagrams.
            hide_descriptions: If True, hide the column descriptions
            include_dlt_tables: If `True` (the default), internal dlt tables (`_dlt_version`,
                `_dlt_loads`, `_dlt_pipeline_state`)

        Returns:
            A string containing a Mermaid ERdiagram of the schema.
        """
        from dlt.helpers.mermaid import schema_to_mermaid

        stored_schema = self.to_dict(
            # setting this to `True` removes `name` fields that are used in `schema_to_dbml()`
            # if required, we can refactor `dlt.helpers.dbml` to support this
            remove_defaults=False,
            remove_processing_hints=remove_processing_hints,
        )

        return schema_to_mermaid(
            stored_schema,
            references=self.references,
            hide_columns=hide_columns,
            hide_descriptions=hide_descriptions,
            include_dlt_tables=include_dlt_tables,
        )

    def clone(
        self,
        with_name: str = None,
        remove_processing_hints: bool = False,
        update_normalizers: bool = False,
    ) -> "Schema":
        """Make a deep copy of the schema, optionally changing the name, removing processing markers and updating normalizers and identifiers in the schema if `update_normalizers` is True
        Processing markers are `x-` hints created by normalizer (`x-normalizer`) and loader (`x-loader`) to ie. mark newly inferred tables and tables that seen data.
        Note that changing of name will break the previous version chain
        """
        d = deepcopy(
            self.to_dict(bump_version=False, remove_processing_hints=remove_processing_hints)
        )
        if with_name is not None:
            d["version"] = d["version_hash"] = None
            d.pop("imported_version_hash", None)
            d["name"] = with_name
            d["previous_hashes"] = []
        schema = Schema.from_stored_schema(d)
        # update normalizers and possibly all schema identifiers
        if update_normalizers:
            schema.update_normalizers()
        return schema

    def update_normalizers(self) -> None:
        """Looks for new normalizer configuration or for destination capabilities context and updates all identifiers in the schema

        Table and column names will be normalized with new naming convention, except tables that have seen data ('x-normalizer`) which will
        raise if any identifier is to be changed.
        Default hints, preferred data types and normalize configs (ie. column propagation) are normalized as well. Regexes are included as long
        as textual parts can be extracted from an expression.
        """
        self._configure_normalizers(configured_normalizers(schema_name=self._schema_name))
        self.data_item_normalizer.extend_schema()
        self._compile_settings()

    def will_update_normalizers(self) -> bool:
        """Checks if schema has any pending normalizer updates due to configuration or destination capabilities"""

        # import desired modules
        _, to_naming, _ = import_normalizers(
            configured_normalizers(schema_name=self._schema_name), self._normalizers_config
        )
        return type(to_naming) is not type(self.naming)  # noqa

    def set_schema_contract(self, settings: TSchemaContract) -> None:
        if not settings:
            self._settings.pop("schema_contract", None)
        else:
            self._settings["schema_contract"] = settings

    def _infer_hint(self, hint_type: TColumnDefaultHint, col_name: str) -> bool:
        if hint_type in self._compiled_hints:
            return any(h.search(col_name) for h in self._compiled_hints[hint_type])
        else:
            return False

    def _merge_hints(
        self,
        new_hints: Mapping[TColumnDefaultHint, Sequence[TSimpleRegex]],
        replace: bool = False,
        normalize_identifiers: bool = True,
    ) -> None:
        """Used by `merge_hints method, does not compile settings at the end"""
        # validate regexes
        validate_dict(
            TSchemaSettings,
            {"default_hints": new_hints},
            ".",
            validator_f=utils.simple_regex_validator,
        )
        if normalize_identifiers:
            new_hints = self._normalize_default_hints(new_hints)
        # prepare hints to be added
        default_hints = self._settings.setdefault("default_hints", {})
        # add `new_hints` to existing hints
        for h, l in new_hints.items():
            if h in default_hints and not replace:
                extend_list_deduplicated(default_hints[h], l, utils.canonical_simple_regex)
            else:
                # set new hint type
                default_hints[h] = l  # type: ignore

    def _update_preferred_types(
        self,
        new_preferred_types: Mapping[TSimpleRegex, TDataType],
        normalize_identifiers: bool = True,
    ) -> None:
        # validate regexes
        validate_dict(
            TSchemaSettings,
            {"preferred_types": new_preferred_types},
            ".",
            validator_f=utils.simple_regex_validator,
        )
        if normalize_identifiers:
            new_preferred_types = self._normalize_preferred_types(new_preferred_types)
        preferred_types = self._settings.setdefault("preferred_types", {})
        # we must update using canonical simple regex
        canonical_preferred = {
            utils.canonical_simple_regex(rx): rx for rx in preferred_types.keys()
        }
        for new_rx, new_dt in new_preferred_types.items():
            canonical_new_rx = utils.canonical_simple_regex(new_rx)
            if canonical_new_rx not in canonical_preferred:
                preferred_types[new_rx] = new_dt
            else:
                preferred_types[canonical_preferred[canonical_new_rx]] = new_dt

    def _bump_version(self) -> Tuple[int, str]:
        """Computes schema hash in order to check if schema content was modified. In such case the schema ``stored_version`` and ``stored_version_hash`` are updated.

        Should not be used directly. The method ``to_dict`` will generate TStoredSchema with correct value, only once before persisting schema to storage.

        Returns:
            Tuple[int, str]: Current (``stored_version``, ``stored_version_hash``) tuple
        """
        self._stored_version, self._stored_version_hash, _, self._stored_previous_hashes = (
            utils.bump_version_if_modified(self.to_dict(bump_version=False))
        )
        return self._stored_version, self._stored_version_hash

    def _drop_version(self) -> None:
        """Stores first prev hash as stored hash and decreases numeric version"""
        if len(self.previous_hashes) == 0 or self._stored_version is None:
            self._stored_version = None
            self._stored_version_hash = None
        else:
            self._stored_version -= 1
            self._stored_version_hash = self._stored_previous_hashes.pop(0)

    def _add_standard_tables(self) -> None:
        self._schema_tables[self.version_table_name] = utils.normalize_table_identifiers(
            utils.version_table(), self.naming
        )
        self._schema_tables[self.loads_table_name] = utils.normalize_table_identifiers(
            utils.loads_table(), self.naming
        )

    def _add_standard_hints(self) -> None:
        default_hints = utils.default_hints()
        if default_hints:
            self._merge_hints(default_hints, normalize_identifiers=False)
        type_detections = utils.standard_type_detections()
        if type_detections:
            self._settings["detections"] = type_detections

    def _normalize_default_hints(
        self, default_hints: Mapping[TColumnDefaultHint, Sequence[TSimpleRegex]]
    ) -> Dict[TColumnDefaultHint, List[TSimpleRegex]]:
        """Normalizes the column names in default hints. In case of column names that are regexes, normalization is skipped"""
        return {
            hint: [utils.normalize_simple_regex_column(self.naming, regex) for regex in regexes]
            for hint, regexes in default_hints.items()
        }

    def _normalize_preferred_types(
        self, preferred_types: Mapping[TSimpleRegex, TDataType]
    ) -> Dict[TSimpleRegex, TDataType]:
        """Normalizes the column names in preferred types mapping. In case of column names that are regexes, normalization is skipped"""
        return {
            utils.normalize_simple_regex_column(self.naming, regex): data_type
            for regex, data_type in preferred_types.items()
        }

    def _verify_update_normalizers(
        self,
        normalizers_config: TNormalizersConfig,
        to_naming: NamingConvention,
        from_naming: NamingConvention,
    ) -> TSchemaTables:
        """Verifies if normalizers can be updated before schema is changed"""
        allow_ident_change = normalizers_config.get(
            "allow_identifier_change_on_table_with_data", False
        )

        def _verify_identifiers(table: TTableSchema, norm_table: TTableSchema) -> None:
            if not allow_ident_change:
                # make sure no identifier got changed in table
                if norm_table["name"] != table["name"]:
                    raise TableIdentifiersFrozen(
                        self.name,
                        table["name"],
                        to_naming,
                        from_naming,
                        f"Attempt to rename table to `{norm_table['name']}`.",
                    )
                # if len(norm_table["columns"]) != len(table["columns"]):
                #     print(norm_table["columns"])
                #     raise TableIdentifiersFrozen(
                #         self.name,
                #         table["name"],
                #         to_naming,
                #         from_naming,
                #         "Number of columns changed after normalization. Some columns must have"
                #         " merged.",
                #     )
                col_diff = set(norm_table["columns"].keys()).symmetric_difference(
                    table["columns"].keys()
                )
                if len(col_diff) > 0:
                    raise TableIdentifiersFrozen(
                        self.name,
                        table["name"],
                        to_naming,
                        from_naming,
                        f"Some columns got renamed to `{col_diff}`.",
                    )

        naming_changed = from_naming and type(from_naming) is not type(to_naming)
        if naming_changed:
            schema_tables = {}
            # check dlt tables
            schema_seen_data = any(
                utils.has_table_seen_data(t) for t in self._schema_tables.values()
            )
            # modify dlt tables using original naming
            orig_dlt_tables = [
                (self.version_table_name, utils.version_table()),
                (self.loads_table_name, utils.loads_table()),
                (self.state_table_name, utils.pipeline_state_table(add_dlt_id=True)),
            ]
            for existing_table_name, original_table in orig_dlt_tables:
                table = self._schema_tables.get(existing_table_name)
                # state table is optional
                if table:
                    table = copy(table)
                    # keep all attributes of the schema table, copy only what we need to normalize
                    table["columns"] = original_table["columns"]
                    norm_table = utils.normalize_table_identifiers(table, to_naming)
                    table_seen_data = utils.has_table_seen_data(norm_table)
                    if schema_seen_data:
                        _verify_identifiers(table, norm_table)
                    schema_tables[norm_table["name"]] = norm_table

            schema_seen_data = False
            for table in self.data_tables(include_incomplete=True):
                # TODO: when lineage is fully implemented we should use source identifiers
                # not `table` which was already normalized
                norm_table = utils.normalize_table_identifiers(table, to_naming)
                table_seen_data = utils.has_table_seen_data(norm_table)
                if table_seen_data:
                    _verify_identifiers(table, norm_table)
                schema_tables[norm_table["name"]] = norm_table
                schema_seen_data |= table_seen_data
            if schema_seen_data and not allow_ident_change:
                # if any of the tables has seen data, fail naming convention change
                # NOTE: this will be dropped with full identifier lineage. currently we cannot detect
                # strict schemas being changed to lax
                raise TableIdentifiersFrozen(
                    self.name,
                    "-",
                    to_naming,
                    from_naming,
                    "Schema contains tables that received data. As a precaution, changing naming"
                    " conventions is disallowed until full identifier lineage is implemented.",
                )
            # re-index the table names
            return schema_tables
        else:
            return self._schema_tables

    def _replace_and_apply_naming(
        self,
        normalizers_config: TNormalizersConfig,
        to_naming: NamingConvention,
        from_naming: NamingConvention,
    ) -> None:
        """Normalizes all identifiers in the schema in place according to `to_naming`"""
        self._schema_tables = self._verify_update_normalizers(
            normalizers_config, to_naming, from_naming
        )
        self._normalizers_config = normalizers_config
        self.naming = to_naming
        # name normalization functions
        self._dlt_tables_prefix = to_naming.normalize_table_identifier(DLT_NAME_PREFIX)
        self.version_table_name = to_naming.normalize_table_identifier(VERSION_TABLE_NAME)
        self.loads_table_name = to_naming.normalize_table_identifier(LOADS_TABLE_NAME)
        self.state_table_name = to_naming.normalize_table_identifier(PIPELINE_STATE_TABLE_NAME)
        # do a sanity check - dlt tables must start with dlt prefix
        for table_name in [self.version_table_name, self.loads_table_name, self.state_table_name]:
            if not table_name.startswith(self._dlt_tables_prefix):
                raise SchemaCorruptedException(
                    self.name,
                    f"A naming convention `{self.naming.name()}` mangles `_dlt` table prefix to"
                    f" '{self._dlt_tables_prefix}'. A table '{table_name}' does not start with it.",
                )
        # normalize default hints
        if default_hints := self._settings.get("default_hints"):
            self._settings["default_hints"] = self._normalize_default_hints(default_hints)
        # normalized preferred types
        if preferred_types := self.settings.get("preferred_types"):
            self._settings["preferred_types"] = self._normalize_preferred_types(preferred_types)

    def _configure_normalizers(self, explicit_normalizers: TNormalizersConfig) -> None:
        """Gets naming and item normalizer from schema yaml, config providers and destination capabilities and applies them to schema."""
        # preserve current schema settings if not explicitly set in `explicit_normalizers`
        if explicit_normalizers and self._normalizers_config:
            for prop_ in [
                "use_break_path_on_normalize",
                "allow_identifier_change_on_table_with_data",
            ]:
                if prop_ in self._normalizers_config and prop_ not in explicit_normalizers:
                    explicit_normalizers[prop_] = self._normalizers_config[prop_]  # type: ignore[literal-required]

        normalizers_config, to_naming, item_normalizer_class = import_normalizers(
            explicit_normalizers, self._normalizers_config
        )
        self._replace_and_apply_naming(normalizers_config, to_naming, self.naming)
        # data item normalization function
        self.data_item_normalizer = item_normalizer_class(self)

    def _reset_schema(self, name: str, normalizers: TNormalizersConfig = None) -> None:
        self._schema_tables: TSchemaTables = {}
        self._schema_name: str = None
        self._stored_version = None
        self._stored_version_hash: str = None
        self._imported_version_hash: str = None
        self._schema_description: str = None
        self._stored_previous_hashes: List[str] = []

        self._settings: TSchemaSettings = {}
        self._compiled_preferred_types: List[Tuple[REPattern, TDataType]] = []
        self._compiled_hints: Dict[TColumnDefaultHint, Sequence[REPattern]] = {}
        self._compiled_excludes: Dict[str, Sequence[REPattern]] = {}
        self._compiled_includes: Dict[str, Sequence[REPattern]] = {}
        self._type_detections: Sequence[TTypeDetections] = None

        self._normalizers_config = None
        self.naming = None
        self.data_item_normalizer = None

        # verify schema name - it does not depend on normalizers
        self._set_schema_name(name)
        # add standard hints
        self._add_standard_hints()
        # configure normalizers, including custom config if present
        if not normalizers:
            normalizers = configured_normalizers(schema_name=self._schema_name)
        self._configure_normalizers(normalizers)
        self.data_item_normalizer.extend_schema()  # type: ignore[attr-defined]
        # add version tables
        self._add_standard_tables()
        # compile all known regexes
        self._compile_settings()

    def _from_stored_schema(self, stored_schema: TStoredSchema) -> None:
        self._schema_tables = stored_schema.get("tables") or {}
        if self.version_table_name not in self._schema_tables:
            raise SchemaCorruptedException(
                stored_schema["name"], f"Schema must contain table `{self.version_table_name}`"
            )
        if self.loads_table_name not in self._schema_tables:
            raise SchemaCorruptedException(
                stored_schema["name"], f"Schema must contain table `{self.loads_table_name}`"
            )
        self._stored_version = stored_schema["version"]
        self._stored_version_hash = stored_schema["version_hash"]
        self._imported_version_hash = stored_schema.get("imported_version_hash")
        self._schema_description = stored_schema.get("description")
        self._settings = stored_schema.get("settings") or {}
        self._stored_previous_hashes = stored_schema.get("previous_hashes")
        self._compile_settings()

    def _set_schema_name(self, name: str) -> None:
        """Validates and sets the schema name. Validation does not need naming convention and checks against Python identifier"""
        if not utils.is_valid_schema_name(name):
            raise InvalidSchemaName(name)
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
                        self._compiled_excludes[table["name"]] = list(
                            map(utils.compile_simple_regex, table["filters"]["excludes"])
                        )
                    if "includes" in table["filters"]:
                        self._compiled_includes[table["name"]] = list(
                            map(utils.compile_simple_regex, table["filters"]["includes"])
                        )
        # look for auto-detections in settings and then normalizer
        self._type_detections = self._settings.get("detections") or self._normalizers_config.get("detections") or []  # type: ignore

    def __getstate__(self) -> Any:
        state = self.__dict__.copy()
        del state["naming"]
        del state["data_item_normalizer"]
        return state
