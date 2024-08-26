from copy import copy, deepcopy
from typing import TypedDict, cast, Any, Optional, Dict

from dlt.common import logger
from dlt.common.schema.typing import (
    TColumnNames,
    TColumnProp,
    TFileFormat,
    TPartialTableSchema,
    TTableSchema,
    TTableSchemaColumns,
    TWriteDispositionConfig,
    TMergeDispositionDict,
    TAnySchemaColumns,
    TTableFormat,
    TSchemaContract,
    DEFAULT_VALIDITY_COLUMN_NAMES,
    MERGE_STRATEGIES,
)
from dlt.common.schema.utils import (
    DEFAULT_WRITE_DISPOSITION,
    DEFAULT_MERGE_STRATEGY,
    merge_column,
    merge_columns,
    new_column,
    new_table,
)
from dlt.common.typing import TDataItem
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.utils import clone_dict_nested
from dlt.common.normalizers.json.relational import DataItemNormalizer
from dlt.common.validation import validate_dict_ignoring_xkeys
from dlt.extract.exceptions import (
    DataItemRequiredForDynamicTableHints,
    InconsistentTableTemplate,
)
from dlt.extract.incremental import Incremental
from dlt.extract.items import TFunHintTemplate, TTableHintTemplate, TableNameMeta, ValidateItem
from dlt.extract.utils import ensure_table_schema_columns, ensure_table_schema_columns_hint
from dlt.extract.validation import create_item_validator


class TResourceHints(TypedDict, total=False):
    name: TTableHintTemplate[str]
    # description: TTableHintTemplate[str]
    write_disposition: TTableHintTemplate[TWriteDispositionConfig]
    # table_sealed: Optional[bool]
    parent: TTableHintTemplate[str]
    columns: TTableHintTemplate[TTableSchemaColumns]
    primary_key: TTableHintTemplate[TColumnNames]
    merge_key: TTableHintTemplate[TColumnNames]
    incremental: Incremental[Any]
    schema_contract: TTableHintTemplate[TSchemaContract]
    table_format: TTableHintTemplate[TTableFormat]
    file_format: TTableHintTemplate[TFileFormat]
    validator: ValidateItem
    original_columns: TTableHintTemplate[TAnySchemaColumns]


class HintsMeta:
    __slots__ = ("hints", "create_table_variant")

    def __init__(self, hints: TResourceHints, create_table_variant: bool) -> None:
        self.hints = hints
        self.create_table_variant = create_table_variant


NATURAL_CALLABLES = ["incremental", "validator", "original_columns"]


def make_hints(
    table_name: TTableHintTemplate[str] = None,
    parent_table_name: TTableHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    primary_key: TTableHintTemplate[TColumnNames] = None,
    merge_key: TTableHintTemplate[TColumnNames] = None,
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
    table_format: TTableHintTemplate[TTableFormat] = None,
    file_format: TTableHintTemplate[TFileFormat] = None,
) -> TResourceHints:
    """A convenience function to create resource hints. Accepts both static and dynamic hints based on data.

    This method accepts the same table hints arguments as `dlt.resource` decorator.
    """
    validator, schema_contract = create_item_validator(columns, schema_contract)
    clean_columns = columns
    if columns is not None:
        clean_columns = ensure_table_schema_columns_hint(columns)
        if not callable(clean_columns):
            clean_columns = clean_columns.values()  # type: ignore
    # create a table schema template where hints can be functions taking TDataItem
    new_template: TResourceHints = new_table(
        table_name,  # type: ignore
        parent_table_name,  # type: ignore
        write_disposition=write_disposition,  # type: ignore
        columns=clean_columns,  # type: ignore
        schema_contract=schema_contract,  # type: ignore
        table_format=table_format,  # type: ignore
        file_format=file_format,  # type: ignore
    )
    if not table_name:
        new_template.pop("name")
    if not write_disposition and "write_disposition" in new_template:
        new_template.pop("write_disposition")
    # remember original columns
    if columns is not None:
        new_template["original_columns"] = columns
    # always remove resource
    new_template.pop("resource", None)  # type: ignore
    if primary_key is not None:
        new_template["primary_key"] = primary_key
    if merge_key is not None:
        new_template["merge_key"] = merge_key
    if validator:
        new_template["validator"] = validator
    DltResourceHints.validate_dynamic_hints(new_template)
    return new_template


class DltResourceHints:
    def __init__(self, table_schema_template: TResourceHints = None):
        self.__qualname__ = self.__name__ = self.name
        self._table_name_hint_fun: TFunHintTemplate[str] = None
        self._table_has_other_dynamic_hints: bool = False
        self._hints: TResourceHints = None
        """Hints for the resource"""
        self._hints_variants: Dict[str, TResourceHints] = {}
        """Hints for tables emitted from resources"""
        if table_schema_template:
            self._set_hints(table_schema_template)

    @property
    def name(self) -> str:
        pass

    @property
    def table_name(self) -> TTableHintTemplate[str]:
        """Get table name to which resource loads data. May return a callable."""
        if self._table_name_hint_fun:
            return self._table_name_hint_fun
        # get table name or default name
        return self._hints.get("name") or self.name if self._hints else self.name

    @table_name.setter
    def table_name(self, value: TTableHintTemplate[str]) -> None:
        self.apply_hints(table_name=value)

    @property
    def write_disposition(self) -> TTableHintTemplate[TWriteDispositionConfig]:
        if self._hints is None or self._hints.get("write_disposition") is None:
            return DEFAULT_WRITE_DISPOSITION
        return self._hints.get("write_disposition")

    @write_disposition.setter
    def write_disposition(self, value: TTableHintTemplate[TWriteDispositionConfig]) -> None:
        self.apply_hints(write_disposition=value)

    @property
    def columns(self) -> TTableHintTemplate[TTableSchemaColumns]:
        """Gets columns' schema that can be modified in place"""
        return None if self._hints is None else self._hints.get("columns")

    @property
    def schema_contract(self) -> TTableHintTemplate[TSchemaContract]:
        return self._hints.get("schema_contract")

    @property
    def table_format(self) -> TTableHintTemplate[TTableFormat]:
        return None if self._hints is None else self._hints.get("table_format")

    def compute_table_schema(self, item: TDataItem = None, meta: Any = None) -> TTableSchema:
        """Computes the table schema based on hints and column definitions passed during resource creation.
        `item` parameter is used to resolve table hints based on data.
        `meta` parameter is taken from Pipe and may further specify table name if variant is to be used
        """
        if isinstance(meta, TableNameMeta):
            # look for variant
            table_template = self._hints_variants.get(meta.table_name, self._hints)
        else:
            table_template = self._hints
        if not table_template:
            return new_table(self.name, resource=self.name)

        # resolve a copy of a held template
        table_template = self._clone_hints(table_template)
        if "name" not in table_template:
            table_template["name"] = self.name

        # if table template present and has dynamic hints, the data item must be provided.
        if self._table_name_hint_fun and item is None:
            raise DataItemRequiredForDynamicTableHints(self.name)
        # resolve
        resolved_template: TResourceHints = {
            k: self._resolve_hint(item, v)
            for k, v in table_template.items()
            if k not in NATURAL_CALLABLES
        }  # type: ignore
        table_schema = self._create_table_schema(resolved_template, self.name)
        validate_dict_ignoring_xkeys(
            spec=TTableSchema,
            doc=table_schema,
            path=f"new_table/{self.name}",
        )
        return table_schema

    def apply_hints(
        self,
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        incremental: Incremental[Any] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        additional_table_hints: Optional[Dict[str, TTableHintTemplate[Any]]] = None,
        table_format: TTableHintTemplate[TTableFormat] = None,
        file_format: TTableHintTemplate[TFileFormat] = None,
        create_table_variant: bool = False,
    ) -> None:
        """Creates or modifies existing table schema by setting provided hints. Accepts both static and dynamic hints based on data.

        If `create_table_variant` is specified, the `table_name` must be a string and hints will be used to create a separate set of hints
        for a particular `table_name`. Such hints may be retrieved via compute_table_schema(meta=TableNameMeta(table_name)).
        Table variant hints may not contain dynamic hints.

        This method accepts the same table hints arguments as `dlt.resource` decorator with the following additions.
        Skip the argument or pass None to leave the existing hint.
        Pass empty value (for a particular type i.e. "" for a string) to remove a hint.

        parent_table_name (str, optional): A name of parent table if foreign relation is defined. Please note that if you use merge, you must define `root_key` columns explicitly
        incremental (Incremental, optional): Enables the incremental loading for a resource.

        Please note that for efficient incremental loading, the resource must be aware of the Incremental by accepting it as one if its arguments and then using are to skip already loaded data.
        In non-aware resources, `dlt` will filter out the loaded values, however, the resource will yield all the values again.
        """
        if create_table_variant:
            if not isinstance(table_name, str):
                raise ValueError(
                    "Please provide string table name if you want to create a table variant of"
                    " hints"
                )
            # select hints variant
            t = self._hints_variants.get(table_name, None)
            if t is None:
                # use resource hints as starting point
                if self._hints:
                    t = self._clone_hints(self._hints)
                    # but remove callables
                    t = {n: h for n, h in t.items() if not callable(h)}  # type: ignore[assignment]
        else:
            t = self._hints

        if t is None:
            # if there is no template yet, create and set a new one.
            default_wd = None if parent_table_name else DEFAULT_WRITE_DISPOSITION
            t = make_hints(
                table_name,
                parent_table_name,
                write_disposition or default_wd,
                columns,
                primary_key,
                merge_key,
                schema_contract,
                table_format,
                file_format,
            )
        else:
            t = self._clone_hints(t)
            if table_name is not None:
                if table_name:
                    t["name"] = table_name
                else:
                    t.pop("name", None)
            if parent_table_name is not None:
                if parent_table_name:
                    t["parent"] = parent_table_name
                else:
                    t.pop("parent", None)
            if write_disposition:
                t["write_disposition"] = write_disposition
            if columns is not None:
                # keep original columns: i.e. in case it is a Pydantic model.
                t["original_columns"] = columns
                # if callable then override existing
                if callable(columns) or callable(t["columns"]):
                    t["columns"] = ensure_table_schema_columns_hint(columns)
                elif columns:
                    # normalize columns
                    columns = ensure_table_schema_columns(columns)
                    # this updates all columns with defaults
                    t["columns"] = merge_columns(t["columns"], columns, merge_columns=True)
                else:
                    # set to empty columns
                    t["columns"] = ensure_table_schema_columns(columns)
            if primary_key is not None:
                if primary_key:
                    t["primary_key"] = primary_key
                else:
                    t.pop("primary_key", None)
            if merge_key is not None:
                if merge_key:
                    t["merge_key"] = merge_key
                else:
                    t.pop("merge_key", None)
            if schema_contract is not None:
                if schema_contract:
                    t["schema_contract"] = schema_contract
                else:
                    t.pop("schema_contract", None)
            if additional_table_hints is not None:
                for k, v in additional_table_hints.items():
                    if v:
                        t[k] = v  # type: ignore[literal-required]
                    else:
                        t.pop(k, None)  # type: ignore[misc]
                t.pop("additional_table_hints", None)  # type: ignore

            # recreate validator if column definition or contract changed
            if schema_contract is not None or columns is not None:
                t["validator"], schema_contract = create_item_validator(
                    t.get("original_columns"), t.get("schema_contract")
                )
            if schema_contract is not None:
                t["schema_contract"] = schema_contract
            if table_format is not None:
                if table_format:
                    t["table_format"] = table_format
                else:
                    t.pop("table_format", None)
            if file_format is not None:
                if file_format:
                    t["file_format"] = file_format
                else:
                    t.pop("file_format", None)

        # set properties that can't be passed to make_hints
        if incremental is not None:
            t["incremental"] = incremental

        self._set_hints(t, create_table_variant)

    def _set_hints(
        self, hints_template: TResourceHints, create_table_variant: bool = False
    ) -> None:
        DltResourceHints.validate_dynamic_hints(hints_template)
        DltResourceHints.validate_write_disposition_hint(hints_template.get("write_disposition"))
        if create_table_variant:
            table_name: str = hints_template["name"]  # type: ignore[assignment]
            # incremental cannot be specified in variant
            if hints_template.get("incremental"):
                raise InconsistentTableTemplate(
                    f"You can specify incremental only for the resource `{self.name}` hints, not in"
                    f" table `{table_name}` variant-"
                )
            if hints_template.get("validator"):
                logger.warning(
                    f"A data item validator was created from column schema in {self.name} for a"
                    f" table `{table_name}` variant. Currently such validator is ignored."
                )
            # dynamic hints will be ignored
            for name, hint in hints_template.items():
                if callable(hint) and name not in NATURAL_CALLABLES:
                    raise InconsistentTableTemplate(
                        f"Table `{table_name}` variant hint is resource {self.name} cannot have"
                        f" dynamic hint but {name} does."
                    )
            self._hints_variants[table_name] = hints_template
        else:
            # if "name" is callable in the template, then the table schema requires data item to be inferred.
            name_hint = hints_template.get("name")
            self._table_name_hint_fun = name_hint if callable(name_hint) else None
            # check if any other hints in the table template should be inferred from data.
            self._table_has_other_dynamic_hints = any(
                callable(v) for k, v in hints_template.items() if k != "name"
            )
            self._hints = hints_template

    def merge_hints(
        self, hints_template: TResourceHints, create_table_variant: bool = False
    ) -> None:
        self.apply_hints(
            table_name=hints_template.get("name"),
            parent_table_name=hints_template.get("parent"),
            write_disposition=hints_template.get("write_disposition"),
            columns=hints_template.get("original_columns"),
            primary_key=hints_template.get("primary_key"),
            merge_key=hints_template.get("merge_key"),
            incremental=hints_template.get("incremental"),
            schema_contract=hints_template.get("schema_contract"),
            table_format=hints_template.get("table_format"),
            file_format=hints_template.get("file_format"),
            create_table_variant=create_table_variant,
        )

    @staticmethod
    def _clone_hints(hints_template: TResourceHints) -> TResourceHints:
        if hints_template is None:
            return None
        # creates a deep copy of dict structure without actually copying the objects
        # deepcopy(hints_template) #
        return clone_dict_nested(hints_template)  # type: ignore[type-var]

    @staticmethod
    def _resolve_hint(item: TDataItem, hint: TTableHintTemplate[Any]) -> Any:
        """Calls each dynamic hint passing a data item"""
        return hint(item) if callable(hint) else hint

    @staticmethod
    def _merge_key(hint: TColumnProp, keys: TColumnNames, partial: TPartialTableSchema) -> None:
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            if key in partial["columns"]:
                # set nullable to False if not set
                nullable = partial["columns"][key].get("nullable", False)
                merge_column(partial["columns"][key], {hint: True, "nullable": nullable})  # type: ignore
            else:
                partial["columns"][key] = new_column(key, nullable=False)
                partial["columns"][key][hint] = True

    @staticmethod
    def _merge_keys(dict_: Dict[str, Any]) -> None:
        """Merges primary and merge keys into columns in place."""

        if "primary_key" in dict_:
            DltResourceHints._merge_key("primary_key", dict_.pop("primary_key"), dict_)  # type: ignore
        if "merge_key" in dict_:
            DltResourceHints._merge_key("merge_key", dict_.pop("merge_key"), dict_)  # type: ignore

    @staticmethod
    def _merge_write_disposition_dict(dict_: Dict[str, Any]) -> None:
        """Merges write disposition dictionary into write disposition shorthand and x-hints in place."""

        if dict_["write_disposition"]["disposition"] == "merge":
            DltResourceHints._merge_merge_disposition_dict(dict_)
        # reduce merge disposition from dict to shorthand
        dict_["write_disposition"] = dict_["write_disposition"]["disposition"]

    @staticmethod
    def _merge_merge_disposition_dict(dict_: Dict[str, Any]) -> None:
        """Merges merge disposition dict into x-hints in place."""

        mddict: TMergeDispositionDict = deepcopy(dict_["write_disposition"])
        if mddict is not None:
            dict_["x-merge-strategy"] = mddict.get("strategy", DEFAULT_MERGE_STRATEGY)
            if "boundary_timestamp" in mddict:
                dict_["x-boundary-timestamp"] = mddict["boundary_timestamp"]
            # add columns for `scd2` merge strategy
            if dict_.get("x-merge-strategy") == "scd2":
                if mddict.get("validity_column_names") is None:
                    from_, to = DEFAULT_VALIDITY_COLUMN_NAMES
                else:
                    from_, to = mddict["validity_column_names"]
                dict_["columns"][from_] = {
                    "name": from_,
                    "data_type": "timestamp",
                    "nullable": (
                        True
                    ),  # validity columns are empty when first loaded into staging table
                    "x-valid-from": True,
                }
                dict_["columns"][to] = {
                    "name": to,
                    "data_type": "timestamp",
                    "nullable": True,
                    "x-valid-to": True,
                    "x-active-record-timestamp": mddict.get("active_record_timestamp"),
                }
                # unique constraint is dropped for C_DLT_ID when used to store
                # SCD2 row hash (only applies to root table)
                hash_ = mddict.get("row_version_column_name", DataItemNormalizer.C_DLT_ID)
                dict_["columns"][hash_] = {
                    "name": hash_,
                    "nullable": False,
                    "x-row-version": True,
                    # duplicate value in row hash column is possible in case
                    # of insert-delete-reinsert pattern
                    "unique": False,
                }

    @staticmethod
    def _create_table_schema(resource_hints: TResourceHints, resource_name: str) -> TTableSchema:
        """Creates table schema from resource hints and resource name."""

        dict_ = cast(Dict[str, Any], resource_hints)
        DltResourceHints._merge_keys(dict_)
        dict_["resource"] = resource_name
        if "write_disposition" in dict_:
            if isinstance(dict_["write_disposition"], str):
                dict_["write_disposition"] = {
                    "disposition": dict_["write_disposition"]
                }  # wrap in dict
            DltResourceHints._merge_write_disposition_dict(dict_)
        return cast(TTableSchema, dict_)

    @staticmethod
    def validate_dynamic_hints(template: TResourceHints) -> None:
        table_name = template.get("name")
        # if any of the hints is a function, then name must be as well.
        if any(
            callable(v) for k, v in template.items() if k not in ["name", *NATURAL_CALLABLES]
        ) and not callable(table_name):
            raise InconsistentTableTemplate(
                f"Table name {table_name} must be a function if any other table hint is a function"
            )

    @staticmethod
    def validate_write_disposition_hint(wd: TTableHintTemplate[TWriteDispositionConfig]) -> None:
        if isinstance(wd, dict) and wd["disposition"] == "merge":
            wd = cast(TMergeDispositionDict, wd)
            if "strategy" in wd and wd["strategy"] not in MERGE_STRATEGIES:
                raise ValueError(
                    f'`{wd["strategy"]}` is not a valid merge strategy. '
                    f"""Allowed values: {', '.join(['"' + s + '"' for s in MERGE_STRATEGIES])}."""
                )

            for ts in ("active_record_timestamp", "boundary_timestamp"):
                if ts == "active_record_timestamp" and wd.get("active_record_timestamp") is None:
                    continue  # None is allowed for active_record_timestamp
                if ts in wd:
                    try:
                        ensure_pendulum_datetime(wd[ts])  # type: ignore[literal-required]
                    except Exception:
                        raise ValueError(
                            f'could not parse `{ts}` value "{wd[ts]}"'  # type: ignore[literal-required]
                        )
