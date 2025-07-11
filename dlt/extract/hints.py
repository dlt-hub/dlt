from typing import (
    Tuple,
    cast,
    Any,
    Optional,
    Dict,
    Sequence,
    Mapping,
    List,
    NamedTuple,
)
from typing_extensions import Self

from dlt.common import logger
from dlt.common.schema.typing import (
    C_DLT_ID,
    TColumnProp,
    TFileFormat,
    TPartialTableSchema,
    TTableSchema,
    TTableSchemaColumns,
    TWriteDispositionConfig,
    TMergeDispositionDict,
    TScd2StrategyDict,
    TAnySchemaColumns,
    TTableFormat,
    TSchemaContract,
    DEFAULT_VALIDITY_COLUMN_NAMES,
    MERGE_STRATEGIES,
    TTableReferenceParam,
)
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.common.typing import TTableNames, TypedDict, Unpack
from dlt.common.schema.utils import (
    DEFAULT_WRITE_DISPOSITION,
    is_nested_table,
    may_be_nested,
    merge_column,
    merge_columns,
    migrate_complex_types,
    new_column,
    new_table,
    merge_table,
)
from dlt.common.typing import TAny, TDataItem, TColumnNames
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.utils import clone_dict_nested
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.validation import validate_dict_ignoring_xkeys
from dlt.extract.exceptions import (
    DataItemRequiredForDynamicTableHints,
    InconsistentTableTemplate,
)
from dlt.extract.incremental import Incremental, TIncrementalConfig
from dlt.extract.items import TFunHintTemplate, TTableHintTemplate, TableNameMeta
from dlt.extract.items_transform import ValidateItem
from dlt.extract.utils import ensure_table_schema_columns, ensure_table_schema_columns_hint
from dlt.extract.validation import create_item_validator

import sqlglot


DLT_HINTS_METADATA_KEY = "dlt_resource_hints"


class TResourceNestedHints(TypedDict, total=False):
    # used to force a parent for rare cases where normalizer skips intermediate tables that
    # do not receive data
    parent_table_name: Optional[TTableHintTemplate[str]]
    write_disposition: Optional[TTableHintTemplate[TWriteDispositionConfig]]
    primary_key: Optional[TTableHintTemplate[TColumnNames]]
    columns: Optional[TTableHintTemplate[TAnySchemaColumns]]
    schema_contract: Optional[TTableHintTemplate[TSchemaContract]]
    table_format: Optional[TTableHintTemplate[TTableFormat]]
    file_format: Optional[TTableHintTemplate[TFileFormat]]
    merge_key: Optional[TTableHintTemplate[TColumnNames]]
    references: Optional[TTableHintTemplate[TTableReferenceParam]]


class TResourceHintsBase(TResourceNestedHints, total=False):
    table_name: Optional[TTableHintTemplate[str]]
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]]


class TResourceHints(TResourceHintsBase, total=False):
    # description: TTableHintTemplate[str]
    incremental: Optional[Incremental[Any]]
    validator: ValidateItem
    original_columns: Optional[TTableHintTemplate[TAnySchemaColumns]]
    additional_table_hints: Optional[Dict[str, TTableHintTemplate[Any]]]


class HintsMeta:
    __slots__ = ("hints", "create_table_variant")

    def __init__(
        self,
        hints: TResourceHints,
        create_table_variant: bool,
    ) -> None:
        self.hints = hints
        self.create_table_variant = create_table_variant


class SqlModel:
    """
    A SqlModel is a named tuple that contains a query and a dialect.
    It is used to represent a SQL query and the dialect to use for parsing it.
    """

    __slots__ = ("_query", "_dialect")

    def __init__(self, query: str, dialect: Optional[str] = None) -> None:
        self._query = query
        self._dialect = dialect

    def to_sql(self) -> str:
        return self._query

    def query_dialect(self) -> str:
        return self._dialect

    @classmethod
    def from_query_string(cls, query: str, dialect: Optional[str] = None) -> "SqlModel":
        """
        Creates a SqlModel from a raw SQL query string using sqlglot.
        Ensures that the parsed query is an instance of sqlglot.exp.Select.

        Args:
            query (str): The raw SQL query string.
            dialect (Optional[str]): The SQL dialect to use for parsing.

        Returns:
            SqlModel: An instance of SqlModel with the normalized query and dialect.

        Raises:
            ValueError: If the parsed query is not an instance of sqlglot.exp.Select.
        """

        parsed_query = sqlglot.parse_one(query, read=dialect)

        # Ensure the parsed query is a SELECT statement
        if not isinstance(parsed_query, sqlglot.exp.Select):
            raise ValueError("Only SELECT statements are allowed to create a `SqlModel`.")

        normalized_query = parsed_query.sql(dialect=dialect)
        return cls(query=normalized_query, dialect=dialect)


NATURAL_CALLABLES = ["incremental", "validator", "original_columns"]


def table_schema_to_hints(table: TTableSchema) -> TResourceHints:
    """Converts table schema into resource hints in place. Does not attempt to extract primary keys,
    additional table properties etc. into respective TResourceHints fields. Suitable only
    to be used for a result of utils.new_table.
    """
    template: TResourceHints = table  # type: ignore[assignment]
    # parent and name are different in table schema vs. resource hints so we need to rename
    template["table_name"] = table.pop("name")
    if "parent" in table:
        template["parent_table_name"] = table.pop("parent")
    # always remove resource
    table.pop("resource", None)
    return template


def make_nested_hints(**hints: Unpack[TResourceNestedHints]) -> TResourceNestedHints:
    return make_hints(**hints)


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
    additional_table_hints: Optional[Dict[str, TTableHintTemplate[Any]]] = None,
    references: TTableHintTemplate[TTableReferenceParam] = None,
    incremental: TIncrementalConfig = None,
    nested_hints: Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]] = None,
) -> TResourceHints:
    """A convenience function to create resource hints. Accepts both static and dynamic hints based on data.

    This method accepts the same table hints arguments as `dlt.resource` decorator.
    """

    validator, schema_contract = create_item_validator(columns, schema_contract)
    # create a table schema template where hints can be functions taking TDataItem
    new_template = table_schema_to_hints(
        new_table(
            table_name,  # type: ignore
            parent_table_name,  # type: ignore
            write_disposition=write_disposition,  # type: ignore
            schema_contract=schema_contract,  # type: ignore
            table_format=table_format,  # type: ignore
            file_format=file_format,  # type: ignore
            references=references,  # type: ignore
        )
    )
    if not table_name:
        del new_template["table_name"]
    if not write_disposition and "write_disposition" in new_template:
        new_template.pop("write_disposition")
    # remember original columns and set template columns
    if columns is not None:
        new_template["original_columns"] = columns
        new_template["columns"] = ensure_table_schema_columns_hint(columns)

    if primary_key is not None:
        new_template["primary_key"] = primary_key
    if merge_key is not None:
        new_template["merge_key"] = merge_key
    if validator:
        new_template["validator"] = validator
    if nested_hints is not None:
        new_template["nested_hints"] = nested_hints
    if additional_table_hints is not None:
        new_template["additional_table_hints"] = additional_table_hints
    DltResourceHints.validate_dynamic_hints(new_template)
    if incremental is not None:  # TODO: Validate
        new_template["incremental"] = Incremental.ensure_instance(incremental)
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
        return self._hints.get("table_name") or self.name if self._hints else self.name

    @table_name.setter
    def table_name(self, value: TTableHintTemplate[str]) -> None:
        self.apply_hints(table_name=value)

    @property
    def has_dynamic_table_name(self) -> bool:
        """Tells the extractor wether computed table name may change based on invididual data items"""
        return self._table_name_hint_fun is not None

    @property
    def has_other_dynamic_hints(self) -> bool:
        """Tells the extractor wether computed hints may change based on invididual data items"""
        return self._table_has_other_dynamic_hints

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
        return None if self._hints is None else self._hints.get("columns")  # type: ignore[return-value]

    @property
    def nested_hints(self) -> Optional[TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]]]:
        return (
            None
            if self._hints is None or self._hints.get("nested_hints") is None
            else self._hints["nested_hints"]
        )

    @property
    def schema_contract(self) -> TTableHintTemplate[TSchemaContract]:
        return None if self._hints is None else self._hints.get("schema_contract")

    @property
    def table_format(self) -> TTableHintTemplate[TTableFormat]:
        return None if self._hints is None else self._hints.get("table_format")

    @property
    def parent_table_name(self) -> TTableHintTemplate[str]:
        return None if self._hints is None else self._hints.get("parent_table_name")

    def compute_table_schema(self, item: TDataItem = None, meta: Any = None) -> TTableSchema:
        """Computes the table schema based on hints and column definitions passed during resource creation.
        `item` parameter is used to resolve table hints based on data.
        `meta` parameter is taken from Pipe and may further specify table name if variant is to be used
        """
        if isinstance(meta, TableNameMeta):
            # look for variant
            default_table_name = meta.table_name
            root_table_template = self._hints_variants.get(default_table_name, self._hints)
        else:
            default_table_name = self.name
            root_table_template = self._hints

        if not root_table_template:
            return new_table(default_table_name, resource=self.name)

        # resolve a copy of a held template
        root_table_template = self._clone_hints(root_table_template)
        if "table_name" not in root_table_template:
            root_table_template["table_name"] = default_table_name

        # if table template present and has dynamic hints, the data item must be provided.
        if self._table_name_hint_fun and item is None:
            raise DataItemRequiredForDynamicTableHints(self.name)

        # resolve
        resolved_template: TResourceHints = {
            k: self._resolve_hint(item, v)
            for k, v in root_table_template.items()
            if k not in NATURAL_CALLABLES
        }  # type: ignore

        if "incremental" in root_table_template:
            incremental = root_table_template["incremental"]
            if isinstance(incremental, Incremental) and incremental is not Incremental.EMPTY:
                resolved_template["incremental"] = incremental

        table_schema = self._hints_to_table_schema(resolved_template)
        if not is_nested_table(table_schema):
            table_schema["resource"] = self.name
        migrate_complex_types(table_schema, warn=True)
        validate_dict_ignoring_xkeys(
            spec=TTableSchema,
            doc=table_schema,
            path=f"new_table/{self.name}",
        )

        return table_schema

    def compute_nested_table_schemas(
        self,
        root_table_name: str,
        naming: NamingConvention,
        item: TDataItem = None,
        meta: Any = None,
    ) -> List[TTableSchema]:
        """Compute the table schema based on the current and all nested hints.
        Nested hints are resolved recursively.
        """
        # allow for nested hints to be a part of a table variant
        if isinstance(meta, TableNameMeta):
            root_table_template = self._hints_variants.get(root_table_name, self._hints)
        else:
            root_table_template = self._hints
        # no nested hints if no table template
        if root_table_template is None:
            return []

        nested_hints = root_table_template.get("nested_hints")
        if not nested_hints:
            return []
        # resolve dynamic hint with actual data item
        nested_hints = self._resolve_hint(item, nested_hints)

        nested_table_schemas = []
        # make sure shorter paths go first
        sorted_items = sorted(
            nested_hints.items(), key=lambda item: len(item[0]) if isinstance(item[0], tuple) else 1
        )
        for sub_path, hints in sorted_items:
            # both str and sequence is supported
            if isinstance(sub_path, str):
                full_path: Tuple[str, ...] = (root_table_name, sub_path)
            else:
                full_path = (root_table_name, *sub_path)
            table_name = naming.shorten_fragments(*full_path)
            nested_table_template = self._clone_hints(hints)

            resolved_template: TResourceHints = {
                k: self._resolve_hint(item, v)
                for k, v in nested_table_template.items()
                if k not in NATURAL_CALLABLES
            }  # type: ignore

            nested_table_schema = self._hints_to_table_schema(resolved_template)
            # table_name is not a part of TResourceNestedHints but in the future we may allow those tables
            # to be renamed. that will require a bigger refactor in the relational normalizer
            nested_table_schema["name"] = table_name
            migrate_complex_types(nested_table_schema, warn=True)

            # in very rare cases parent may be explicitly defined for a nested table
            if "parent" not in nested_table_schema and may_be_nested(nested_table_schema):
                # we add default parent linking if table is eligible
                nested_table_schema["parent"] = naming.shorten_fragments(*full_path[:-1])

            if not is_nested_table(nested_table_schema):
                nested_table_schema["resource"] = self.name
            elif nested_write_disposition := nested_table_schema.get("write_disposition"):
                write_disposition = root_table_template.get("write_disposition")
                # TODO: write_disposition may also be a callable, in that case generate warning as well
                if nested_write_disposition != write_disposition:
                    logger.warning(
                        f"You defined '{nested_write_disposition}' write disposition on a nested"
                        f" table {table_name} while root table {root_table_name} has"
                        f" {write_disposition} write disposition. This may lead to data"
                        " inconsistency across root and nested tables. Consider setting primary or"
                        " merge key on a nested table to break nesting chain and convert it into a"
                        "  top level table."
                    )

            validate_dict_ignoring_xkeys(
                spec=TTableSchema,
                doc=nested_table_schema,
                path=f"new_table/{table_name}",
            )
            nested_table_schemas.append(nested_table_schema)
        # TODO: restore this implicit table creation but initially I'd push it to
        # the user to create intermediate tables
        # NOTE we insert missing parents at the beginning to ensure that `schema.update_table()` is later called
        # in an an order that respect parent-child relationships
        # for table_name in tables_to_create:
        #     # TODO need to clean up the path from args -> hints -> schema
        #     placeholder_table_template = new_table(table_name=table_name)
        #     placeholder_table_schema = self._create_table_schema(placeholder_table_template, self.name)
        #     nested_table_schemas.insert(0, placeholder_table_schema)

        return nested_table_schemas

    def apply_hints(
        self,
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDispositionConfig] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        incremental: TIncrementalConfig = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        additional_table_hints: Optional[Dict[str, TTableHintTemplate[Any]]] = None,
        table_format: TTableHintTemplate[TTableFormat] = None,
        file_format: TTableHintTemplate[TFileFormat] = None,
        references: TTableHintTemplate[TTableReferenceParam] = None,
        create_table_variant: bool = False,
        nested_hints: TTableHintTemplate[Dict[TTableNames, TResourceNestedHints]] = None,
    ) -> Self:
        """Creates or modifies existing table schema by setting provided hints. Accepts both static and dynamic hints based on data.

        If `create_table_variant` is specified, the `table_name` must be a string and hints will be used to create a separate set of hints
        for a particular `table_name`. Such hints may be retrieved via compute_table_schema(meta=TableNameMeta(table_name)).
        Table variant hints may not contain dynamic hints.

        This method accepts the same table hints arguments as `dlt.resource` decorator with the following additions.
        Skip the argument or pass None to leave the existing hint.
        Pass empty value (for a particular type i.e. "" for a string) to remove a hint.

        Args:
            table_name (TTableHintTemplate[str]): name of the table which resource will generate

            parent_table_parent (str, optional): A name of parent table if you want the resource to generate nested table. Please note that if you use merge, you must define `root_key` columns explicitly

            incremental (Incremental, optional): Enables the incremental loading for a resource.

        Please note that for efficient incremental loading, the resource must be aware of the Incremental by accepting it as one if its arguments and then using are to skip already loaded data.
        In non-aware resources, `dlt` will filter out the loaded values, however, the resource will yield all the values again.

        Returns: self for chaining
        """
        if create_table_variant:
            if not isinstance(table_name, str):
                raise ValueError(
                    "Please provide `str` table name if you want to create a table variant of hints"
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
                table_name=table_name,
                parent_table_name=parent_table_name,
                write_disposition=write_disposition or default_wd,
                columns=columns,
                primary_key=primary_key,
                merge_key=merge_key,
                schema_contract=schema_contract,
                table_format=table_format,
                file_format=file_format,
                references=references,
                nested_hints=nested_hints,
            )
        else:
            t = self._clone_hints(t)
            if table_name is not None:
                if table_name:
                    t["table_name"] = table_name
                else:
                    t.pop("table_name", None)
            if parent_table_name is not None:
                if parent_table_name:
                    t["parent_table_name"] = parent_table_name
                else:
                    t.pop("parent_table_name", None)
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
                    assert isinstance(t["columns"], dict)
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
                if additional_table_hints:
                    if t.get("additional_table_hints") is not None:
                        for k, v in additional_table_hints.items():
                            t["additional_table_hints"][k] = v
                    else:
                        t["additional_table_hints"] = additional_table_hints
                else:
                    t.pop("additional_table_hints", None)

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
            if references is not None:
                if callable(references) or callable(t.get("references")):
                    t["references"] = references
                else:
                    # Replace existing refs for same table
                    new_references = t.get("references") or []
                    ref_dict = {r["referenced_table"]: r for r in new_references}  # type: ignore[union-attr]
                    for ref in references:
                        ref_dict[ref["referenced_table"]] = ref
                    t["references"] = list(ref_dict.values())
            # NOTE: here we just replace nested hints fully (or drop them). what we could do instead
            #   is to re-use the code above (ie special handling of references or columns) for each nested hint
            #   for now we are good
            if nested_hints is not None:
                if nested_hints:
                    t["nested_hints"] = nested_hints
                else:
                    t.pop("nested_hints", None)

        # set properties that can't be passed to make_hints
        if incremental is not None:
            t["incremental"] = Incremental.ensure_instance(incremental)

        self._set_hints(t, create_table_variant)
        return self

    def _set_hints(
        self, hints_template: TResourceHints, create_table_variant: bool = False
    ) -> None:
        DltResourceHints.validate_dynamic_hints(hints_template)
        DltResourceHints.validate_write_disposition_hint(hints_template)
        DltResourceHints.validate_reference_hint(hints_template)
        if create_table_variant:
            # for table variants, table name must be a str
            table_name: str = hints_template["table_name"]  # type: ignore[assignment]
            # incremental cannot be specified in variant
            if hints_template.get("incremental"):
                raise InconsistentTableTemplate(
                    f"You can specify `incremental` only for the resource `{self.name}` hints, not"
                    f" in table `{table_name}` variant-"
                )
            if hints_template.get("validator"):
                logger.warning(
                    f"A data item validator was created from column schema in `{self.name}` for a"
                    f" table `{table_name}` variant. Currently such validator is ignored."
                )
            # dynamic hints will be ignored
            for name, hint in hints_template.items():
                if callable(hint) and name not in NATURAL_CALLABLES:
                    raise InconsistentTableTemplate(
                        f"Table `{table_name}` variant hint is resource `{self.name}` can't have"
                        f" dynamic hint but `{name}` does."
                    )
            self._hints_variants[table_name] = hints_template
        else:
            # if "name" is callable in the template, then the table schema requires data item to be inferred.
            name_hint = hints_template.get("table_name")
            self._table_name_hint_fun = name_hint if callable(name_hint) else None
            # check if any other hints in the table template should be inferred from data.
            self._table_has_other_dynamic_hints = any(
                callable(v) for k, v in hints_template.items() if k != "table_name"
            )
            self._hints = hints_template

    def merge_hints(
        self, hints_template: TResourceHints, create_table_variant: bool = False
    ) -> None:
        self.apply_hints(
            table_name=hints_template.get("table_name"),
            parent_table_name=hints_template.get("parent_table_name"),
            write_disposition=hints_template.get("write_disposition"),
            columns=hints_template.get("original_columns"),
            primary_key=hints_template.get("primary_key"),
            merge_key=hints_template.get("merge_key"),
            incremental=hints_template.get("incremental"),
            schema_contract=hints_template.get("schema_contract"),
            table_format=hints_template.get("table_format"),
            file_format=hints_template.get("file_format"),
            additional_table_hints=hints_template.get("additional_table_hints"),
            references=hints_template.get("references"),
            create_table_variant=create_table_variant,
            nested_hints=hints_template.get("nested_hints"),
        )

    @staticmethod
    def _clone_hints(hints_template: TAny) -> TAny:
        if hints_template is None:
            return None
        # creates a deep copy of dict structure without actually copying the objects
        return clone_dict_nested(hints_template)

    @staticmethod
    def _resolve_hint(item: TDataItem, hint: TTableHintTemplate[TAny]) -> TAny:
        """Calls each dynamic hint passing a data item"""
        return hint(item) if callable(hint) else hint  # type: ignore[no-any-return]

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
    def _merge_keys(dict_: TResourceHints) -> None:
        """Merges primary and merge keys into columns in place."""

        if "primary_key" in dict_:
            DltResourceHints._merge_key("primary_key", dict_.pop("primary_key"), dict_)  # type: ignore
        if "merge_key" in dict_:
            DltResourceHints._merge_key("merge_key", dict_.pop("merge_key"), dict_)  # type: ignore

    @staticmethod
    def _merge_write_disposition_dict(dict_: Dict[str, Any]) -> None:
        """Merges write disposition dictionary into write disposition shorthand and x-hints in place."""

        write_disposition = dict_["write_disposition"]["disposition"]
        if write_disposition == "merge":
            DltResourceHints._merge_merge_disposition_dict(dict_)
        # reduce merge disposition from dict to shorthand
        dict_["write_disposition"] = write_disposition

    @staticmethod
    def _merge_merge_disposition_dict(dict_: Dict[str, Any]) -> None:
        """Merges merge disposition dict into x-hints in place."""

        md_dict: TMergeDispositionDict = dict_.pop("write_disposition")
        if merge_strategy := md_dict.get("strategy"):
            dict_["x-merge-strategy"] = merge_strategy

        if deduplicated := md_dict.get("deduplicated"):
            dict_["x-stage-data-deduplicated"] = deduplicated

        if merge_strategy == "scd2":
            md_dict = cast(TScd2StrategyDict, md_dict)
            if "boundary_timestamp" in md_dict:
                dict_["x-boundary-timestamp"] = md_dict["boundary_timestamp"]
            if md_dict.get("validity_column_names") is None:
                from_, to = DEFAULT_VALIDITY_COLUMN_NAMES
            else:
                from_, to = md_dict["validity_column_names"]
            dict_["columns"][from_] = {
                "name": from_,
                "data_type": "timestamp",
                "nullable": True,  # validity columns are empty when first loaded into staging table
                "x-valid-from": True,
            }
            dict_["columns"][to] = {
                "name": to,
                "data_type": "timestamp",
                "nullable": True,
                "x-valid-to": True,
                "x-active-record-timestamp": md_dict.get("active_record_timestamp"),
            }
            # unique constraint is dropped for C_DLT_ID when used to store
            # SCD2 row hash (only applies to root table)
            hash_ = md_dict.get("row_version_column_name", C_DLT_ID)
            dict_["columns"][hash_] = {
                "name": hash_,
                "nullable": False,
                "x-row-version": True,
                # duplicate value in row hash column is possible in case
                # of insert-delete-reinsert pattern
                "unique": False,
                "row_key": False,
            }

    @staticmethod
    def _merge_incremental_column_hint(dict_: Dict[str, Any]) -> None:
        incremental = dict_.pop("incremental")
        if incremental is None:
            return
        col_name = incremental.get_cursor_column_name()
        if not col_name:
            # cursor cannot resolve to a single column, no hint added
            return
        incremental_col = dict_["columns"].get(col_name)
        if not incremental_col:
            incremental_col = {"name": col_name}

        incremental_col["incremental"] = True
        dict_["columns"][col_name] = incremental_col

    @staticmethod
    def _hints_to_table_schema(resolved_hints: TResourceHints) -> TTableSchema:
        """Creates table schema from resource hints in place. Resource hints must be resolved
        (cannot contain callables).
        """
        resolved_hints.pop("nested_hints", None)
        resolved_hints["columns"] = resolved_hints.get("columns", {})
        DltResourceHints._merge_keys(resolved_hints)
        if "write_disposition" in resolved_hints:
            if isinstance(resolved_hints["write_disposition"], str):
                resolved_hints["write_disposition"] = {
                    "disposition": resolved_hints["write_disposition"]
                }  # wrap in dict
            DltResourceHints._merge_write_disposition_dict(resolved_hints)  # type: ignore[arg-type]
        if "incremental" in resolved_hints:
            DltResourceHints._merge_incremental_column_hint(resolved_hints)  # type: ignore[arg-type]
        table = cast(TTableSchema, resolved_hints)
        # rename two props that differ
        if "table_name" in resolved_hints:
            table["name"] = resolved_hints.pop("table_name")  # type: ignore[typeddict-item]
        if "parent_table_name" in resolved_hints:
            table["parent"] = resolved_hints.pop("parent_table_name")  # type: ignore[typeddict-item]
        # apply table hints
        if additional_table_hints := resolved_hints.get("additional_table_hints"):
            for k, v in additional_table_hints.items():
                table[k] = v  # type: ignore[literal-required]
        resolved_hints.pop("additional_table_hints", None)
        return table

    @staticmethod
    def validate_dynamic_hints(template: TResourceHints) -> None:
        table_name = template.get("table_name")
        # if any of the hints is a function, then name must be as well.
        if any(
            callable(v) for k, v in template.items() if k not in ["table_name", *NATURAL_CALLABLES]
        ) and not callable(table_name):
            raise InconsistentTableTemplate(
                f"Table name `{table_name}` must be a function if any other table hint is a"
                " function"
            )

    @staticmethod
    def validate_write_disposition_hint(template: TResourceHints) -> None:
        wd = template.get("write_disposition")
        if isinstance(wd, dict) and wd["disposition"] == "merge":
            wd = cast(TMergeDispositionDict, wd)
            if "strategy" in wd and wd["strategy"] not in MERGE_STRATEGIES:
                raise ValueErrorWithKnownValues(
                    "write_disposition['strategy']", wd["strategy"], MERGE_STRATEGIES
                )

            if wd.get("strategy") == "scd2":
                wd = cast(TScd2StrategyDict, wd)
                for ts in ("active_record_timestamp", "boundary_timestamp"):
                    if (
                        ts == "active_record_timestamp"
                        and wd.get("active_record_timestamp") is None
                    ):
                        continue  # None is allowed for active_record_timestamp
                    if ts in wd:
                        try:
                            ensure_pendulum_datetime(wd[ts])  # type: ignore[literal-required]
                        except Exception:
                            raise ValueError(
                                f"could not parse `{ts}` value `{wd[ts]}`"  # type: ignore[literal-required]
                            )

    @staticmethod
    def validate_reference_hint(template: TResourceHints) -> None:
        ref = template.get("reference")
        if ref is None:
            return
        if not isinstance(ref, Sequence):
            raise ValueError("Reference hint must be a sequence of table references.")
        for r in ref:
            if not isinstance(r, Mapping):
                raise ValueError("Table reference must be a dictionary.")
            columns = r.get("columns")
            referenced_columns = r.get("referenced_columns")
            table = r.get("referenced_table")
            if not table:
                raise ValueError("Referenced table must be specified.")
            if not columns or not referenced_columns:
                raise ValueError("Both columns and referenced_columns must be specified.")
            if len(columns) != len(referenced_columns):
                raise ValueError("Columns and referenced_columns must have the same length.")
