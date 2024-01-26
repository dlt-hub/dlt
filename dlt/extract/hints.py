from copy import copy, deepcopy
from typing import List, TypedDict, cast, Any

from dlt.common.schema.utils import DEFAULT_WRITE_DISPOSITION, merge_columns, new_column, new_table
from dlt.common.schema.typing import (
    TColumnNames,
    TColumnProp,
    TColumnSchema,
    TPartialTableSchema,
    TTableSchema,
    TTableSchemaColumns,
    TWriteDisposition,
    TAnySchemaColumns,
    TTableFormat,
    TSchemaContract,
)
from dlt.common.typing import TDataItem
from dlt.common.utils import update_dict_nested
from dlt.common.validation import validate_dict_ignoring_xkeys

from dlt.extract.incremental import Incremental
from dlt.extract.typing import TFunHintTemplate, TTableHintTemplate, ValidateItem
from dlt.extract.exceptions import (
    DataItemRequiredForDynamicTableHints,
    InconsistentTableTemplate,
    TableNameMissing,
)
from dlt.extract.utils import ensure_table_schema_columns, ensure_table_schema_columns_hint
from dlt.extract.validation import create_item_validator


class TResourceHints(TypedDict, total=False):
    name: TTableHintTemplate[str]
    # description: TTableHintTemplate[str]
    write_disposition: TTableHintTemplate[TWriteDisposition]
    # table_sealed: Optional[bool]
    parent: TTableHintTemplate[str]
    columns: TTableHintTemplate[TTableSchemaColumns]
    primary_key: TTableHintTemplate[TColumnNames]
    merge_key: TTableHintTemplate[TColumnNames]
    incremental: Incremental[Any]
    schema_contract: TTableHintTemplate[TSchemaContract]
    validator: ValidateItem
    original_columns: TTableHintTemplate[TAnySchemaColumns]


class DltResourceHints:
    def __init__(self, table_schema_template: TResourceHints = None):
        self.__qualname__ = self.__name__ = self.name
        self._table_name_hint_fun: TFunHintTemplate[str] = None
        self._table_has_other_dynamic_hints: bool = False
        self._hints: TResourceHints = None
        if table_schema_template:
            self.set_hints(table_schema_template)

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
    def write_disposition(self) -> TTableHintTemplate[TWriteDisposition]:
        if self._hints is None or self._hints.get("write_disposition") is None:
            return DEFAULT_WRITE_DISPOSITION
        return self._hints.get("write_disposition")

    @write_disposition.setter
    def write_disposition(self, value: TTableHintTemplate[TWriteDisposition]) -> None:
        self.apply_hints(write_disposition=value)

    @property
    def columns(self) -> TTableHintTemplate[TTableSchemaColumns]:
        """Gets columns schema that can be modified in place"""
        if self._hints is None:
            return None
        return self._hints.get("columns")

    @property
    def schema_contract(self) -> TTableHintTemplate[TSchemaContract]:
        return self._hints.get("schema_contract")

    def compute_table_schema(self, item: TDataItem = None) -> TTableSchema:
        """Computes the table schema based on hints and column definitions passed during resource creation. `item` parameter is used to resolve table hints based on data"""
        if not self._hints:
            return new_table(self.name, resource=self.name)

        # resolve a copy of a held template
        table_template = copy(self._hints)
        if "name" not in table_template:
            table_template["name"] = self.name
        table_template["columns"] = copy(self._hints["columns"])

        # if table template present and has dynamic hints, the data item must be provided
        if self._table_name_hint_fun and item is None:
            raise DataItemRequiredForDynamicTableHints(self.name)
        # resolve
        resolved_template: TResourceHints = {k: self._resolve_hint(item, v) for k, v in table_template.items() if k not in ["incremental", "validator", "original_columns"]}  # type: ignore
        table_schema = self._merge_keys(resolved_template)
        table_schema["resource"] = self.name
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
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        incremental: Incremental[Any] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
    ) -> None:
        """Creates or modifies existing table schema by setting provided hints. Accepts both static and dynamic hints based on data.

        This method accepts the same table hints arguments as `dlt.resource` decorator with the following additions.
        Skip the argument or pass None to leave the existing hint.
        Pass empty value (for particular type ie "" for a string) to remove hint

        parent_table_name (str, optional): A name of parent table if foreign relation is defined. Please note that if you use merge you must define `root_key` columns explicitly
        incremental (Incremental, optional): Enables the incremental loading for a resource.

        Please note that for efficient incremental loading, the resource must be aware of the Incremental by accepting it as one if its arguments and then using is to skip already loaded data.
        In non-aware resources, `dlt` will filter out the loaded values, however the resource will yield all the values again.
        """
        t = None
        if not self._hints:
            # if there's no template yet, create and set new one
            t = self.new_table_template(
                table_name,
                parent_table_name,
                write_disposition,
                columns,
                primary_key,
                merge_key,
                schema_contract,
            )
        else:
            # set single hints
            t = self._clone_hints(self._hints)
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
                # keep original columns: ie in case it is a Pydantic model
                t["original_columns"] = columns
                # if callable then override existing
                if callable(columns) or callable(t["columns"]):
                    t["columns"] = ensure_table_schema_columns_hint(columns)
                elif columns:
                    # normalize columns
                    columns = ensure_table_schema_columns(columns)
                    # this updates all columns with defaults
                    t["columns"] = update_dict_nested(t["columns"], columns)
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
            # recreate validator if columns definition or contract changed
            if schema_contract is not None or columns is not None:
                t["validator"], schema_contract = create_item_validator(
                    t.get("original_columns"), t.get("schema_contract")
                )
                if schema_contract is not None:
                    t["schema_contract"] = schema_contract

        # set properties that cannot be passed to new_table_template
        if incremental is not None:
            if incremental is Incremental.EMPTY:
                t["incremental"] = None
            else:
                t["incremental"] = incremental
        self.set_hints(t)

    def set_hints(self, hints_template: TResourceHints) -> None:
        DltResourceHints.validate_dynamic_hints(hints_template)
        # if "name" is callable in the template then the table schema requires actual data item to be inferred
        name_hint = hints_template.get("name")
        if callable(name_hint):
            self._table_name_hint_fun = name_hint
        else:
            self._table_name_hint_fun = None
        # check if any other hints in the table template should be inferred from data
        self._table_has_other_dynamic_hints = any(
            callable(v) for k, v in hints_template.items() if k != "name"
        )
        self._hints = hints_template

    @staticmethod
    def _clone_hints(hints_template: TResourceHints) -> TResourceHints:
        t_ = copy(hints_template)
        t_["columns"] = deepcopy(hints_template["columns"])
        if "schema_contract" in hints_template:
            t_["schema_contract"] = deepcopy(hints_template["schema_contract"])
        return t_

    @staticmethod
    def _resolve_hint(item: TDataItem, hint: TTableHintTemplate[Any]) -> Any:
        """Calls each dynamic hint passing a data item"""
        if callable(hint):
            return hint(item)
        else:
            return hint

    @staticmethod
    def _merge_key(hint: TColumnProp, keys: TColumnNames, partial: TPartialTableSchema) -> None:
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            if key in partial["columns"]:
                merge_columns(partial["columns"][key], {hint: True})  # type: ignore
            else:
                partial["columns"][key] = new_column(key, nullable=False)
                partial["columns"][key][hint] = True

    @staticmethod
    def _merge_keys(t_: TResourceHints) -> TPartialTableSchema:
        """Merges resolved keys into columns"""
        partial = cast(TPartialTableSchema, t_)
        # assert not callable(t_["merge_key"])
        # assert not callable(t_["primary_key"])
        if "primary_key" in t_:
            DltResourceHints._merge_key("primary_key", t_.pop("primary_key"), partial)  # type: ignore
        if "merge_key" in t_:
            DltResourceHints._merge_key("merge_key", t_.pop("merge_key"), partial)  # type: ignore

        return partial

    @staticmethod
    def new_table_template(
        table_name: TTableHintTemplate[str],
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        schema_contract: TTableHintTemplate[TSchemaContract] = None,
        table_format: TTableHintTemplate[TTableFormat] = None,
    ) -> TResourceHints:
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
        )
        if not table_name:
            new_template.pop("name")
        # remember original columns
        if columns is not None:
            new_template["original_columns"] = columns
        # always remove resource
        new_template.pop("resource", None)  # type: ignore
        if primary_key:
            new_template["primary_key"] = primary_key
        if merge_key:
            new_template["merge_key"] = merge_key
        if validator:
            new_template["validator"] = validator
        DltResourceHints.validate_dynamic_hints(new_template)
        return new_template

    @staticmethod
    def validate_dynamic_hints(template: TResourceHints) -> None:
        table_name = template.get("name")
        # if any of the hints is a function then name must be as well
        if any(
            callable(v)
            for k, v in template.items()
            if k not in ["name", "incremental", "validator", "original_columns"]
        ) and not callable(table_name):
            raise InconsistentTableTemplate(
                f"Table name {table_name} must be a function if any other table hint is a function"
            )
