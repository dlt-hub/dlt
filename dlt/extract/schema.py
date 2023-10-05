from copy import copy, deepcopy
from collections.abc import Mapping as C_Mapping
from typing import List, TypedDict, cast, Any

from dlt.common.schema.utils import DEFAULT_WRITE_DISPOSITION, merge_columns, new_column, new_table
from dlt.common.schema.typing import TColumnNames, TColumnProp, TColumnSchema, TPartialTableSchema, TTableSchemaColumns, TWriteDisposition, TAnySchemaColumns
from dlt.common.typing import TDataItem
from dlt.common.utils import update_dict_nested
from dlt.common.validation import validate_dict_ignoring_xkeys

from dlt.extract.incremental import Incremental
from dlt.extract.typing import TFunHintTemplate, TTableHintTemplate, ValidateItem
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints, InconsistentTableTemplate, TableNameMissing
from dlt.extract.utils import ensure_table_schema_columns, ensure_table_schema_columns_hint
from dlt.extract.validation import get_column_validator


class TTableSchemaTemplate(TypedDict, total=False):
    name: TTableHintTemplate[str]
    # description: TTableHintTemplate[str]
    write_disposition: TTableHintTemplate[TWriteDisposition]
    # table_sealed: Optional[bool]
    parent: TTableHintTemplate[str]
    columns: TTableHintTemplate[TTableSchemaColumns]
    primary_key: TTableHintTemplate[TColumnNames]
    merge_key: TTableHintTemplate[TColumnNames]
    incremental: Incremental[Any]
    validator: ValidateItem


class DltResourceSchema:
    def __init__(self, table_schema_template: TTableSchemaTemplate = None):
        self.__qualname__ = self.__name__ = self.name
        self._table_name_hint_fun: TFunHintTemplate[str] = None
        self._table_has_other_dynamic_hints: bool = False
        self._table_schema_template: TTableSchemaTemplate = None
        if table_schema_template:
            self.set_template(table_schema_template)

    @property
    def name(self) -> str:
        pass

    @property
    def table_name(self) -> TTableHintTemplate[str]:
        """Get table name to which resource loads data. May return a callable."""
        if self._table_name_hint_fun:
            return self._table_name_hint_fun
        # get table name or default name
        return self._table_schema_template.get("name") or self.name if self._table_schema_template else self.name

    @table_name.setter
    def table_name(self, value: TTableHintTemplate[str]) -> None:
        self.apply_hints(table_name=value)

    @property
    def write_disposition(self) -> TTableHintTemplate[TWriteDisposition]:
        if self._table_schema_template is None or self._table_schema_template.get("write_disposition") is None:
            return DEFAULT_WRITE_DISPOSITION
        return self._table_schema_template.get("write_disposition")

    @write_disposition.setter
    def write_disposition(self, value: TTableHintTemplate[TWriteDisposition]) -> None:
        self.apply_hints(write_disposition=value)

    @property
    def columns(self) -> TTableHintTemplate[TTableSchemaColumns]:
        """Gets columns schema that can be modified in place"""
        if self._table_schema_template is None:
            return None
        return self._table_schema_template.get("columns")

    def compute_table_schema(self, item: TDataItem =  None) -> TPartialTableSchema:
        """Computes the table schema based on hints and column definitions passed during resource creation. `item` parameter is used to resolve table hints based on data"""
        if not self._table_schema_template:
            return new_table(self.name, resource=self.name)

        # resolve a copy of a held template
        table_template = copy(self._table_schema_template)
        if "name" not in table_template:
            table_template["name"] = self.name
        table_template["columns"] = copy(self._table_schema_template["columns"])

        # if table template present and has dynamic hints, the data item must be provided
        if self._table_name_hint_fun and item is None:
            raise DataItemRequiredForDynamicTableHints(self.name)
        # resolve
        resolved_template: TTableSchemaTemplate = {k: self._resolve_hint(item, v) for k, v in table_template.items()}  # type: ignore
        resolved_template.pop("incremental", None)
        resolved_template.pop("validator", None)
        table_schema = self._merge_keys(resolved_template)
        table_schema["resource"] = self.name
        validate_dict_ignoring_xkeys(
            spec=TPartialTableSchema,
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
        incremental: Incremental[Any] = None
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
        if not self._table_schema_template:
            # if there's no template yet, create and set new one
            t = self.new_table_template(table_name, parent_table_name, write_disposition, columns, primary_key, merge_key)
        else:
            # set single hints
            t = deepcopy(self._table_schema_template)
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
                t['validator'] = get_column_validator(columns)
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

        # set properties that cannot be passed to new_table_template
        t["incremental"] = incremental
        self.set_template(t)

    def set_template(self, table_schema_template: TTableSchemaTemplate) -> None:
        DltResourceSchema.validate_dynamic_hints(table_schema_template)
        # if "name" is callable in the template then the table schema requires actual data item to be inferred
        name_hint = table_schema_template.get("name")
        if callable(name_hint):
            self._table_name_hint_fun = name_hint
        else:
            self._table_name_hint_fun = None
        # check if any other hints in the table template should be inferred from data
        self._table_has_other_dynamic_hints = any(callable(v) for k, v in table_schema_template.items() if k != "name")
        self._table_schema_template = table_schema_template

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
    def _merge_keys(t_: TTableSchemaTemplate) -> TPartialTableSchema:
        """Merges resolved keys into columns"""
        partial = cast(TPartialTableSchema, t_)
        # assert not callable(t_["merge_key"])
        # assert not callable(t_["primary_key"])
        if "primary_key" in t_:
            DltResourceSchema._merge_key("primary_key", t_.pop("primary_key"), partial)  # type: ignore
        if "merge_key" in t_:
            DltResourceSchema._merge_key("merge_key", t_.pop("merge_key"), partial)  # type: ignore

        return partial

    @staticmethod
    def new_table_template(
        table_name: TTableHintTemplate[str],
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TAnySchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnNames] = None,
        merge_key: TTableHintTemplate[TColumnNames] = None,
        ) -> TTableSchemaTemplate:
        if columns is not None:
            validator = get_column_validator(columns)
            columns = ensure_table_schema_columns_hint(columns)
            if not callable(columns):
                columns = columns.values()  # type: ignore
        else:
            validator = None
        # create a table schema template where hints can be functions taking TDataItem
        new_template: TTableSchemaTemplate = new_table(
            table_name, parent_table_name, write_disposition=write_disposition, columns=columns  # type: ignore
        )
        if not table_name:
            new_template.pop("name")
        # always remove resource
        new_template.pop("resource", None)  # type: ignore
        if primary_key:
            new_template["primary_key"] = primary_key
        if merge_key:
            new_template["merge_key"] = merge_key
        if validator:
            new_template["validator"] = validator
        DltResourceSchema.validate_dynamic_hints(new_template)
        return new_template

    @staticmethod
    def validate_dynamic_hints(template: TTableSchemaTemplate) -> None:
        table_name = template.get("name")
        # if any of the hints is a function then name must be as well
        if any(callable(v) for k, v in template.items() if k not in ["name", "incremental", "validator"]) and not callable(table_name):
            raise InconsistentTableTemplate(f"Table name {table_name} must be a function if any other table hint is a function")
