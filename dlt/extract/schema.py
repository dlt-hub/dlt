from copy import copy, deepcopy
from collections.abc import Mapping as C_Mapping
from typing import List, TypedDict, cast, Any

from dlt.common.schema.utils import DEFAULT_WRITE_DISPOSITION, merge_columns, new_column, new_table
from dlt.common.schema.typing import TColumnKey, TColumnProp, TColumnSchema, TPartialTableSchema, TTableSchemaColumns, TWriteDisposition
from dlt.common.typing import TDataItem
from dlt.common.validation import validate_dict

from dlt.extract.incremental import Incremental
from dlt.extract.typing import TFunHintTemplate, TTableHintTemplate
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints, InconsistentTableTemplate, TableNameMissing


class TTableSchemaTemplate(TypedDict, total=False):
    name: TTableHintTemplate[str]
    # description: TTableHintTemplate[str]
    write_disposition: TTableHintTemplate[TWriteDisposition]
    # table_sealed: Optional[bool]
    parent: TTableHintTemplate[str]
    columns: TTableHintTemplate[TTableSchemaColumns]
    primary_key: TTableHintTemplate[TColumnKey]
    merge_key: TTableHintTemplate[TColumnKey]
    incremental: Incremental[Any]


class DltResourceSchema:
    def __init__(self, name: str, table_schema_template: TTableSchemaTemplate = None):
        self.__qualname__ = self.__name__ = self._name = name
        self._table_name_hint_fun: TFunHintTemplate[str] = None
        self._table_has_other_dynamic_hints: bool = False
        self._table_schema_template: TTableSchemaTemplate = None
        if table_schema_template:
            self.set_template(table_schema_template)

    @property
    def table_name(self) -> str:
        """Get table name to which resource loads data. Raises in case of table names derived from data."""
        if self._table_name_hint_fun:
            raise DataItemRequiredForDynamicTableHints(self._name)
        return self._table_schema_template["name"] if self._table_schema_template else self._name  # type: ignore

    @property
    def write_disposition(self) -> TWriteDisposition:
        if self._table_schema_template is None or self._table_schema_template.get("write_disposition") is None:
            return DEFAULT_WRITE_DISPOSITION
        w_d = self._table_schema_template.get("write_disposition")
        if callable(w_d):
            raise DataItemRequiredForDynamicTableHints(self._name)
        else:
            return w_d

    def table_schema(self, item: TDataItem =  None) -> TPartialTableSchema:
        """Computes the table schema based on hints and column definitions passed during resource creation. `item` parameter is used to resolve table hints based on data"""
        if not self._table_schema_template:
            return new_table(self._name, resource=self._name)

        # resolve a copy of a held template
        table_template = copy(self._table_schema_template)
        table_template["columns"] = copy(self._table_schema_template["columns"])

        # if table template present and has dynamic hints, the data item must be provided
        if self._table_name_hint_fun and item is None:
            raise DataItemRequiredForDynamicTableHints(self._name)
        # resolve
        resolved_template: TTableSchemaTemplate = {k: self._resolve_hint(item, v) for k, v in table_template.items()}  # type: ignore
        resolved_template.pop("incremental", None)
        table_schema = self._merge_keys(resolved_template)
        table_schema["resource"] = self._name
        validate_dict(TPartialTableSchema, table_schema, f"new_table/{self._name}")
        return table_schema

    def apply_hints(
        self,
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TTableSchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnKey] = None,
        merge_key: TTableHintTemplate[TColumnKey] = None,
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
                t["columns"] = columns
            if primary_key is not None:
                t["primary_key"] = primary_key
            if merge_key is not None:
                t["merge_key"] = merge_key
            t["incremental"] = incremental
        self.set_template(t)

    def set_template(self, table_schema_template: TTableSchemaTemplate) -> None:
        # if "name" is callable in the template then the table schema requires actual data item to be inferred
        name_hint = table_schema_template["name"]
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
    def _merge_key(hint: TColumnProp, keys: TColumnKey, partial: TPartialTableSchema) -> None:
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
        columns: TTableHintTemplate[TTableSchemaColumns] = None,
        primary_key: TTableHintTemplate[TColumnKey] = None,
        merge_key: TTableHintTemplate[TColumnKey] = None
        ) -> TTableSchemaTemplate:
        if not table_name:
            raise TableNameMissing()

        # create a table schema template where hints can be functions taking TDataItem
        if isinstance(columns, C_Mapping):
            # new_table accepts a sequence
            column_list: List[TColumnSchema] = []
            for name, column in columns.items():
                column["name"] = name
                column_list.append(column)
            columns = column_list  # type: ignore

        new_template: TTableSchemaTemplate = new_table(table_name, parent_table_name, write_disposition=write_disposition, columns=columns)  # type: ignore
        if primary_key:
            new_template["primary_key"] = primary_key
        if merge_key:
            new_template["merge_key"] = merge_key
        # if any of the hints is a function then name must be as well
        if any(callable(v) for k, v in new_template.items() if k != "name") and not callable(table_name):
            raise InconsistentTableTemplate(f"Table name {table_name} must be a function if any other table hint is a function")
        return new_template
