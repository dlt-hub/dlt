from copy import copy
from typing import Set, Dict, Any, Optional, Set

from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.exceptions import MissingDependencyException

from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.utils import update_dict_nested
from dlt.common.typing import TDataItems, TDataItem
from dlt.common.schema import Schema, utils
from dlt.common.schema.typing import (
    TSchemaContractDict,
    TSchemaEvolutionMode,
    TTableSchema,
    TTableSchemaColumns,
    TPartialTableSchema,
)
from dlt.extract.hints import HintsMeta
from dlt.extract.resource import DltResource
from dlt.extract.items import TableNameMeta
from dlt.extract.storage import ExtractStorage, ExtractorItemStorage

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa, TAnyArrowItem
except MissingDependencyException:
    pyarrow = None

try:
    from dlt.common.libs.pandas import pandas
except MissingDependencyException:
    pandas = None


class Extractor:
    file_format: TLoaderFileFormat

    @configspec
    class ExtractorConfiguration(BaseConfiguration):
        _caps: Optional[DestinationCapabilitiesContext] = None

    @with_config(spec=ExtractorConfiguration)
    def __init__(
        self,
        load_id: str,
        storage: ExtractStorage,
        schema: Schema,
        resources_with_items: Set[str],
        collector: Collector = NULL_COLLECTOR,
        *,
        _caps: DestinationCapabilitiesContext = None,
    ) -> None:
        self.schema = schema
        self.naming = schema.naming
        self.collector = collector
        self.resources_with_items = resources_with_items
        self.load_id = load_id
        self._table_contracts: Dict[str, TSchemaContractDict] = {}
        self._filtered_tables: Set[str] = set()
        self._filtered_columns: Dict[str, Dict[str, TSchemaEvolutionMode]] = {}
        self._storage = storage
        self._caps = _caps or DestinationCapabilitiesContext.generic_capabilities()

    @property
    def storage(self) -> ExtractorItemStorage:
        return self._storage.get_storage(self.file_format)

    @staticmethod
    def item_format(items: TDataItems) -> Optional[TLoaderFileFormat]:
        """Detect the loader file format of the data items based on type.
        Currently this is either 'arrow' or 'puae-jsonl'

        Returns:
            The loader file format or `None` if if can't be detected.
        """
        for item in items if isinstance(items, list) else [items]:
            # Assume all items in list are the same type
            if (pyarrow and pyarrow.is_arrow_item(item)) or (
                pandas and isinstance(item, pandas.DataFrame)
            ):
                return "arrow"
            return "puae-jsonl"
        return None  # Empty list is unknown format

    def write_items(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        """Write `items` to `resource` optionally computing table schemas and revalidating/filtering data"""
        if isinstance(meta, HintsMeta):
            # update the resource with new hints, remove all caches so schema is recomputed
            # and contracts re-applied
            resource.merge_hints(meta.hints)
            self._reset_contracts_cache()

        if table_name := self._get_static_table_name(resource, meta):
            # write item belonging to table with static name
            self._write_to_static_table(resource, table_name, items)
        else:
            # table has name or other hints depending on data items
            self._write_to_dynamic_table(resource, items)

    def write_empty_items_file(self, table_name: str) -> None:
        table_name = self.naming.normalize_table_identifier(table_name)
        self.storage.write_empty_items_file(self.load_id, self.schema.name, table_name, None)

    def _get_static_table_name(self, resource: DltResource, meta: Any) -> Optional[str]:
        if resource._table_name_hint_fun:
            return None
        if isinstance(meta, TableNameMeta):
            table_name = meta.table_name
        else:
            table_name = resource.table_name  # type: ignore[assignment]
        return self.naming.normalize_table_identifier(table_name)

    def _get_dynamic_table_name(self, resource: DltResource, item: TDataItem) -> str:
        return self.naming.normalize_table_identifier(resource._table_name_hint_fun(item))

    def _write_item(
        self,
        table_name: str,
        resource_name: str,
        items: TDataItems,
        columns: TTableSchemaColumns = None,
    ) -> None:
        new_rows_count = self.storage.write_data_item(
            self.load_id, self.schema.name, table_name, items, columns
        )
        self.collector.update(table_name, inc=new_rows_count)
        if new_rows_count > 0:
            self.resources_with_items.add(resource_name)

    def _write_to_dynamic_table(self, resource: DltResource, items: TDataItems) -> None:
        if not isinstance(items, list):
            items = [items]

        for item in items:
            table_name = self._get_dynamic_table_name(resource, item)
            if table_name in self._filtered_tables:
                continue
            if table_name not in self._table_contracts or resource._table_has_other_dynamic_hints:
                item = self._compute_and_update_table(resource, table_name, item)
            # write to storage with inferred table name
            if table_name not in self._filtered_tables:
                self._write_item(table_name, resource.name, item)

    def _write_to_static_table(
        self, resource: DltResource, table_name: str, items: TDataItems
    ) -> None:
        if table_name not in self._table_contracts:
            items = self._compute_and_update_table(resource, table_name, items)
        if table_name not in self._filtered_tables:
            self._write_item(table_name, resource.name, items)

    def _compute_table(self, resource: DltResource, items: TDataItems) -> TTableSchema:
        """Computes a schema for a new or dynamic table and normalizes identifiers"""
        return self.schema.normalize_table_identifiers(resource.compute_table_schema(items))

    def _compute_and_update_table(
        self, resource: DltResource, table_name: str, items: TDataItems
    ) -> TDataItems:
        """
        Computes new table and does contract checks, if false is returned, the table may not be created and no items should be written
        """
        computed_table = self._compute_table(resource, items)
        # overwrite table name (if coming from meta)
        computed_table["name"] = table_name
        # get or compute contract
        schema_contract = self._table_contracts.setdefault(
            table_name, self.schema.resolve_contract_settings_for_table(table_name, computed_table)
        )

        # this is a new table so allow evolve once
        if schema_contract["columns"] != "evolve" and self.schema.is_new_table(table_name):
            computed_table["x-normalizer"] = {"evolve-columns-once": True}  # type: ignore[typeddict-unknown-key]
        existing_table = self.schema._schema_tables.get(table_name, None)
        if existing_table:
            diff_table = utils.diff_tables(existing_table, computed_table)
        else:
            diff_table = computed_table

        # apply contracts
        diff_table, filters = self.schema.apply_schema_contract(
            schema_contract, diff_table, data_item=items
        )

        # merge with schema table
        if diff_table:
            self.schema.update_table(diff_table)

        # process filters
        if filters:
            for entity, name, mode in filters:
                if entity == "tables":
                    self._filtered_tables.add(name)
                elif entity == "columns":
                    filtered_columns = self._filtered_columns.setdefault(table_name, {})
                    filtered_columns[name] = mode
        return items

    def _reset_contracts_cache(self) -> None:
        """Removes all cached contracts, filtered columns and tables"""
        self._table_contracts.clear()
        self._filtered_tables.clear()
        self._filtered_columns.clear()


class JsonLExtractor(Extractor):
    file_format = "puae-jsonl"


class ArrowExtractor(Extractor):
    file_format = "arrow"

    def write_items(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        static_table_name = self._get_static_table_name(resource, meta)
        items = [
            # 3. remove columns and rows in data contract filters
            # 2. Remove null-type columns from the table(s) as they can't be loaded
            self._apply_contract_filters(
                pyarrow.remove_null_columns(tbl), resource, static_table_name
            )
            for tbl in (
                (
                    # 1. Convert pandas frame(s) to arrow Table
                    pa.Table.from_pandas(item)
                    if (pandas and isinstance(item, pandas.DataFrame))
                    else item
                )
                for item in (items if isinstance(items, list) else [items])
            )
        ]
        super().write_items(resource, items, meta)

    def _apply_contract_filters(
        self, item: "TAnyArrowItem", resource: DltResource, static_table_name: Optional[str]
    ) -> "TAnyArrowItem":
        """Removes the columns (discard value) or rows (discard rows) as indicated by contract filters."""
        # convert arrow schema names into normalized names
        rename_mapping = pyarrow.get_normalized_arrow_fields_mapping(item, self.naming)
        # find matching columns and delete by original name
        table_name = static_table_name or self._get_dynamic_table_name(resource, item)
        filtered_columns = self._filtered_columns.get(table_name)
        if filtered_columns:
            # remove rows where columns have non null values
            # create a mask where rows will be False if any of the specified columns are non-null
            mask = None
            rev_mapping = {v: k for k, v in rename_mapping.items()}
            for column in [
                name for name, mode in filtered_columns.items() if mode == "discard_row"
            ]:
                is_null = pyarrow.pyarrow.compute.is_null(item[rev_mapping[column]])
                mask = is_null if mask is None else pyarrow.pyarrow.compute.and_(mask, is_null)
            # filter the table using the mask
            if mask is not None:
                item = item.filter(mask)

            # remove value actually removes the whole columns from the table
            # NOTE: filtered columns has normalized column names so we need to go through mapping
            removed_columns = [
                name
                for name in rename_mapping
                if filtered_columns.get(rename_mapping[name]) is not None
            ]
            if removed_columns:
                item = pyarrow.remove_columns(item, removed_columns)

        return item

    def _write_item(
        self,
        table_name: str,
        resource_name: str,
        items: TDataItems,
        columns: TTableSchemaColumns = None,
    ) -> None:
        columns = columns or self.schema.tables[table_name]["columns"]
        # Note: `items` is always a list here due to the conversion in `write_table`
        items = [
            pyarrow.normalize_py_arrow_schema(item, columns, self.naming, self._caps)
            for item in items
        ]
        super()._write_item(table_name, resource_name, items, columns)

    def _compute_table(self, resource: DltResource, items: TDataItems) -> TPartialTableSchema:
        items = items[0]
        computed_table = super()._compute_table(resource, items)

        # Merge the columns to include primary_key and other hints that may be set on the resource
        arrow_table = copy(computed_table)
        arrow_table["columns"] = pyarrow.py_arrow_to_table_schema_columns(items.schema)
        # normalize arrow table before merging
        arrow_table = self.schema.normalize_table_identifiers(arrow_table)
        # issue warnings when overriding computed with arrow
        for col_name, column in arrow_table["columns"].items():
            if src_column := computed_table["columns"].get(col_name):
                print(src_column)
                for hint_name, hint in column.items():
                    if (src_hint := src_column.get(hint_name)) is not None:
                        if src_hint != hint:
                            logger.warning(
                                f"In resource: {resource.name}, when merging arrow schema on column"
                                f" {col_name}. The hint {hint_name} value {src_hint} defined in"
                                f" resource is overwritten from arrow with value {hint}."
                            )

        # we must override the columns to preserve the order in arrow table
        arrow_table["columns"] = update_dict_nested(
            arrow_table["columns"], computed_table["columns"], keep_dst_values=True
        )

        return arrow_table

    def _compute_and_update_table(
        self, resource: DltResource, table_name: str, items: TDataItems
    ) -> TDataItems:
        items = super()._compute_and_update_table(resource, table_name, items)
        # filter data item as filters could be updated in compute table
        items = [self._apply_contract_filters(item, resource, table_name) for item in items]
        return items
