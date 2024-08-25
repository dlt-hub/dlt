from copy import copy
from typing import Set, Dict, Any, Optional, List, Union

from dlt.common.configuration import known_sections, resolve_configuration, with_config
from dlt.common import logger
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.exceptions import MissingDependencyException
from dlt.common.metrics import DataWriterMetrics
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.typing import TDataItems, TDataItem, TLoaderFileFormat
from dlt.common.schema import Schema, utils
from dlt.common.schema.typing import (
    TSchemaContractDict,
    TSchemaEvolutionMode,
    TTableSchema,
    TTableSchemaColumns,
    TPartialTableSchema,
)
from dlt.extract.hints import HintsMeta, TResourceHints
from dlt.extract.resource import DltResource
from dlt.extract.items import DataItemWithMeta, TableNameMeta
from dlt.extract.storage import ExtractorItemStorage
from dlt.normalize.configuration import ItemsNormalizerConfiguration

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa, TAnyArrowItem
except MissingDependencyException:
    pyarrow = None
    pa = None

try:
    from dlt.common.libs.pandas import pandas, pandas_to_arrow
except MissingDependencyException:
    pandas = None


class MaterializedEmptyList(List[Any]):
    """A list variant that will materialize tables even if empty list was yielded"""

    pass


def materialize_schema_item() -> MaterializedEmptyList:
    """Yield this to materialize schema in the destination, even if there's no data."""
    return MaterializedEmptyList()


class ImportFileMeta(HintsMeta):
    __slots__ = ("file_path", "metrics", "file_format")

    def __init__(
        self,
        file_path: str,
        metrics: DataWriterMetrics,
        file_format: TLoaderFileFormat = None,
        hints: TResourceHints = None,
        create_table_variant: bool = None,
    ) -> None:
        super().__init__(hints, create_table_variant)
        self.file_path = file_path
        self.metrics = metrics
        self.file_format = file_format


def with_file_import(
    file_path: str,
    file_format: TLoaderFileFormat,
    items_count: int = 0,
    hints: Union[TResourceHints, TDataItem] = None,
) -> DataItemWithMeta:
    """Marks file under `file_path` to be associated with current resource and imported into the load package as a file of
    type `file_format`.

    You can provide optional `hints` that will be applied to the current resource. Note that you should avoid schema inference at
    runtime if possible and if that is not possible - to do that only once per extract process. Use `make_hints` in `mark` module
    to create hints. You can also pass Arrow table or Pandas data frame form which schema will be taken (but content discarded).
    Create `TResourceHints` with `make_hints`.

    If number of records in `file_path` is known, pass it in `items_count` so `dlt` can generate correct extract metrics.

    Note that `dlt` does not sniff schemas from data and will not guess right file format for you.
    """
    metrics = DataWriterMetrics(file_path, items_count, 0, 0, 0)
    item: TDataItem = None
    # if hints are dict assume that this is dlt schema, if not - that it is arrow table
    if not isinstance(hints, dict):
        item = hints
        hints = None
    return DataItemWithMeta(ImportFileMeta(file_path, metrics, file_format, hints, False), item)


class Extractor:
    @configspec
    class ExtractorConfiguration(BaseConfiguration):
        _caps: Optional[DestinationCapabilitiesContext] = None

    @with_config(spec=ExtractorConfiguration)
    def __init__(
        self,
        load_id: str,
        item_storage: ExtractorItemStorage,
        schema: Schema,
        collector: Collector = NULL_COLLECTOR,
        *,
        _caps: DestinationCapabilitiesContext = None,
    ) -> None:
        self.schema = schema
        self.naming = schema.naming
        self.collector = collector
        self.resources_with_items: Set[str] = set()
        """Tracks resources that received items"""
        self.resources_with_empty: Set[str] = set()
        """Track resources that received empty materialized list"""
        self.load_id = load_id
        self.item_storage = item_storage
        self._table_contracts: Dict[str, TSchemaContractDict] = {}
        self._filtered_tables: Set[str] = set()
        self._filtered_columns: Dict[str, Dict[str, TSchemaEvolutionMode]] = {}
        self._caps = _caps or DestinationCapabilitiesContext.generic_capabilities()

    def write_items(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        """Write `items` to `resource` optionally computing table schemas and revalidating/filtering data"""
        if isinstance(meta, HintsMeta) and meta.hints:
            # update the resource with new hints, remove all caches so schema is recomputed
            # and contracts re-applied
            resource.merge_hints(meta.hints, meta.create_table_variant)
            # convert to table meta if created table variant so item is assigned to this table
            if meta.create_table_variant:
                # name in hints meta must be a string, otherwise merge_hints would fail
                meta = TableNameMeta(meta.hints["name"])  # type: ignore[arg-type]
            self._reset_contracts_cache()

        if table_name := self._get_static_table_name(resource, meta):
            # write item belonging to table with static name
            self._write_to_static_table(resource, table_name, items, meta)
        else:
            # table has name or other hints depending on data items
            self._write_to_dynamic_table(resource, items, meta)

    def write_empty_items_file(self, table_name: str) -> None:
        table_name = self.naming.normalize_table_identifier(table_name)
        self.item_storage.write_empty_items_file(self.load_id, self.schema.name, table_name, None)

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
        new_rows_count = self.item_storage.write_data_item(
            self.load_id, self.schema.name, table_name, items, columns
        )
        self.collector.update(table_name, inc=new_rows_count)
        # if there were rows or item was empty arrow table
        if new_rows_count > 0 or self.__class__ is ArrowExtractor:
            self.resources_with_items.add(resource_name)
        else:
            if isinstance(items, MaterializedEmptyList):
                self.resources_with_empty.add(resource_name)

    def _import_item(
        self,
        table_name: str,
        resource_name: str,
        meta: ImportFileMeta,
    ) -> None:
        metrics = self.item_storage.import_items_file(
            self.load_id,
            self.schema.name,
            table_name,
            meta.file_path,
            meta.metrics,
            meta.file_format,
        )
        self.collector.update(table_name, inc=metrics.items_count)
        self.resources_with_items.add(resource_name)

    def _write_to_dynamic_table(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        if not isinstance(items, list):
            items = [items]

        for item in items:
            table_name = self._get_dynamic_table_name(resource, item)
            if table_name in self._filtered_tables:
                continue
            if table_name not in self._table_contracts or resource._table_has_other_dynamic_hints:
                item = self._compute_and_update_table(
                    resource, table_name, item, TableNameMeta(table_name)
                )
            # write to storage with inferred table name
            if table_name not in self._filtered_tables:
                if isinstance(meta, ImportFileMeta):
                    self._import_item(table_name, resource.name, meta)
                else:
                    self._write_item(table_name, resource.name, item)

    def _write_to_static_table(
        self, resource: DltResource, table_name: str, items: TDataItems, meta: Any
    ) -> None:
        if table_name not in self._table_contracts:
            items = self._compute_and_update_table(resource, table_name, items, meta)
        if table_name not in self._filtered_tables:
            if isinstance(meta, ImportFileMeta):
                self._import_item(table_name, resource.name, meta)
            else:
                self._write_item(table_name, resource.name, items)

    def _compute_table(self, resource: DltResource, items: TDataItems, meta: Any) -> TTableSchema:
        """Computes a schema for a new or dynamic table and normalizes identifiers"""
        return utils.normalize_table_identifiers(
            resource.compute_table_schema(items, meta), self.schema.naming
        )

    def _compute_and_update_table(
        self, resource: DltResource, table_name: str, items: TDataItems, meta: Any
    ) -> TDataItems:
        """
        Computes new table and does contract checks, if false is returned, the table may not be created and no items should be written
        """
        computed_table = self._compute_table(resource, items, meta)
        # overwrite table name (if coming from meta)
        computed_table["name"] = table_name
        # get or compute contract
        schema_contract = self._table_contracts.setdefault(
            table_name, self.schema.resolve_contract_settings_for_table(table_name, computed_table)
        )

        # this is a new table so allow evolve once
        if schema_contract["columns"] != "evolve" and self.schema.is_new_table(table_name):
            computed_table["x-normalizer"] = {"evolve-columns-once": True}
        existing_table = self.schema._schema_tables.get(table_name, None)
        if existing_table:
            # TODO: revise this. computed table should overwrite certain hints (ie. primary and merge keys) completely
            diff_table = utils.diff_table(self.schema.name, existing_table, computed_table)
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


class ObjectExtractor(Extractor):
    """Extracts Python object data items into typed jsonl"""

    pass


class ArrowExtractor(Extractor):
    """Extracts arrow data items into parquet. Normalizes arrow items column names.
    Compares the arrow schema to actual dlt table schema to reorder the columns and to
    insert missing columns (without data). Adds _dlt_load_id column to the table if
    `add_dlt_load_id` is set to True in normalizer config.

    We do things that normalizer should do here so we do not need to load and save parquet
    files again later.

    Handles the following types:
    - `pyarrow.Table`
    - `pyarrow.RecordBatch`
    - `pandas.DataFrame` (is converted to arrow `Table` before processing)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._normalize_config = self._retrieve_normalize_config()

    def _retrieve_normalize_config(self) -> ItemsNormalizerConfiguration:
        """Get normalizer settings that are used here"""
        return resolve_configuration(
            ItemsNormalizerConfiguration(),
            sections=(known_sections.NORMALIZE, "parquet_normalizer"),
        )

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
                    pandas_to_arrow(item)
                    if (pandas and isinstance(item, pandas.DataFrame))
                    else item
                )
                for item in (items if isinstance(items, list) else [items])
            )
        ]
        super().write_items(resource, items, meta)

    def _write_to_static_table(
        self, resource: DltResource, table_name: str, items: TDataItems, meta: Any
    ) -> None:
        # contract cache not supported for arrow tables
        self._reset_contracts_cache()
        super()._write_to_static_table(resource, table_name, items, meta)

    def _apply_contract_filters(
        self, item: "TAnyArrowItem", resource: DltResource, static_table_name: Optional[str]
    ) -> "TAnyArrowItem":
        """Removes the columns (discard value) or rows (discard rows) as indicated by contract filters."""
        # convert arrow schema names into normalized names
        rename_mapping = pyarrow.get_normalized_arrow_fields_mapping(item.schema, self.naming)
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
        columns = columns or self.schema.get_table_columns(table_name)
        # Note: `items` is always a list here due to the conversion in `write_table`
        items = [
            pyarrow.normalize_py_arrow_item(
                item,
                columns,
                self.naming,
                self._caps,
                load_id=self.load_id if self._normalize_config.add_dlt_load_id else None,
            )
            for item in items
        ]
        # write items one by one
        super()._write_item(table_name, resource_name, items, columns)

    def _compute_table(
        self, resource: DltResource, items: TDataItems, meta: Any
    ) -> TPartialTableSchema:
        arrow_table: TTableSchema = None

        # several arrow tables will update the pipeline schema and we want that earlier
        # arrow tables override the latter so the resultant schema is the same as if
        # they are sent separately
        for item in reversed(items):
            computed_table = super()._compute_table(resource, item, Any)
            # Merge the columns to include primary_key and other hints that may be set on the resource
            if arrow_table:
                utils.merge_table(self.schema.name, computed_table, arrow_table)
            else:
                arrow_table = copy(computed_table)
            arrow_table["columns"] = pyarrow.py_arrow_to_table_schema_columns(item.schema)

            # Add load_id column if needed
            dlt_load_id_col = self.naming.normalize_table_identifier("_dlt_load_id")
            if (
                self._normalize_config.add_dlt_load_id
                and dlt_load_id_col not in arrow_table["columns"]
            ):
                arrow_table["columns"][dlt_load_id_col] = {
                    "name": dlt_load_id_col,
                    "data_type": "text",
                    "nullable": False,
                }

            # normalize arrow table before merging
            arrow_table = utils.normalize_table_identifiers(arrow_table, self.schema.naming)
            # issue warnings when overriding computed with arrow
            override_warn: bool = False
            for col_name, column in arrow_table["columns"].items():
                if src_column := computed_table["columns"].get(col_name):
                    for hint_name, hint in column.items():
                        if (src_hint := src_column.get(hint_name)) is not None:
                            if src_hint != hint:
                                override_warn = True
                                logger.info(
                                    f"In resource: {resource.name}, when merging arrow schema on"
                                    f" column {col_name}. The hint {hint_name} value"
                                    f" {src_hint} defined in resource will overwrite arrow hint"
                                    f" with value {hint}."
                                )
            if override_warn:
                logger.warning(
                    f"In resource: {resource.name}, when merging arrow schema with dlt schema,"
                    " several column hints were different. dlt schema hints were kept and arrow"
                    " schema and data were unmodified. It is up to destination to coerce the"
                    " differences when loading. Change log level to INFO for more details."
                )

            utils.merge_columns(
                arrow_table["columns"], computed_table["columns"], merge_columns=True
            )

        return arrow_table

    def _compute_and_update_table(
        self, resource: DltResource, table_name: str, items: TDataItems, meta: Any
    ) -> TDataItems:
        items = super()._compute_and_update_table(resource, table_name, items, meta)
        # filter data item as filters could be updated in compute table
        items = [self._apply_contract_filters(item, resource, table_name) for item in items]
        return items
