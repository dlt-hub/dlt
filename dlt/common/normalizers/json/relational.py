from typing import Mapping, Optional, cast, TypedDict, Any

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumn, TColumnName, TSimpleRegex
from dlt.common.schema.utils import column_name_validator
from dlt.common.utils import uniq_id, digest128
from dlt.common.typing import DictStrAny, DictStrStr, TEvent, StrAny
from dlt.common.normalizers.json import TUnpackedRowIterator
from dlt.common.sources import DLT_METADATA_FIELD, TEventDLTMeta, get_table_name
from dlt.common.validation import validate_dict


class TEventRow(TypedDict, total=False):
    _dlt_id: str  # unique id of current row


class TEventRowRoot(TEventRow, total=False):
    _dlt_load_id: str  # load id to identify records loaded together that ie. need to be processed
    _dlt_meta: TEventDLTMeta  # stores metadata, should never be sent to the normalizer


class TEventRowChild(TEventRow, total=False):
    _dlt_root_id: str  # unique id of top level parent
    _dlt_parent_id: str  # unique id of parent row
    _dlt_list_idx: int  # position in the list of rows
    value: Any  # for lists of simple types


class JSONNormalizerConfigPropagation(TypedDict, total=True):
    root: Optional[Mapping[str, TColumnName]]
    tables: Optional[Mapping[str, Mapping[str, TColumnName]]]


class JSONNormalizerConfig(TypedDict, total=True):
    generate_dlt_id: Optional[bool]
    propagation: Optional[JSONNormalizerConfigPropagation]


# for those paths the complex nested objects should be left in place
# current use case: we want to preserve event_slot__value in db even if it's an object
def _is_complex_type(schema: Schema, table_name: str, field_name: str) -> bool:
    column: TColumn = None
    table = schema._schema_tables.get(table_name)
    if table:
        column = table["columns"].get(field_name, None)
    if column is None:
        data_type = schema.get_preferred_type(field_name)
    else:
        data_type = column["data_type"]
    return data_type == "complex"


def _flatten(schema: Schema, table: str, dict_row: TEventRow) -> TEventRow:
    out_rec_row: DictStrAny = {}

    def norm_row_dicts(dict_row: StrAny, parent_name: Optional[str]) -> None:
        for k, v in dict_row.items():
            corrected_k = schema.normalize_column_name(k)
            child_name = corrected_k if not parent_name else schema.normalize_make_path(parent_name, corrected_k)
            if isinstance(v, dict):
                if _is_complex_type(schema, table, child_name):
                    out_rec_row[child_name] = v
                else:
                    norm_row_dicts(v, parent_name=child_name)
            else:
                out_rec_row[child_name] = v

    norm_row_dicts(dict_row, None)
    return cast(TEventRow, out_rec_row)


def _get_child_row_hash(parent_hash: str, child_table: str, list_pos: int) -> str:
    # create deterministic unique id of the child row taking into account that all lists are ordered
    # and all child tables must be lists
    return digest128(f"{parent_hash}_{child_table}_{list_pos}")


def _get_content_hash(schema: Schema, table: str, row: StrAny) -> str:
    # generate content hashes only for tables with merge content disposition
    # TODO: extend schema with write disposition
    # WARNING: row may contain complex types: exclude from hash
    return digest128(uniq_id())


def _get_propagated_values(schema: Schema, table: str, row: TEventRow, is_top_level: bool) -> StrAny:
    config: JSONNormalizerConfigPropagation = schema._normalizers_config["json"].get("config", {}).get("propagation", None)
    extend: DictStrAny = {}
    if config:
        # mapping(k:v): propagate property with name "k" as property with name "v" in child table
        mappings: DictStrStr = {}
        if is_top_level:
            mappings.update(config.get("root", {}))
        if table in config.get("tables", {}):
            mappings.update(config["tables"][table])
        # look for keys and create propagation as values
        for prop_from, prop_as in mappings.items():
            if prop_from in row:
                extend[prop_as] = row[prop_from]  # type: ignore

    return extend


def _normalize_row(
    schema: Schema,
    dict_row: TEventRow,
    extend: DictStrAny,
    table: str,
    parent_table: Optional[str] = None,
    parent_hash: Optional[str] = None,
    pos: Optional[int] = None
) -> TUnpackedRowIterator:

    def _add_linking(_child_row: TEventRowChild, _p_hash: str, _p_pos: int) -> TEventRowChild:
        _child_row["_dlt_parent_id"] = _p_hash
        _child_row["_dlt_list_idx"] = _p_pos
        _child_row.update(extend)  # type: ignore

        return _child_row

    is_top_level = parent_table is None

    # flatten current row
    flattened_row = _flatten(schema, table, dict_row)
    # infer record hash or leave existing primary key if present
    record_hash = flattened_row.get("_dlt_id", None)
    if not record_hash:
        # check if we have primary key: if so use it
        primary_key = schema.filter_row_with_hint(table, "primary_key", flattened_row)
        if primary_key:
            # create row id from primary key
            record_hash = digest128("_".join(map(lambda v: str(v), primary_key.values())))
        elif not is_top_level:
            # child table row deterministic hash
            record_hash = _get_child_row_hash(parent_hash, table, pos)
            # link to parent table
            _add_linking(cast(TEventRowChild, flattened_row), parent_hash, pos)
        else:
            # create hash based on the content of the row
            record_hash = _get_content_hash(schema, table, flattened_row)
        flattened_row["_dlt_id"] = record_hash

    # find fields to propagate to child tables in config
    extend.update(_get_propagated_values(schema, table, flattened_row, is_top_level))

    # remove all lists from row before yielding
    lists = {k: flattened_row[k] for k in flattened_row if isinstance(flattened_row[k], list)}  # type: ignore
    for k in list(lists.keys()):
        # leave complex lists in flattened_row
        if not _is_complex_type(schema, table, k):
            # remove child list
            del flattened_row[k]  # type: ignore
        else:
            pass
            # delete complex types from
            del lists[k]

    # yield parent table first
    yield (table, parent_table), flattened_row

    # generate child tables only for lists
    for k, list_content in lists.items():
        child_table = schema.normalize_make_path(table, k)
        # this will skip empty lists
        v: TEventRowChild = None
        for idx, v in enumerate(list_content):
            # yield child table row
            tv = type(v)
            if tv is dict:
                yield from _normalize_row(schema, v, extend, child_table, table, record_hash, idx)
            elif tv is list:
                # unpack lists of lists
                raise ValueError(v)
            else:
                # list of simple types
                child_row_hash = _get_child_row_hash(record_hash, child_table, idx)
                e = _add_linking({"value": v, "_dlt_id": child_row_hash}, record_hash, idx)
                yield (child_table, table), e


def extend_schema(schema: Schema) -> None:
    # validate config
    config = schema._normalizers_config["json"].get("config", {})
    validate_dict(JSONNormalizerConfig, config, "./normalizers/json/config", validator=column_name_validator(schema.normalize_column_name))

    # quick check to see if hints are applied
    default_hints = schema.schema_settings.get("default_hints", {})
    if "not_null" in default_hints and "^_dlt_id$" in default_hints["not_null"]:
        return
    # add hints
    schema.merge_hints(
        {
            "not_null": [
                TSimpleRegex("re:^_dlt_id$"), TSimpleRegex("re:^_dlt_root_id$"), TSimpleRegex("re:^_dlt_parent_id$"),
                TSimpleRegex("re:^_dlt_list_idx$"), TSimpleRegex("_dlt_load_id")
                ],
            "foreign_key": [TSimpleRegex("re:^_dlt_parent_id$")],
            "unique": [TSimpleRegex("re:^_dlt_id$")]
        }
    )


def normalize(schema: Schema, source_event: TEvent, load_id: str) -> TUnpackedRowIterator:
    # we will extend event with all the fields necessary to load it as root row
    event = cast(TEventRowRoot, source_event)
    # identify load id if loaded data must be processed after loading incrementally
    event["_dlt_load_id"] = load_id
    # find table name
    table_name = schema.normalize_table_name(get_table_name(event) or schema.schema_name)
    # drop dlt metadata before normalizing
    event.pop(DLT_METADATA_FIELD, None)  # type: ignore
    # use event type or schema name as table name, request _dlt_root_id propagation
    yield from _normalize_row(schema, cast(TEventRowChild, event), {}, table_name)
