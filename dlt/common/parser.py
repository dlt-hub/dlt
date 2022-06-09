from typing import Iterator, Optional, Tuple, Callable, cast, TypedDict, Any

from dlt.common import json
from dlt.common.schema import Schema
from dlt.common.utils import uniq_id, digest128
from dlt.common.typing import TEvent, StrAny
from dlt.common.names import normalize_db_name
from dlt.common.sources import DLT_METADATA_FIELD, TEventDLTMeta, get_table_name


class TEventRow(TypedDict, total=False):
    _timestamp: float  # used for partitioning
    _dist_key: str  # distribution key used for clustering
    _record_hash: str  # unique id of current row
    _root_hash: str  # unique id of top level parent


class TEventRowRoot(TEventRow, total=False):
    _load_id: str  # load id to identify records loaded together that ie. need to be processed
    _event_json: str  # dump of the original event
    __dlt_meta: TEventDLTMeta  # stores metadata


class TEventRowChild(TEventRow, total=False):
    _parent_hash: str  # unique id of parent row
    _pos: int  # position in the list of rows
    value: Any  # for lists of simple types


# I(table name, row data)
TUnpackedRowIterator = Iterator[Tuple[str, StrAny]]
TExtractFunc = Callable[[Schema, TEvent, str, bool], TUnpackedRowIterator]


# subsequent nested fields will be separated with the string below, applies both to field and table names
PATH_SEPARATOR = "__"


# for those paths the complex nested objects should be left in place
# current use case: we want to preserve event_slot__value in db even if it's an object
# TODO: pass table definition and accept complex type
def _should_preserve_complex_value(table: str, field_name: str) -> bool:
    path = f"{table}{PATH_SEPARATOR}{field_name}"
    return path in ["event_slot__value"]


def _flatten(table: str, dict_row: TEventRowChild) -> TEventRowChild:
    out_rec_row: TEventRowChild = {}

    def unpack_row_dicts(dict_row: StrAny, parent_name: Optional[str]) -> None:
        for k, v in dict_row.items():
            corrected_k = normalize_db_name(k)
            child_name = corrected_k if not parent_name else f'{parent_name}{PATH_SEPARATOR}{corrected_k}'
            if type(v) is dict:
                unpack_row_dicts(v, parent_name=child_name)
                if _should_preserve_complex_value(table, child_name):
                    out_rec_row[child_name] = v  # type: ignore
            else:
                out_rec_row[child_name] = v  # type: ignore

    unpack_row_dicts(dict_row, None)
    return out_rec_row


def _get_child_row_hash(parent_hash: str, child_table: str, list_pos: int) -> str:
    # create deterministic unique id of the child row taking into account that all lists are ordered
    # and all child tables must be lists
    return digest128(f"{parent_hash}_{child_table}_{list_pos}")


def _unpack_row(
    schema: Schema,
    dict_row: TEventRowChild,
    extend: TEventRowChild,
    table: str,
    parent_hash: Optional[str] = None,
    pos: Optional[int] = None
    ) -> TUnpackedRowIterator:

    def _append_child_meta(_row: TEventRowChild, _hash: str, _p_hash: str, _p_pos: int) -> TEventRowChild:
        _row["_parent_hash"] = _p_hash
        _row["_pos"] = _p_pos
        _row.update(extend)

        return _row

    is_top_level = parent_hash is None

    # flatten current row
    new_dict_row = _flatten(table, dict_row)
    # infer record hash or leave existing primary key if present
    record_hash = new_dict_row.get("_record_hash", None)
    if not record_hash:
        # check if we have primary key: if so use it
        primary_key = schema.filter_hints_in_row(table, "primary_key", new_dict_row)
        if primary_key:
            # create row id from primary key
            record_hash = digest128("_".join(map(lambda v: str(v), primary_key.values())))
        elif not is_top_level:
            # child table row deterministic hash
            record_hash = _get_child_row_hash(parent_hash, table, pos)
            # link to parent table
            _append_child_meta(new_dict_row, record_hash, parent_hash, pos)
        else:
            # create random row id, note that incremental loads will not work with such tables
            record_hash = uniq_id()
        new_dict_row["_record_hash"] = record_hash

    # if _root_hash propagation requested and we are at the top level then update extend
    if "_root_hash" in extend and extend["_root_hash"] is None and is_top_level:
        extend["_root_hash"] = record_hash

    # generate child tables only for lists
    children = [k for k in new_dict_row if type(new_dict_row[k]) is list]  # type: ignore
    for k in children:
        child_table = f"{table}{PATH_SEPARATOR}{k}"
        # this will skip empty lists
        v: TEventRowChild
        for idx, v in enumerate(new_dict_row[k]):  # type: ignore
            # yield child table row
            if type(v) is dict:
                yield from _unpack_row(schema, v, extend, child_table, record_hash, idx)
            elif type(v) is list:
                # unpack lists of lists
                raise ValueError(v)
            else:
                # list of simple types
                child_row_hash = _get_child_row_hash(record_hash, child_table, idx)
                e = _append_child_meta({"value": v, "_record_hash": child_row_hash}, child_row_hash, record_hash, idx)
                yield child_table, e
        if not _should_preserve_complex_value(table, k):
            # remove child list
            del new_dict_row[k]  # type: ignore

    yield table, new_dict_row


def extract(schema: Schema, source_event: TEvent, load_id: str, add_json: bool) -> TUnpackedRowIterator:
    # we will extend event with all the fields necessary to load it as root row
    event = cast(TEventRowRoot, source_event)
    # identify load id if loaded data must be processed after loading incrementally
    event["_load_id"] = load_id
    # add original json field, mostly useful for debugging
    if add_json:
        event["_event_json"] = json.dumps(event)
    # find table name
    table_name = get_table_name(event) or schema.schema_name
    # drop dlt metadata before unpacking
    event.pop(DLT_METADATA_FIELD, None)  # type: ignore
    # TODO: if table_name exist get "_dist_key" and "_timestamp" from the table definition in schema and propagate, if not take them from global hints
    # use event type or schema name as table name, request _root_hash propagation
    yield from _unpack_row(schema, cast(TEventRowChild, event), {"_root_hash": None}, table_name)
