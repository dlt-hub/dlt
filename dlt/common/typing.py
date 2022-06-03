from typing import Dict, Any, List, Literal, Mapping, Sequence, TypedDict, Optional, Union

DictStrAny = Dict[str, Any]
DictStrStr = Dict[str, str]
StrAny = Mapping[str, Any]  # immutable, covariant entity
StrStr = Mapping[str, str]  # immutable, covariant entity
StrStrStr = Mapping[str, Mapping[str, str]]  # immutable, covariant entity

class TEventRow(TypedDict, total=False):
    _timestamp: float  # used for partitioning
    _dist_key: str  # distribution key used for clustering
    _record_hash: str  # unique id of current row
    _root_hash: str  # unique id of top level parent

class TEventRowRoot(TEventRow, total=False):
    _load_id: str  # load id to identify records loaded together that ie. need to be processed
    _event_json: str  # dump of the original event
    _event_type: str  # sets event type which will be translated to table


class TEventRowChild(TEventRow, total=False):
    _parent_hash: str  # unique id of parent row
    _pos: int  # position in the list of rows
    value: Any  # for lists of simple types


class TEvent(TypedDict, total=False):
    pass


class TTimestampEvent(TEvent, total=False):
    timestamp: float  # timestamp of event
