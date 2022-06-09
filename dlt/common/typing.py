from typing import Callable, Dict, Any, List, Literal, Mapping, Sequence, TypeVar, TypedDict, Optional, Union

DictStrAny = Dict[str, Any]
DictStrStr = Dict[str, str]
StrAny = Mapping[str, Any]  # immutable, covariant entity
StrStr = Mapping[str, str]  # immutable, covariant entity
StrStrStr = Mapping[str, Mapping[str, str]]  # immutable, covariant entity
TFun = TypeVar("TFun", bound=Callable[..., Any])


class TEvent(TypedDict, total=False):
    pass


class TTimestampEvent(TEvent, total=False):
    timestamp: float  # timestamp of event
