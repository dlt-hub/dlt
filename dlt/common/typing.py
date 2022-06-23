from typing import Callable, Dict, Any, Mapping, TypeVar, TypedDict, TYPE_CHECKING
if TYPE_CHECKING:
    from _typeshed import StrOrBytesPath
else:
    StrOrBytesPath = Any

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
