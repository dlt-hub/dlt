from collections.abc import Mapping as C_Mapping, Sequence as C_Sequence
from datetime import datetime, date  # noqa: I251
import inspect
from re import Pattern as _REPattern
from typing import Callable, Dict, Any, Final, Literal, List, Mapping, NewType, Optional, Tuple, Type, TypeVar, Generic, Protocol, TYPE_CHECKING, Union, runtime_checkable, get_args, get_origin
from typing_extensions import TypeAlias, ParamSpec, Concatenate

from dlt.common.pendulum import timedelta, pendulum

if TYPE_CHECKING:
    from _typeshed import StrOrBytesPath
    from typing import _TypedDict
    REPattern = _REPattern[str]
else:
    StrOrBytesPath = Any
    from typing import _TypedDictMeta as _TypedDict
    REPattern = _REPattern

AnyType: TypeAlias = Any
NoneType = type(None)
DictStrAny: TypeAlias = Dict[str, Any]
DictStrStr: TypeAlias = Dict[str, str]
StrAny: TypeAlias = Mapping[str, Any]  # immutable, covariant entity
StrStr: TypeAlias = Mapping[str, str]  # immutable, covariant entity
StrStrStr: TypeAlias = Mapping[str, Mapping[str, str]]  # immutable, covariant entity
AnyFun: TypeAlias = Callable[..., Any]
TFun = TypeVar("TFun", bound=AnyFun)  # any function
TAny = TypeVar("TAny", bound=Any)
TAnyClass = TypeVar("TAnyClass", bound=object)
TimedeltaSeconds = Union[int, float, timedelta]
# represent secret value ie. coming from Kubernetes/Docker secrets or other providers
TSecretValue = NewType("TSecretValue", Any)  # type: ignore
TSecretStrValue = NewType("TSecretValue", str)  # type: ignore
TDataItem: TypeAlias = Any
"""A single data item as extracted from data source"""
TDataItems: TypeAlias = Union[TDataItem, List[TDataItem]]
"A single data item or a list as extracted from the data source"
TAnyDateTime = Union[pendulum.DateTime, pendulum.Date, datetime, date, str, float, int]
"""DateTime represented as pendulum/python object, ISO string or unix timestamp"""

ConfigValue: None = None
"""value of type None indicating argument that may be injected by config provider"""

TVariantBase = TypeVar("TVariantBase", covariant=True)
TVariantRV = Tuple[str, Any]
VARIANT_FIELD_FORMAT = "v_%s"

@runtime_checkable
class SupportsVariant(Protocol, Generic[TVariantBase]):
    """Defines variant type protocol that should be recognized by normalizers

        Variant types behave like TVariantBase type (ie. Decimal) but also implement the protocol below that is used to extract the variant value from it.
        See `Wei` type declaration which returns Decimal or str for values greater than supported by destination warehouse.
    """
    def __call__(self) -> Union[TVariantBase, TVariantRV]:
        ...


class SupportsHumanize(Protocol):
    def asdict(self) -> DictStrAny:
        """Represents object as dict with a schema loadable by dlt"""
        ...

    def asstr(self, verbosity: int = 0) -> str:
        """Represents object as human readable string"""
        ...


def is_optional_type(t: Type[Any]) -> bool:
    return get_origin(t) is Union and type(None) in get_args(t)


def is_final_type(t: Type[Any]) -> bool:
    return get_origin(t) is Final


def extract_optional_type(t: Type[Any]) -> Any:
    return get_args(t)[0]


def is_literal_type(hint: Type[Any]) -> bool:
    return get_origin(hint) is Literal


def is_union(hint: Type[Any]) -> bool:
    return get_origin(hint) is Union


def is_newtype_type(t: Type[Any]) -> bool:
    return hasattr(t, "__supertype__")


def is_typeddict(t: Type[Any]) -> bool:
    return isinstance(t, _TypedDict)


def is_list_generic_type(t: Type[Any]) -> bool:
    try:
        return issubclass(get_origin(t), C_Sequence)
    except TypeError:
        return False


def is_dict_generic_type(t: Type[Any]) -> bool:
    try:
        return issubclass(get_origin(t), C_Mapping)
    except TypeError:
        return False


def extract_inner_type(hint: Type[Any], preserve_new_types: bool = False) -> Type[Any]:
    """Gets the inner type from Literal, Optional, Final and NewType

    Args:
        hint (Type[Any]): Type to extract
        preserve_new_types (bool): Do not extract supertype of a NewType

    Returns:
        Type[Any]: Inner type if hint was Literal, Optional or NewType, otherwise hint
    """
    if is_literal_type(hint):
        # assume that all literals are of the same type
        return extract_inner_type(type(get_args(hint)[0]), preserve_new_types)
    if is_optional_type(hint) or is_final_type(hint):
        # extract specialization type and call recursively
        return extract_inner_type(get_args(hint)[0], preserve_new_types)
    if is_newtype_type(hint) and not preserve_new_types:
        # descend into supertypes of NewType
        return extract_inner_type(hint.__supertype__, preserve_new_types)
    return hint


def get_all_types_of_class_in_union(hint: Type[Any], cls: Type[TAny]) -> List[Type[TAny]]:
    # hint is an Union that contains classes, return all classes that are a subclass or superclass of cls
    return [t for t in get_args(hint) if inspect.isclass(t) and (issubclass(t, cls) or issubclass(cls, t))]


def get_generic_type_argument_from_instance(instance: Any, sample_value: Optional[Any]) -> Type[Any]:
    """Infers type argument of a Generic class from an `instance` of that class using optional `sample_value` of the argument type

    Inference depends on the presence of __orig_class__ attribute in instance, if not present - sample_Value will be used

    Args:
        instance (Any): instance of Generic class
        sample_value (Optional[Any]): instance of type of generic class, optional

    Returns:
        Type[Any]: type argument or Any if not known
    """
    orig_param_type = Any
    if hasattr(instance, "__orig_class__"):
        orig_param_type = get_args(instance.__orig_class__)[0]
    if orig_param_type is Any and sample_value is not None:
        orig_param_type = type(sample_value)
    return orig_param_type  # type: ignore


TInputArgs = ParamSpec("TInputArgs")
TReturnVal = TypeVar("TReturnVal")

def copy_sig(wrapper: Callable[TInputArgs, Any]) -> Callable[[Callable[..., TReturnVal]], Callable[TInputArgs, TReturnVal]]:
    """Copies docstring and signature from wrapper to func but keeps the func return value type"""

    def decorator(func: Callable[..., TReturnVal]) -> Callable[TInputArgs, TReturnVal]:
        func.__doc__ = wrapper.__doc__
        return func

    return decorator
