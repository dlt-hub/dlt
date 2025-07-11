from collections.abc import Mapping as C_Mapping, Sequence as C_Sequence, Callable as C_Callable
from datetime import datetime, date  # noqa: I251
import inspect
import os
from re import Pattern as _REPattern
from types import FunctionType
from typing import (
    Callable,
    ClassVar,
    Dict,
    Any,
    Final,
    Literal,
    List,
    Mapping,
    NewType,
    Optional,
    Tuple,
    Type,
    Protocol,
    TYPE_CHECKING,
    Union,
    runtime_checkable,
    IO,
    Iterator,
    Generator,
    NamedTuple,
    Sequence,
)

from typing_extensions import (
    ForwardRef,
    Annotated,
    Never,
    ParamSpec,
    TypeAlias,
    Concatenate,
    Unpack,
    Self,
    Generic,
    get_args,
    TypeVar,
    get_origin,
    get_type_hints,
    get_origin,
    get_original_bases,
)

from typing_extensions import is_typeddict as _is_typeddict

from typing_extensions import TypedDict  # noqa: I251

try:
    from types import UnionType  # type: ignore[attr-defined]
except ImportError:
    # Since new Union syntax was introduced in Python 3.10
    # we need to substitute it here for older versions.
    # it is defined as type(int | str) but for us having it
    # as shown here should suffice because it is valid only
    # in versions of Python>=3.10.
    UnionType = Never

from typing import _SpecialGenericAlias, _GenericAlias  # type: ignore[attr-defined]
from types import GenericAlias

typingGenericAlias: Tuple[Any, ...] = (_GenericAlias, _SpecialGenericAlias, GenericAlias)


from dlt.common.pendulum import timedelta, pendulum

if TYPE_CHECKING:
    from _typeshed import StrOrBytesPath
    from typing import _TypedDict

    REPattern = _REPattern[str]
    PathLike = os.PathLike[str]
else:
    StrOrBytesPath = Any
    from typing import _TypedDictMeta as _TypedDict

    REPattern = _REPattern
    PathLike = os.PathLike


AnyType: TypeAlias = Any
CallableAny = NewType("CallableAny", Any)  # type: ignore[valid-newtype]
"""A special callable Any that returns argument but is recognized as Any type by dlt hint checkers"""
NoneType = type(None)
DictStrAny: TypeAlias = Dict[str, Any]
DictStrStr: TypeAlias = Dict[str, str]
DictStrOptionalStr: TypeAlias = Dict[str, Optional[str]]
StrAny: TypeAlias = Mapping[str, Any]  # immutable, covariant entity
StrStr: TypeAlias = Mapping[str, str]  # immutable, covariant entity
StrStrStr: TypeAlias = Mapping[str, Mapping[str, str]]  # immutable, covariant entity
AnyFun: TypeAlias = Callable[..., Any]
TFun = TypeVar("TFun", bound=AnyFun)  # any function
TAny = TypeVar("TAny", bound=Any)
TAnyFunOrGenerator = TypeVar(
    "TAnyFunOrGenerator", AnyFun, Generator[Any, Optional[Any], Optional[Any]]
)
TAnyClass = TypeVar("TAnyClass", bound=object)
TimedeltaSeconds = Union[int, float, timedelta]
# represent secret value ie. coming from Kubernetes/Docker secrets or other providers


class SecretSentinel:
    """Marks a secret type when part of type annotations"""


if TYPE_CHECKING:
    TSecretValue = Annotated[Any, SecretSentinel]
else:
    # use callable Any type for backward compatibility at runtime
    TSecretValue = Annotated[CallableAny, SecretSentinel]

TSecretStrValue = Annotated[str, SecretSentinel]

TColumnNames = Union[str, Sequence[str]]
"""A string representing a column name or a sequence of"""
TTableNames = Union[str, Sequence[str]]
"""A string representing a table name or a sequence of"""
TDataItem: TypeAlias = Any
"""A single data item as extracted from data source"""
TDataItems: TypeAlias = Union[TDataItem, List[TDataItem]]
"A single data item or a list as extracted from the data source"
TAnyDateTime = Union[pendulum.DateTime, pendulum.Date, datetime, date, str, float, int]
"""DateTime represented as pendulum/python object, ISO string or unix timestamp"""
TVariantBase = TypeVar("TVariantBase", covariant=True)
TVariantRV = Tuple[str, Any]
VARIANT_FIELD_FORMAT = "v_%s"
TFileOrPath = Union[str, PathLike, IO[Any]]
TSortOrder = Literal["asc", "desc"]
TLoaderFileFormat = Literal[
    "jsonl", "typed-jsonl", "insert_values", "parquet", "csv", "reference", "model"
]
"""known loader file formats"""

TDynHintType = TypeVar("TDynHintType")
TFunHintTemplate = Callable[[TDataItem], TDynHintType]
TTableHintTemplate = Union[TDynHintType, TFunHintTemplate[TDynHintType]]


class ConfigValueSentinel(NamedTuple):
    """Class to create singleton sentinel for config and secret injected value"""

    default_literal: str
    default_type: AnyType

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        if self.default_literal == "dlt.config.value":
            inst_ = "ConfigValue"
        else:
            inst_ = "SecretValue"
        return f"{inst_}({self.default_literal}) awaiting injection"


ConfigValue: None = ConfigValueSentinel("dlt.config.value", AnyType)  # type: ignore[assignment]
"""Config value indicating argument that may be injected by config provider. Evaluates to None when type checking"""

SecretValue: None = ConfigValueSentinel("dlt.secrets.value", TSecretValue)  # type: ignore[assignment]
"""Secret value indicating argument that may be injected by config provider. Evaluates to None when type checking"""


@runtime_checkable
class SupportsVariant(Protocol, Generic[TVariantBase]):
    """Defines variant type protocol that should be recognized by normalizers

    Variant types behave like TVariantBase type (ie. Decimal) but also implement the protocol below that is used to extract the variant value from it.
    See `Wei` type declaration which returns Decimal or str for values greater than supported by destination warehouse.
    """

    def __call__(self) -> Union[TVariantBase, TVariantRV]: ...


class SupportsHumanize(Protocol):
    def asdict(self) -> DictStrAny:
        """Represents object as dict with a schema loadable by dlt"""
        ...

    def asstr(self, verbosity: int = 0) -> str:
        """Represents object as human readable string"""
        ...


class SocketLike(Protocol):
    def send(self, data: bytes) -> int:
        """Send bytes; return number of bytes sent."""
        ...

    def recv(self, bufsize: int) -> bytes:
        """Receive up to `bufsize` bytes and return them."""
        ...

    def close(self) -> None:
        """Close the socket-like connection."""
        ...

    def settimeout(self, timeout: float) -> None:
        """Set the timeout (in seconds) for send/recv operations."""
        ...


def get_type_name(t: Type[Any]) -> str:
    """Returns a human-friendly name of type `t`"""
    if name := getattr(t, "__name__", None):
        return name  # type: ignore[no-any-return]
    t = get_origin(t) or t
    if name := getattr(t, "__name__", None):
        return name  # type: ignore[no-any-return]
    return str(t)


def is_callable_type(hint: Type[Any]) -> bool:
    """Checks if `hint` is callable: a function or callable class. This function does not descend
    into type arguments ie. if Union, Literal or NewType contain callables, those are ignored"""
    if get_origin(hint) is get_origin(Callable):
        return True
    # this skips NewType etc.
    if getattr(hint, "__module__", None) == "typing":
        return False
    if isinstance(hint, FunctionType):
        return True
    # this is how we check if __call__ is implemented in the mro
    if inspect.isclass(hint) and any("__call__" in t_.__dict__ for t_ in inspect.getmro(hint)):
        return True

    return False


def extract_type_if_modifier(t: Type[Any], preserve_annotated: bool = False) -> Optional[Type[Any]]:
    modifiers = (Final, ClassVar) if preserve_annotated else (Final, ClassVar, Annotated)
    if get_origin(t) in modifiers:
        t = get_args(t)[0]
        if m_t := extract_type_if_modifier(t):
            return m_t
        else:
            return t
    return None


def extract_supertype(t: Type[Any]) -> Optional[Type[Any]]:
    return getattr(t, "__supertype__", None)  # type: ignore[no-any-return]


def is_union_type(hint: Type[Any]) -> bool:
    # We need to handle UnionType because with Python>=3.10
    # new Optional syntax was introduced which treats Optionals
    # as unions and probably internally there is no additional
    # type hints to handle this edge case, see the examples below
    # >>> type(str | int)
    # <class 'types.UnionType'>
    # >>> type(str | None)
    # <class 'types.UnionType'>
    # type(Union[int, str])
    # <class 'typing._GenericAlias'>
    origin = get_origin(hint)
    if origin is Union or origin is UnionType:
        return True

    if inner_t := extract_type_if_modifier(hint):
        return is_union_type(inner_t)

    return False


def is_any_type(t: Type[Any]) -> bool:
    """Checks if `t` is one of recognized Any types"""
    return t in (Any, CallableAny)


def is_optional_type(t: Type[Any]) -> bool:
    origin = get_origin(t)
    is_union = origin is Union or origin is UnionType
    if is_union and type(None) in get_args(t):
        return True

    if inner_t := extract_type_if_modifier(t):
        if is_optional_type(inner_t):
            return True
        else:
            t = inner_t
    if super_t := extract_supertype(t):
        return is_optional_type(super_t)

    return False


def is_final_type(t: Type[Any]) -> bool:
    return get_origin(t) is Final


def extract_union_types(t: Type[Any], no_none: bool = False) -> List[Any]:
    if no_none:
        return [arg for arg in get_args(t) if arg is not type(None)]  # noqa: E721
    return list(get_args(t))


def is_literal_type(hint: Type[Any]) -> bool:
    if get_origin(hint) is Literal:
        return True
    if inner_t := extract_type_if_modifier(hint):
        if is_literal_type(inner_t):
            return True
        else:
            hint = inner_t
    if super_t := extract_supertype(hint):
        return is_literal_type(super_t)
    if is_union_type(hint) and is_optional_type(hint):
        return is_literal_type(get_args(hint)[0])

    return False


def get_literal_args(literal: Type[Any]) -> List[Any]:
    """Recursively get arguments from nested Literal types and return an unified list."""
    if not hasattr(literal, "__origin__") or literal.__origin__ is not Literal:
        raise ValueError(f"Provided object is not a `Literal`. Object: {literal}")

    unified_args = []

    def _get_args(literal: Type[Any]) -> None:
        for arg in get_args(literal):
            if hasattr(arg, "__origin__") and arg.__origin__ is Literal:
                _get_args(arg)
            else:
                unified_args.append(arg)

    _get_args(literal)

    return unified_args


def is_newtype_type(t: Type[Any]) -> bool:
    if hasattr(t, "__supertype__"):
        return True
    if inner_t := extract_type_if_modifier(t):
        if is_newtype_type(inner_t):
            return True
        else:
            t = inner_t
    if is_union_type(t) and is_optional_type(t):
        return is_newtype_type(get_args(t)[0])
    return False


def is_typeddict(t: Type[Any]) -> bool:
    if _is_typeddict(t):
        return True
    if inner_t := extract_type_if_modifier(t):
        return is_typeddict(inner_t)
    return False


def is_annotated(ann_type: Any) -> bool:
    try:
        return get_origin(ann_type) is Annotated
    except TypeError:
        return False


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


def extract_inner_type(
    hint: Type[Any],
    preserve_new_types: bool = False,
    preserve_literal: bool = False,
    preserve_annotated: bool = False,
) -> Type[Any]:
    """Gets the inner type from Literal, Optional, Final and NewType

    Args:
        hint (Type[Any]): Type to extract
        preserve_new_types (bool): Do not extract supertype of a NewType

    Returns:
        Type[Any]: Inner type if hint was Literal, Optional or NewType, otherwise hint
    """
    if maybe_modified := extract_type_if_modifier(hint, preserve_annotated):
        return extract_inner_type(
            maybe_modified, preserve_new_types, preserve_literal, preserve_annotated
        )
    # make sure we deal with optional directly
    if is_union_type(hint) and is_optional_type(hint):
        return extract_inner_type(
            get_args(hint)[0], preserve_new_types, preserve_literal, preserve_annotated
        )
    if is_literal_type(hint) and not preserve_literal:
        # assume that all literals are of the same type
        return type(get_args(hint)[0])
    if hasattr(hint, "__supertype__") and not preserve_new_types:
        # descend into supertypes of NewType
        return extract_inner_type(
            hint.__supertype__, preserve_new_types, preserve_literal, preserve_annotated
        )
    return hint


def get_all_types_of_class_in_union(
    hint: Any, cls: TAny, with_superclass: bool = False
) -> List[TAny]:
    """if `hint` is an Union that contains classes, return all classes that are a subclass or (optionally) superclass of cls"""
    return [
        t
        for t in get_args(hint)
        if not is_typeddict(t) and (is_subclass(t, cls) or is_subclass(cls, t) and with_superclass)
    ]


def is_generic_alias(tp: Any) -> bool:
    """Tests if type is a generic alias ie. List[str]"""
    return isinstance(tp, typingGenericAlias) and tp.__origin__ not in (
        Union,
        tuple,
        ClassVar,
        C_Callable,
    )


def is_subclass(subclass: Any, cls: Any) -> bool:
    """Return whether 'cls' is a derived from another class or is the same class.

    Will handle generic types by comparing their origins.
    """
    if is_generic_alias(subclass):
        subclass = get_origin(subclass)
    if is_generic_alias(cls):
        cls = get_origin(cls)

    if inspect.isclass(subclass) and inspect.isclass(cls):
        return issubclass(subclass, cls)
    return False


def get_generic_type_argument_from_instance(
    instance: Any, sample_value: Optional[Any] = None
) -> Type[Any]:
    """Infers type argument of a Generic class from an `instance` of that class using optional `sample_value` of the argument type

    Inference depends on the presence of __orig_class__ attribute in instance, if not present - sample_Value will be used

    Args:
        instance (Any): instance of Generic class
        sample_value (Optional[Any]): instance of type of generic class, optional

    Returns:
        Type[Any]: type argument or Any if not known
    """
    orig_param_type = Any
    if cls_ := getattr(instance, "__orig_class__", None):
        cls_ = extract_inner_type(cls_)
    elif bases_ := get_original_bases(instance.__class__):
        # instance of class deriving from generic
        cls_ = bases_[0]
    if cls_:
        orig_param_type = get_args(cls_)[0]
    if orig_param_type in (Any, CallableAny) and sample_value is not None:
        orig_param_type = type(sample_value)  # type: ignore[assignment]
    return orig_param_type  # type: ignore


TInputArgs = ParamSpec("TInputArgs")
TReturnVal = TypeVar("TReturnVal")


def copy_sig(
    wrapper: Callable[TInputArgs, Any],
) -> Callable[[Callable[..., TReturnVal]], Callable[TInputArgs, TReturnVal]]:
    """Copies docstring and signature from wrapper to func but keeps the func return value type"""

    def decorator(func: Callable[..., TReturnVal]) -> Callable[TInputArgs, TReturnVal]:
        func.__doc__ = wrapper.__doc__
        return func

    return decorator


def copy_sig_any(
    wrapper: Callable[Concatenate[TDataItem, TInputArgs], Any],
) -> Callable[
    [Callable[..., TReturnVal]], Callable[Concatenate[TDataItem, TInputArgs], TReturnVal]
]:
    """Copies docstring and signature from wrapper to func but keeps the func return value type

    It converts the type of first argument of the wrapper to Any which allows to type transformers in DltSources.
    See filesystem source readers as example
    """

    def decorator(
        func: Callable[..., TReturnVal]
    ) -> Callable[Concatenate[Any, TInputArgs], TReturnVal]:
        func.__doc__ = wrapper.__doc__
        return func

    return decorator


def add_value_to_literal(literal: Any, value: Any) -> None:
    """Extends a Literal at runtime with a new value.

    Args:
        literal (Type[Any]): Literal to extend
        value (Any): Value to add

    """
    type_args = get_args(literal)

    if value not in type_args:
        type_args += (value,)
        literal.__args__ = type_args
