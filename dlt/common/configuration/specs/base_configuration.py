import copy
import inspect
import contextlib
import dataclasses
import warnings

from collections.abc import Mapping as C_Mapping
from typing import (
    Callable,
    List,
    Optional,
    Union,
    Any,
    Dict,
    Iterator,
    MutableMapping,
    Type,
    TYPE_CHECKING,
    overload,
    ClassVar,
    TypeVar,
)
from typing_extensions import get_args, get_origin, dataclass_transform, Annotated, TypeAlias
from functools import wraps

if TYPE_CHECKING:
    TDtcField = dataclasses.Field[Any]
else:
    TDtcField = dataclasses.Field

from dlt.common.typing import (
    AnyType,
    TAnyClass,
    extract_inner_type,
    is_annotated,
    is_final_type,
    is_optional_type,
    is_union_type,
)
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingTypeHintException,
    ConfigFieldTypeHintNotSupported,
)


# forward class declaration
_F_BaseConfiguration: Any = type(object)
_F_ContainerInjectableContext: Any = type(object)
_T = TypeVar("_T", bound="BaseConfiguration")
_C = TypeVar("_C", bound="CredentialsConfiguration")


class NotResolved:
    """Used in type annotations to indicate types that should not be resolved."""

    def __init__(self, not_resolved: bool = True):
        self.not_resolved = not_resolved

    def __bool__(self) -> bool:
        return self.not_resolved


def is_hint_not_resolved(hint: AnyType) -> bool:
    """Checks if hint should NOT be resolved. Final and types annotated like

    >>> Annotated[str, NotResolved()]

    are not resolved.
    """
    if is_final_type(hint):
        return True

    if is_annotated(hint):
        _, *a_m = get_args(hint)
        for annotation in a_m:
            if isinstance(annotation, NotResolved):
                return bool(annotation)
    return False


def is_base_configuration_inner_hint(inner_hint: Type[Any]) -> bool:
    return inspect.isclass(inner_hint) and issubclass(inner_hint, BaseConfiguration)


def is_context_inner_hint(inner_hint: Type[Any]) -> bool:
    return inspect.isclass(inner_hint) and issubclass(inner_hint, ContainerInjectableContext)


def is_credentials_inner_hint(inner_hint: Type[Any]) -> bool:
    return inspect.isclass(inner_hint) and issubclass(inner_hint, CredentialsConfiguration)


def get_config_if_union_hint(hint: Type[Any]) -> Type[Any]:
    if is_union_type(hint):
        return next((t for t in get_args(hint) if is_base_configuration_inner_hint(t)), None)
    return None


def is_valid_hint(hint: Type[Any]) -> bool:
    if get_origin(hint) is ClassVar:
        # class vars are skipped by dataclass
        return True

    if is_hint_not_resolved(hint):
        # all hints that are not resolved are valid
        return True

    hint = extract_inner_type(hint)
    hint = get_config_if_union_hint(hint) or hint
    hint = get_origin(hint) or hint

    if hint is Any:
        return True
    if is_base_configuration_inner_hint(hint):
        return True
    with contextlib.suppress(TypeError):
        py_type_to_sc_type(hint)
        return True
    return False


def extract_inner_hint(hint: Type[Any], preserve_new_types: bool = False) -> Type[Any]:
    # extract hint from Optional / Literal / NewType hints
    inner_hint = extract_inner_type(hint, preserve_new_types)
    # get base configuration from union type
    inner_hint = get_config_if_union_hint(inner_hint) or inner_hint
    # extract origin from generic types (ie List[str] -> List)
    return get_origin(inner_hint) or inner_hint


def is_secret_hint(hint: Type[Any]) -> bool:
    is_secret = False
    if hasattr(hint, "__name__"):
        is_secret = hint.__name__ == "TSecretValue"
    if not is_secret:
        is_secret = is_credentials_inner_hint(hint)
    if not is_secret:
        inner_hint = extract_inner_hint(hint, preserve_new_types=True)
        # something was encapsulated
        if inner_hint is not hint:
            is_secret = is_secret_hint(inner_hint)
    return is_secret


@overload
def configspec(cls: Type[TAnyClass], init: bool = True) -> Type[TAnyClass]: ...


@overload
def configspec(
    cls: None = ..., init: bool = True
) -> Callable[[Type[TAnyClass]], Type[TAnyClass]]: ...


@dataclass_transform(eq_default=False, field_specifiers=(dataclasses.Field, dataclasses.field))
def configspec(
    cls: Optional[Type[Any]] = None, init: bool = True
) -> Union[Type[TAnyClass], Callable[[Type[TAnyClass]], Type[TAnyClass]]]:
    """Converts (via derivation) any decorated class to a Python dataclass that may be used as a spec to resolve configurations

    __init__ method is synthesized by default. `init` flag is ignored if the decorated class implements custom __init__ as well as
    when any of base classes has no synthesized __init__

    All fields must have default values. This decorator will add `None` default values that miss one.

    In comparison the Python dataclass, a spec implements full dictionary interface for its attributes, allows instance creation from ie. strings
    or other types (parsing, deserialization) and control over configuration resolution process. See `BaseConfiguration` and CredentialsConfiguration` for
    more information.

    """

    def wrap(cls: Type[TAnyClass]) -> Type[TAnyClass]:
        cls.__hint_resolvers__ = {}  # type: ignore[attr-defined]
        is_context = issubclass(cls, _F_ContainerInjectableContext)
        # if type does not derive from BaseConfiguration then derive it
        with contextlib.suppress(NameError):
            if not issubclass(cls, BaseConfiguration):
                # keep the original module and keep defaults for fields listed in annotations
                fields = {
                    "__module__": cls.__module__,
                    "__annotations__": getattr(cls, "__annotations__", {}),
                }
                for key in fields["__annotations__"].keys():  # type: ignore[union-attr]
                    if key in cls.__dict__:
                        fields[key] = cls.__dict__[key]
                cls = type(cls.__name__, (cls, _F_BaseConfiguration), fields)
        # get all annotations without corresponding attributes and set them to None
        for ann in cls.__annotations__:
            if not hasattr(cls, ann) and not ann.startswith(("__", "_abc_")):
                warnings.warn(
                    f"Missing default value for field {ann} on {cls.__name__}. None assumed. All"
                    " fields in configspec must have default."
                )
                setattr(cls, ann, None)
        # get all attributes without corresponding annotations
        for att_name, att_value in list(cls.__dict__.items()):
            # skip callables, dunder names, class variables and some special names
            if callable(att_value):
                if hint_field_name := getattr(att_value, "__hint_for_field__", None):
                    cls.__hint_resolvers__[hint_field_name] = att_value  # type: ignore[attr-defined]
                    continue
                try:
                    # Allow callable config objects (e.g. Incremental)
                    if not isinstance(att_value, BaseConfiguration):
                        continue
                except NameError:
                    # Dealing with BaseConfiguration itself before it is defined
                    continue
            if not att_name.startswith(("__", "_abc_")) and not isinstance(
                att_value, (staticmethod, classmethod, property)
            ):
                if att_name not in cls.__annotations__:
                    raise ConfigFieldMissingTypeHintException(att_name, cls)
                hint = cls.__annotations__[att_name]

                # context can have any type
                if not is_valid_hint(hint) and not is_context:
                    raise ConfigFieldTypeHintNotSupported(att_name, cls, hint)
                if isinstance(att_value, BaseConfiguration):
                    # Wrap config defaults in default_factory to work around dataclass
                    # blocking mutable defaults
                    def default_factory(att_value=att_value):  # type: ignore[no-untyped-def]
                        return att_value.copy()

                    setattr(cls, att_name, dataclasses.field(default_factory=default_factory))

        # We don't want to overwrite user's __init__ method
        # Create dataclass init only when not defined in the class
        # NOTE: any class without synthesized __init__ breaks the creation chain
        has_default_init = super(cls, cls).__init__ == cls.__init__  # type: ignore[misc]
        base_params = getattr(cls, "__dataclass_params__", None)  # cls.__init__ is object.__init__
        synth_init = init and ((not base_params or base_params.init) and has_default_init)
        if synth_init != init and has_default_init:
            warnings.warn(
                f"__init__ method will not be generated on {cls.__name__} because base class didn't"
                " synthesize __init__. Please correct `init` flag in confispec decorator. You are"
                " probably receiving incorrect __init__ signature for type checking"
            )
        # do not generate repr as it may contain secret values
        return dataclasses.dataclass(cls, init=synth_init, eq=False, repr=False)  # type: ignore

    # called with parenthesis
    if cls is None:
        return wrap

    return wrap(cls)


@configspec
class BaseConfiguration(MutableMapping[str, Any]):
    __is_resolved__: bool = dataclasses.field(default=False, init=False, repr=False, compare=False)
    """True when all config fields were resolved and have a specified value type"""
    __exception__: Exception = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    """Holds the exception that prevented the full resolution"""
    __section__: ClassVar[str] = None
    """Obligatory section used by config providers when searching for keys, always present in the search path"""
    __config_gen_annotations__: ClassVar[List[str]] = []
    """Additional annotations for config generator, currently holds a list of fields of interest that have defaults"""
    __dataclass_fields__: ClassVar[Dict[str, TDtcField]]
    """Typing for dataclass fields"""
    __hint_resolvers__: ClassVar[Dict[str, Callable[["BaseConfiguration"], Type[Any]]]] = {}

    def parse_native_representation(self, native_value: Any) -> None:
        """Initialize the configuration fields by parsing the `native_value` which should be a native representation of the configuration
        or credentials, for example database connection string or JSON serialized GCP service credentials file.

        #### Args:
            native_value (Any): A native representation of the configuration

        Raises:
            NotImplementedError: This configuration does not have a native representation
            ValueError: The value provided cannot be parsed as native representation
        """
        raise NotImplementedError()

    def to_native_representation(self) -> Any:
        """Represents the configuration instance in its native form ie. database connection string or JSON serialized GCP service credentials file.

        Raises:
            NotImplementedError: This configuration does not have a native representation

        Returns:
            Any: A native representation of the configuration
        """
        raise NotImplementedError()

    @classmethod
    def _get_resolvable_dataclass_fields(cls) -> Iterator[TDtcField]:
        """Yields all resolvable dataclass fields in the order they should be resolved"""
        # Sort dynamic type hint fields last because they depend on other values
        yield from sorted(
            (f for f in cls.__dataclass_fields__.values() if cls.__is_valid_field(f)),
            key=lambda f: f.name in cls.__hint_resolvers__,
        )

    @classmethod
    def get_resolvable_fields(cls) -> Dict[str, type]:
        """Returns a mapping of fields to their type hints. Dunders should not be resolved and are not returned"""
        return {f.name: f.type for f in cls._get_resolvable_dataclass_fields()}

    def is_resolved(self) -> bool:
        return self.__is_resolved__

    def is_partial(self) -> bool:
        """Returns True when any required resolvable field has its value missing."""
        if self.__is_resolved__:
            return False
        # check if all resolvable fields have value
        return any(
            field
            for field, hint in self.get_resolvable_fields().items()
            if getattr(self, field) is None and not is_optional_type(hint)
        )

    def resolve(self) -> None:
        self.call_method_in_mro("on_resolved")
        self.__is_resolved__ = True

    def copy(self: _T) -> _T:
        """Returns a deep copy of the configuration instance"""
        return copy.deepcopy(self)

    # implement dictionary-compatible interface on top of dataclass

    def __getitem__(self, __key: str) -> Any:
        if self.__has_attr(__key):
            return getattr(self, __key)
        else:
            raise KeyError(__key)

    def __setitem__(self, __key: str, __value: Any) -> None:
        if self.__has_attr(__key):
            setattr(self, __key, __value)
        else:
            try:
                if not self.__ignore_set_unknown_keys:
                    # assert getattr(self, "__ignore_set_unknown_keys") is not None
                    raise KeyError(__key)
            except AttributeError:
                # __ignore_set_unknown_keys attribute may not be present at the moment of checking, __init__ of BaseConfiguration is not typically called
                raise KeyError(__key)

    def __delitem__(self, __key: str) -> None:
        raise KeyError("Configuration fields cannot be deleted")

    def __iter__(self) -> Iterator[str]:
        """Iterator or valid key names"""
        return map(
            lambda field: field.name,
            filter(lambda val: self.__is_valid_field(val), self.__dataclass_fields__.values()),
        )

    def __len__(self) -> int:
        return sum(1 for _ in self.__iter__())

    def update(self, other: Any = (), /, **kwds: Any) -> None:
        try:
            self.__ignore_set_unknown_keys = True
            super().update(other, **kwds)
        finally:
            self.__ignore_set_unknown_keys = False

    # helper functions

    def __has_attr(self, __key: str) -> bool:
        return __key in self.__dataclass_fields__ and self.__is_valid_field(
            self.__dataclass_fields__[__key]
        )

    @staticmethod
    def __is_valid_field(field: TDtcField) -> bool:
        return not field.name.startswith("__") and field._field_type is dataclasses._FIELD  # type: ignore

    def call_method_in_mro(config, method_name: str) -> None:
        # python multi-inheritance is cooperative and this would require that all configurations cooperatively
        # call each other class_method_name. this is not at all possible as we do not know which configs in the end will
        # be mixed together.

        # get base classes in order of derivation
        mro = type.mro(type(config))
        for c in mro:
            # check if this class implements on_resolved (skip pure inheritance to not do double work)
            if method_name in c.__dict__ and callable(getattr(c, method_name)):
                # pass right class instance
                c.__dict__[method_name](config)


_F_BaseConfiguration = BaseConfiguration


@configspec
class CredentialsConfiguration(BaseConfiguration):
    """Base class for all credentials. Credentials are configurations that may be stored only by providers supporting secrets."""

    __section__: ClassVar[str] = "credentials"

    @classmethod
    def from_init_value(cls: Type[_C], init_value: Any = None) -> _C:
        """Initializes credentials from `init_value`

        Init value may be a native representation of the credentials or a dict. In case of native representation (for example a connection string or JSON with service account credentials)
        a `parse_native_representation` method will be used to parse it. In case of a dict, the credentials object will be updated with key: values of the dict.
        Unexpected values in the dict will be ignored.

        Credentials will be marked as resolved if all required fields are set.
        """
        # create an instance
        self = cls()
        self._apply_init_value(init_value)
        return self

    def to_native_credentials(self) -> Any:
        """Returns native credentials object.

        By default calls `to_native_representation` method.
        """
        return self.to_native_representation()

    def _apply_init_value(self, init_value: Any = None) -> None:
        if isinstance(init_value, C_Mapping):
            self.update(init_value)
        elif init_value is not None:
            self.parse_native_representation(init_value)
        else:
            return
        if not self.is_partial():
            self.resolve()

    def __str__(self) -> str:
        """Get string representation of credentials to be displayed, with all secret parts removed"""
        return super().__str__()


class CredentialsWithDefault:
    """A mixin for credentials that can be instantiated from default ie. from well known env variable with credentials"""

    def has_default_credentials(self) -> bool:
        return hasattr(self, "_default_credentials")

    def _set_default_credentials(self, credentials: Any) -> None:
        self._default_credentials = credentials

    def default_credentials(self) -> Any:
        if self.has_default_credentials():
            return self._default_credentials
        return None


@configspec
class ContainerInjectableContext(BaseConfiguration):
    """Base class for all configurations that may be injected from a Container. Injectable configuration is called a context"""

    can_create_default: ClassVar[bool] = True
    """If True, `Container` is allowed to create default context instance, if none exists"""
    global_affinity: ClassVar[bool] = False
    """If True, `Container` will create context that will be visible in any thread. If False, per thread context is created"""

    def add_extras(self) -> None:
        """Called right after context was added to the container. Benefits mostly the config provider injection context which adds extra providers using the initial ones."""
        pass


_F_ContainerInjectableContext = ContainerInjectableContext


TSpec = TypeVar("TSpec", bound=BaseConfiguration)
THintResolver = Callable[[TSpec], Type[Any]]


def resolve_type(field_name: str) -> Callable[[THintResolver[TSpec]], THintResolver[TSpec]]:
    def decorator(func: THintResolver[TSpec]) -> THintResolver[TSpec]:
        func.__hint_for_field__ = field_name  # type: ignore[attr-defined]

        @wraps(func)
        def wrapper(self: TSpec) -> Type[Any]:
            return func(self)

        return wrapper

    return decorator
