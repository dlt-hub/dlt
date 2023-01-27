import inspect
import contextlib
import dataclasses
from typing import Callable, List, Optional, Union, Any, Dict, Iterator, MutableMapping, Type, TYPE_CHECKING, get_args, get_origin, overload, ClassVar

if TYPE_CHECKING:
    TDtcField = dataclasses.Field[Any]
else:
    TDtcField = dataclasses.Field

from dlt.common.typing import TAnyClass, TSecretValue, extract_inner_type, is_optional_type
from dlt.common.schema.utils import py_type_to_sc_type
from dlt.common.configuration.exceptions import ConfigFieldMissingTypeHintException, ConfigFieldTypeHintNotSupported


# forward class declaration
_F_BaseConfiguration: Any = type(object)
_F_ContainerInjectableContext: Any = type(object)


def is_base_configuration_inner_hint(inner_hint: Type[Any]) -> bool:
    return inspect.isclass(inner_hint) and issubclass(inner_hint, BaseConfiguration)


def is_context_inner_hint(inner_hint: Type[Any]) -> bool:
    return inspect.isclass(inner_hint) and issubclass(inner_hint, ContainerInjectableContext)


def is_credentials_inner_hint(inner_hint: Type[Any]) -> bool:
    return inspect.isclass(inner_hint) and issubclass(inner_hint, CredentialsConfiguration)


def get_config_if_union_hint(hint: Type[Any]) -> Type[Any]:
    if get_origin(hint) is Union:
        return next((t for t in get_args(hint) if is_base_configuration_inner_hint(t)), None)
    return None


def is_valid_hint(hint: Type[Any]) -> bool:
    hint = extract_inner_type(hint)
    hint = get_config_if_union_hint(hint) or hint
    hint = get_origin(hint) or hint

    if hint is Any:
        return True
    if hint is ClassVar:
        # class vars are skipped by dataclass
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
    is_secret =  hint is TSecretValue
    if not is_secret:
        hint = extract_inner_hint(hint, preserve_new_types=True)
        is_secret = hint is TSecretValue or is_credentials_inner_hint(hint)
    return is_secret


@overload
def configspec(cls: Type[TAnyClass], /, *, init: bool = False) -> Type[TAnyClass]:
    ...


@overload
def configspec(cls: None = ..., /, *, init: bool = False) -> Callable[[Type[TAnyClass]], Type[TAnyClass]]:
    ...


def configspec(cls: Optional[Type[Any]] = None, /, *, init: bool = False) -> Union[Type[TAnyClass], Callable[[Type[TAnyClass]], Type[TAnyClass]]]:

    def wrap(cls: Type[TAnyClass]) -> Type[TAnyClass]:
        is_context = issubclass(cls, _F_ContainerInjectableContext)
        # if type does not derive from BaseConfiguration then derive it
        with contextlib.suppress(NameError):
            if not issubclass(cls, BaseConfiguration):
                # keep the original module
                fields = {"__module__": cls.__module__, "__annotations__": getattr(cls, "__annotations__", {})}
                cls = type(cls.__name__, (cls, _F_BaseConfiguration), fields)
        # get all annotations without corresponding attributes and set them to None
        for ann in cls.__annotations__:
            if not hasattr(cls, ann) and not ann.startswith(("__", "_abc_impl")):
                setattr(cls, ann, None)
        # get all attributes without corresponding annotations
        for att_name, att_value in cls.__dict__.items():
            # skip callables, dunder names, class variables and some special names
            if not callable(att_value) and not att_name.startswith(("__", "_abc_impl")):
                if att_name not in cls.__annotations__:
                    raise ConfigFieldMissingTypeHintException(att_name, cls)
                hint = cls.__annotations__[att_name]
                # context can have any type
                if not is_valid_hint(hint) and not is_context:
                    raise ConfigFieldTypeHintNotSupported(att_name, cls, hint)
        # do not generate repr as it may contain secret values
        return dataclasses.dataclass(cls, init=init, eq=False, repr=False)  # type: ignore

    # called with parenthesis
    if cls is None:
        return wrap

    return wrap(cls)


@configspec
class BaseConfiguration(MutableMapping[str, Any]):

    __is_resolved__: bool = dataclasses.field(default = False, init=False, repr=False)
    """True when all config fields were resolved and have a specified value type"""
    __namespace__: str = dataclasses.field(default = None, init=False, repr=False)
    """Namespace used by config providers when searching for keys"""
    __exception__: Exception = dataclasses.field(default = None, init=False, repr=False)
    """Holds the exception that prevented the full resolution"""
    __config_gen_annotations__: ClassVar[List[str]] = None
    """Additional annotations for config generator, currently holds a list of fields of interest that have defaults"""

    def parse_native_representation(self, native_value: Any) -> None:
        """Initialize the configuration fields by parsing the `native_value` which should be a native representation of the configuration
        or credentials, for example database connection string or JSON serialized GCP service credentials file.

        ### Args:
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

    def get_resolvable_fields(self) -> Dict[str, type]:
        """Returns a mapping of fields to their type hints. Dunder should not be resolved and are not returned"""
        return {f.name:f.type for f in self.__fields_dict().values() if not f.name.startswith("__")}

    def is_resolved(self) -> bool:
        return self.__is_resolved__

    def is_partial(self) -> bool:
        """Returns True when any required resolvable field has its value missing."""
        if self.__is_resolved__:
            return False
        # check if all resolvable fields have value
        return any(
            field for field, hint in self.get_resolvable_fields().items() if getattr(self, field) is None and not is_optional_type(hint)
        )

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
        return filter(lambda k: not k.startswith("__"), self.__fields_dict().__iter__())

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
        return __key in self.__fields_dict() and not __key.startswith("__")

    def __fields_dict(self) -> Dict[str, TDtcField]:
        return self.__dataclass_fields__  # type: ignore


_F_BaseConfiguration = BaseConfiguration


@configspec
class CredentialsConfiguration(BaseConfiguration):
    """Base class for all credentials. Credentials are configurations that may be stored only by providers supporting secrets."""

    __namespace__: str = "credentials"

    def __str__(self) -> str:
        """Get string representation of credentials to be displayed, with all secret parts removed """
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
    """Base class for all configurations that may be injected from Container. Injectable configurations are called contexts"""

    # If True, `Container` is allowed to create default context instance, if none exists
    can_create_default: ClassVar[bool] = True


_F_ContainerInjectableContext = ContainerInjectableContext
