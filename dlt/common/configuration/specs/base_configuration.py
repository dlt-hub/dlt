import contextlib
import dataclasses
from typing import Any, Dict, Iterator, MutableMapping, Type, TYPE_CHECKING, get_origin

if TYPE_CHECKING:
    TDtcField = dataclasses.Field[Any]
else:
    TDtcField = dataclasses.Field

from dlt.common.typing import TAny, extract_inner_type, is_optional_type
from dlt.common.schema.utils import py_type_to_sc_type
from dlt.common.configuration.exceptions import ConfigFieldMissingTypeHintException, ConfigFieldTypeHintNotSupported


def is_valid_hint(hint: Type[Any]) -> bool:
    hint = extract_inner_type(hint)
    hint = get_origin(hint) or hint
    if hint is Any:
        return True
    if issubclass(hint, BaseConfiguration):
        return True
    with contextlib.suppress(TypeError):
        py_type_to_sc_type(hint)
        return True
    return False


def configspec(cls: Type[TAny] = None, /, *, init: bool = False) -> Type[TAny]:

    def wrap(cls: Type[TAny]) -> Type[TAny]:
        # get all annotations without corresponding attributes and set them to None
        for ann in cls.__annotations__:
            if not hasattr(cls, ann) and not ann.startswith(("__", "_abc_impl")):
                setattr(cls, ann, None)
        # get all attributes without corresponding annotations
        for att_name, att in cls.__dict__.items():
            if not callable(att) and not att_name.startswith(("__", "_abc_impl")):
                if att_name not in cls.__annotations__:
                    raise ConfigFieldMissingTypeHintException(att_name, cls)
                hint = cls.__annotations__[att_name]
                if not is_valid_hint(hint):
                    raise ConfigFieldTypeHintNotSupported(att_name, cls, hint)
        return dataclasses.dataclass(cls, init=init, eq=False)  # type: ignore

    # called with parenthesis
    if cls is None:
        return wrap  # type: ignore

    return wrap(cls)


@configspec
class BaseConfiguration(MutableMapping[str, Any]):

    # true when all config fields were resolved and have a specified value type
    __is_resolved__: bool = dataclasses.field(default = False, init=False, repr=False)
    # namespace used by config providers when searching for keys
    __namespace__: str = dataclasses.field(default = None, init=False, repr=False)

    def __init__(self) -> None:
        self.__ignore_set_unknown_keys = False

    def from_native_representation(self, native_value: Any) -> None:
        """Initialize the configuration fields by parsing the `initial_value` which should be a native representation of the configuration
        or credentials, for example database connection string or JSON serialized GCP service credentials file.

        Args:
            initial_value (Any): A native representation of the configuration

        Raises:
            NotImplementedError: This configuration does not have a native representation
            ValueError: The value provided cannot be parsed as native representation
        """
        raise ValueError()

    def to_native_representation(self) -> Any:
        """Represents the configuration instance in its native form ie. database connection string or JSON serialized GCP service credentials file.

        Raises:
            NotImplementedError: This configuration does not have a native representation

        Returns:
            Any: A native representation of the configuration
        """
        raise ValueError()

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
            if not self.__ignore_set_unknown_keys:
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


@configspec
class CredentialsConfiguration(BaseConfiguration):
    pass
