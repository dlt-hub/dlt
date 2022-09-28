import dataclasses
from typing import Any, Dict, Iterator, MutableMapping, Type, TYPE_CHECKING

if TYPE_CHECKING:
    TDtcField = dataclasses.Field[Any]
else:
    TDtcField = dataclasses.Field

from dlt.common.typing import TAny
from dlt.common.configuration.exceptions import ConfigFieldTypingMissingException


def configspec(cls: Type[TAny] = None, /, *, init: bool = False) -> Type[TAny]:

    def wrap(cls: Type[TAny]) -> Type[TAny]:
        # get all annotations without corresponding attributes and set them to None
        for ann in cls.__annotations__:
            if not hasattr(cls, ann):
                setattr(cls, ann, None)
        # get all attributes without corresponding annotations
        for att_name, att in cls.__dict__.items():
            if not callable(att) and not att_name.startswith(("__", "_abc_impl")) and att_name not in cls.__annotations__:
                raise ConfigFieldTypingMissingException(att_name, cls)
        return dataclasses.dataclass(cls, init=init, eq=False)  # type: ignore

    # called with parenthesis
    if cls is None:
        return wrap  # type: ignore

    return wrap(cls)


@configspec
class BaseConfiguration(MutableMapping[str, Any]):

    # will be set to true if not all config entries could be resolved
    __is_partial__: bool = dataclasses.field(default = True, init=False, repr=False)
    # namespace used by config providers when searching for keys
    __namespace__: str = dataclasses.field(default = None, init=False, repr=False)
    __dataclass_fields__: Dict[str, TDtcField]

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
        raise NotImplementedError()

    def to_native_representation(self) -> Any:
        """Represents the configuration instance in its native form ie. database connection string or JSON serialized GCP service credentials file.

        Raises:
            NotImplementedError: This configuration does not have a native representation

        Returns:
            Any: A native representation of the configuration
        """
        raise NotImplementedError()

    # implement dictionary-compatible interface on top of dataclass

    def __getitem__(self, __key: str) -> Any:
        if self._has_attr(__key):
            return getattr(self, __key)
        else:
            raise KeyError(__key)

    def __setitem__(self, __key: str, __value: Any) -> None:
        if self._has_attr(__key):
            setattr(self, __key, __value)
        else:
            if not self.__ignore_set_unknown_keys:
                raise KeyError(__key)

    def __delitem__(self, __key: str) -> None:
        raise NotImplementedError("Configuration fields cannot be deleted")

    def __iter__(self) -> Iterator[str]:
        return filter(lambda k: not k.startswith("__"), self.__dataclass_fields__.__iter__())

    def __len__(self) -> int:
        return sum(1 for _ in self.__iter__())

    def update(self, other: Any = (), /, **kwds: Any) -> None:
        try:
            self.__ignore_set_unknown_keys = True
            super().update(other, **kwds)
        finally:
            self.__ignore_set_unknown_keys = False

    # helper functions

    def _has_attr(self, __key: str) -> bool:
        return __key in self.__dataclass_fields__ and not __key.startswith("__")


@configspec
class CredentialsConfiguration(BaseConfiguration):
    pass
