import abc
from typing import Any, Dict, Generic, Type, Generator, Protocol, Tuple, TYPE_CHECKING, TypeVar

from dlt.common.typing import DictStrAny, TDataItem, StrAny
from dlt.common.data_types.typing import TDataType

if TYPE_CHECKING:
    from dlt.common.schema import Schema
else:
    Schema = Any

# type definitions for json normalization function

# iterator of form ((table_name, parent_path, ident_path), dict) must be returned from normalization function
TNormalizedRowIterator = Generator[
    Tuple[Tuple[str, Tuple[str, ...], Tuple[str, ...]], StrAny], bool, None
]

# type var for data item normalizer config
TNormalizerConfig = TypeVar("TNormalizerConfig", bound=Any)


class DataItemNormalizer(abc.ABC, Generic[TNormalizerConfig]):
    @abc.abstractmethod
    def __init__(self, schema: Schema) -> None:
        pass

    @abc.abstractmethod
    def normalize_data_item(
        self, item: TDataItem, load_id: str, table_name: str
    ) -> TNormalizedRowIterator:
        pass

    @abc.abstractmethod
    def extend_schema(self, extend_tables: bool = True) -> None:
        pass

    @abc.abstractmethod
    def extend_table(self, table_name: str) -> None:
        pass

    @abc.abstractmethod
    def remove_table(self, table_name: str) -> None:
        pass

    @property
    def py_type_to_sc_type_map(self) -> Dict[Type[Any], TDataType]:
        """Lookup from Python type to schema type. Override in subclasses."""
        return {}

    @abc.abstractmethod
    def py_type_to_sc_type(self, t: Type[Any]) -> TDataType:
        """Resolve Python type to schema type including subclass/Enum handling."""
        ...

    @abc.abstractmethod
    def can_coerce_type(self, to_type: TDataType, from_type: TDataType) -> bool:
        """Return True if `from_type` can be coerced to `to_type`."""
        ...

    @abc.abstractmethod
    def coerce_type(self, to_type: TDataType, from_type: TDataType, value: Any) -> Any:
        """Coerce `value` from `from_type` to `to_type`. Raises ValueError on failure."""
        ...

    @classmethod
    @abc.abstractmethod
    def update_normalizer_config(cls, schema: Schema, config: TNormalizerConfig) -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def get_normalizer_config(cls, schema: Schema) -> TNormalizerConfig:
        pass


class SupportsDataItemNormalizer(Protocol):
    """Expected of modules defining data item normalizer"""

    DataItemNormalizer: Type[DataItemNormalizer[Any]]
    """A class with a name DataItemNormalizer deriving from normalizers.json.DataItemNormalizer"""


def wrap_in_dict(label: str, item: Any) -> DictStrAny:
    """Wraps `item` that is not a dictionary into dictionary that can be json normalized"""
    return {label: item}


__all__ = [
    "TNormalizedRowIterator",
    "TNormalizerConfig",
    "DataItemNormalizer",
    "SupportsDataItemNormalizer",
    "wrap_in_dict",
]
