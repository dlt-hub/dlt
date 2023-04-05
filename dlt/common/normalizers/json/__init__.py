import abc
from typing import Any, Generic, Type, Iterator, Tuple, Callable, Protocol, TYPE_CHECKING, TypeVar

from dlt.common.typing import DictStrAny, TDataItem, StrAny
if TYPE_CHECKING:
    from dlt.common.schema import Schema
else:
    Schema = Any

# type definitions for json normalization function

# iterator of form ((table_name, parent_table), dict) must be returned from normalization function
TNormalizedRowIterator = Iterator[Tuple[Tuple[str, str], StrAny]]

# type var for data item normalizer config
TNormalizerConfig = TypeVar("TNormalizerConfig", bound=Any)

class DataItemNormalizer(abc.ABC, Generic[TNormalizerConfig]):

    @abc.abstractmethod
    def __init__(self, schema: Schema) -> None:
        pass

    @abc.abstractmethod
    def normalize_data_item(self, item: TDataItem, load_id: str, table_name: str) -> TNormalizedRowIterator:
        pass

    @abc.abstractmethod
    def extend_schema(self) -> None:
        pass

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


def wrap_in_dict(item: Any) -> DictStrAny:
    """Wraps `item` that is not a dictionary into dictionary that can be json normalized"""
    return {"value": item}
