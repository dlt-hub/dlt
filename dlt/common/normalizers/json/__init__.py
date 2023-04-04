import abc
from typing import Any, Type, Iterator, Tuple, Callable, Protocol, TYPE_CHECKING

from dlt.common.typing import DictStrAny, TDataItem, StrAny
if TYPE_CHECKING:
    from dlt.common.schema import Schema
else:
    Schema = Any

# type definitions for json normalization function

# iterator of form ((table_name, parent_table), dict) must be returned from normalization function
TNormalizedRowIterator = Iterator[Tuple[Tuple[str, str], StrAny]]

# normalization function signature
# TNormalizeJSONFunc = Callable[["Schema", TDataItem, str, str], TNormalizedRowIterator]

class DataItemNormalizer(abc.ABC):

    @abc.abstractmethod
    def __init__(self, schema: Schema) -> None:
        pass

    @abc.abstractmethod
    def normalize_data_item(self, item: TDataItem, load_id: str, table_name: str) -> TNormalizedRowIterator:
        pass

    @abc.abstractmethod
    def extend_schema(self) -> None:
        pass

class SupportsDataItemNormalizer(Protocol):
    """Expected of modules defining data item normalizer"""

    DataItemNormalizer: Type[DataItemNormalizer]
    """A class with a name DataItemNormalizer deriving from normalizers.json.DataItemNormalizer"""


def wrap_in_dict(item: Any) -> DictStrAny:
    """Wraps `item` that is not a dictionary into dictionary that can be json normalized"""
    return {"value": item}
