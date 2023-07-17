import abc
import typing as t

from dlt.common.typing import DictStrAny, StrAny, TDataItem

if t.TYPE_CHECKING:
    from dlt.common.schema import Schema
else:
    Schema = t.Any

# type definitions for json normalization function

# iterator of form ((table_name, parent_table), dict) must be returned from normalization function
TNormalizedRowIterator = t.Iterator[t.Tuple[t.Tuple[str, str], StrAny]]

# type var for data item normalizer config
TNormalizerConfig = t.TypeVar("TNormalizerConfig", bound=t.Any)


class DataItemNormalizer(abc.ABC, t.Generic[TNormalizerConfig]):
    @abc.abstractmethod
    def __init__(self, schema: Schema) -> None:
        pass

    @abc.abstractmethod
    def normalize_data_item(
        self, item: TDataItem, load_id: str, table_name: str
    ) -> TNormalizedRowIterator:
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


class SupportsDataItemNormalizer(t.Protocol):
    """Expected of modules defining data item normalizer"""

    DataItemNormalizer: t.Type[DataItemNormalizer[t.Any]]
    """A class with a name DataItemNormalizer deriving from normalizers.json.DataItemNormalizer"""


def wrap_in_dict(item: t.Any) -> DictStrAny:
    """Wraps `item` that is not a dictionary into dictionary that can be json normalized"""
    return {"value": item}
