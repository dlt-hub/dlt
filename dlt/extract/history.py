from typing import TYPE_CHECKING, Union, Any
from dlt.extract.exceptions import InvalidHistoryAccess

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource


class History(dict[str, Any]):
    """
    A wrapper over dict that allows access to historical pipeline inputs by:
    - string key
    - DltResource reference
    - dot-access via attribute
    """

    def __getitem__(self, key: Union[str, "DltResource"]) -> Any:
        assert type(key) is str or hasattr(
            key, "__name__"
        ), "Key of a History object must be str or DltResource"

        if not isinstance(key, str):
            key = key.__name__

        try:
            return super().__getitem__(key)
        except KeyError as e:
            raise InvalidHistoryAccess(key) from e


EMPTY_HISTORY = History()
