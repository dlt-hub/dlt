from typing import TYPE_CHECKING, Union, Any

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource


class History(dict[str, Any]):
    """
    A wrapper over dict that allows access to historical pipeline inputs by:
    - string key
    - DltResource reference
    - dot-access via attribute
    """

    def __getattr__(self, item: str) -> Any:
        try:
            return self[item]
        except KeyError:
            raise AttributeError(f"'History' object has no attribute '{item}'")

    def __getitem__(self, key: Union[str, "DltResource"]) -> Any:
        if not isinstance(key, str):
            key = key.__name__
        return super().__getitem__(key)
