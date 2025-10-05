from __future__ import annotations
from typing import Any, Optional, Union, TYPE_CHECKING
from dlt.extract.exceptions import InvalidHistoryAccess

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource


class History:
    """
    A simple tree node:
    - Each node has a `key`, `value`, `parent`, and `children`.
    - Keys are normalized (string or DltResource).
    - `record` always creates a new child and returns it.
    - Lookup walks up the parent chain to find the first matching key.
    """

    def __init__(self,
                 key: Optional[str] = None,
                 value: Any = None,
                 parent: Optional[History] = None) -> None:
        self.key: Optional[str] = key
        self.value: Any = value
        self.parent: Optional[History] = parent
        self.children: list[History] = []

    @staticmethod
    def _normalize_key(key: Union[str, "DltResource"]) -> str:
        if isinstance(key, str):
            return key
        name = getattr(key, "__name__", None)
        if not isinstance(name, str):
            raise TypeError("Key must be str or DltResource-like (has __name__)")
        return name

    def __getitem__(self, key: Union[str, "DltResource"]) -> Any:
        normalized = self._normalize_key(key)
        node: Optional[History] = self
        while node:
            if node.key == normalized:
                return node.value
            node = node.parent
        raise InvalidHistoryAccess(normalized)

    def record(self, key: Union[str, "DltResource"], value: Any) -> History:
        """
        Always add a new child node and return it.
        """
        normalized = self._normalize_key(key)
        child = History(key=normalized, value=value, parent=self)
        self.children.append(child)
        return child

    def path(self) -> list[tuple[str, Any]]:
        """
        Return the chain of (key, value) pairs from root to this node.
        """
        out: list[tuple[str, Any]] = []
        node: Optional[History] = self
        while node and node.key is not None:
            out.append((node.key, node.value))
            node = node.parent
        return list(reversed(out))

    def __repr__(self) -> str:
        return f"<History key={self.key!r} value={self.value!r} children={len(self.children)}>"


EMPTY_HISTORY = History()
