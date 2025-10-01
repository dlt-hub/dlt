from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union, Iterable
from dlt.extract.exceptions import InvalidHistoryAccess

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource


class History(dict[str, Any]):
    """
    A dict-like wrapper that provides ergonomic access to historical resource outputs
    by either a string key or a ``DltResource`` reference.

    Key normalization rules
    -----------------------
    - If ``key`` is a string, it is used as-is.
    - If ``key`` is a DltResource (or any object with a ``__name__`` attribute),
      its ``__name__`` is used.
    """

    @staticmethod
    def _normalize_key(key: Union[str, "DltResource"]) -> str:
        if isinstance(key, str):
            return key
        name = getattr(key, "__name__", None)
        if not isinstance(name, str):
            raise TypeError(
                "Key of a History object must be str or DltResource-like (has __name__)"
            )
        return name

    def __getitem__(self, key: Union[str, "DltResource"]) -> Any:
        normalized_key = self._normalize_key(key)
        try:
            return super().__getitem__(normalized_key)
        except KeyError as e:
            raise InvalidHistoryAccess(normalized_key) from e

    def record(self, key: Union[str, "DltResource"], value: Any) -> "History":
        """
        Record a resouroce result in history:
        - If ``key`` is already present: return a *new* ``History`` with the updated value.
        - If ``key`` is not present: mutate ``self`` and return ``self``.

        This way we don't mutate other history branches
        """
        normalized_key = self._normalize_key(key)
        if normalized_key in self:
            new_history = History(self)
            new_history[normalized_key] = value
            return new_history
        self[normalized_key] = value
        return self


EMPTY_HISTORY = History()
