import re
from functools import lru_cache

from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention


class NamingConvention(SnakeCaseNamingConvention):
    _CLEANUP_TABLE = str.maketrans('\n\r"', "___")

    def __init__(self, max_length: int = None) -> None:
        """Case sensitive naming convention preserving all unicode characters except new line(s). Uses __ for path
        separation and will replace multiple underscores with a single one.
        """
        super().__init__(max_length)
        self.is_case_sensitive = True

    @staticmethod
    @lru_cache(maxsize=None)
    def _normalize_identifier(identifier: str, max_length: int) -> str:
        """Normalizes the identifier according to naming convention represented by this function"""

        normalized_ident = identifier.translate(NamingConvention._CLEANUP_TABLE)

        # shorten identifier
        return NamingConvention.shorten_identifier(
            NamingConvention._RE_UNDERSCORES.sub("_", normalized_ident), identifier, max_length
        )
