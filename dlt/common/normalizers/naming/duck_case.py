import re
from functools import lru_cache

from dlt.common.normalizers.naming.snake_case import NamingConvention as BaseNamingConvention


class NamingConvention(BaseNamingConvention):

    _RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d_+-]+")
    _REDUCE_ALPHABET = ("*@|", "xal")
    _TR_REDUCE_ALPHABET = str.maketrans(_REDUCE_ALPHABET[0], _REDUCE_ALPHABET[1])

    @staticmethod
    @lru_cache(maxsize=None)
    def _normalize_identifier(identifier: str, max_length: int) -> str:
        """Normalizes the identifier according to naming convention represented by this function"""
        # all characters that are not letters digits or a few special chars are replaced with underscore
        normalized_ident = identifier.translate(NamingConvention._TR_REDUCE_ALPHABET)
        normalized_ident = NamingConvention._RE_NON_ALPHANUMERIC.sub("_", normalized_ident)

        # shorten identifier
        return NamingConvention.shorten_identifier(
            NamingConvention._to_snake_case(normalized_ident),
            identifier,
            max_length
        )
