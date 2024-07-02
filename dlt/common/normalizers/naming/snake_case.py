import re
from functools import lru_cache
from typing import ClassVar

from dlt.common.normalizers.naming.naming import NamingConvention as BaseNamingConvention
from dlt.common.normalizers.naming.sql_cs_v1 import (
    RE_UNDERSCORES,
    RE_LEADING_DIGITS,
    RE_NON_ALPHANUMERIC,
)
from dlt.common.typing import REPattern


class NamingConvention(BaseNamingConvention):
    """Case insensitive naming convention, converting source identifiers into lower case snake case with reduced alphabet.

    - Spaces around identifier are trimmed
    - Removes all ascii characters except ascii alphanumerics and underscores
    - Prepends `_` if name starts with number.
    - Multiples of `_` are converted into single `_`.
    - Replaces all trailing `_` with `x`
    - Replaces `+` and `*` with `x`, `-` with `_`, `@` with `a` and `|` with `l`

    Uses __ as patent-child separator for tables and flattened column names.
    """

    RE_UNDERSCORES: ClassVar[REPattern] = RE_UNDERSCORES
    RE_LEADING_DIGITS: ClassVar[REPattern] = RE_LEADING_DIGITS
    RE_NON_ALPHANUMERIC: ClassVar[REPattern] = RE_NON_ALPHANUMERIC

    _SNAKE_CASE_BREAK_1 = re.compile("([^_])([A-Z][a-z]+)")
    _SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")
    _REDUCE_ALPHABET = ("+-*@|", "x_xal")
    _TR_REDUCE_ALPHABET = str.maketrans(_REDUCE_ALPHABET[0], _REDUCE_ALPHABET[1])

    @property
    def is_case_sensitive(self) -> bool:
        return False

    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        # print(f"{identifier} -> {self.shorten_identifier(identifier, self.max_length)} ({self.max_length})")
        return self._normalize_identifier(identifier, self.max_length)

    @staticmethod
    @lru_cache(maxsize=None)
    def _normalize_identifier(identifier: str, max_length: int) -> str:
        """Normalizes the identifier according to naming convention represented by this function"""
        # all characters that are not letters digits or a few special chars are replaced with underscore
        normalized_ident = identifier.translate(NamingConvention._TR_REDUCE_ALPHABET)
        normalized_ident = NamingConvention.RE_NON_ALPHANUMERIC.sub("_", normalized_ident)

        # shorten identifier
        return NamingConvention.shorten_identifier(
            NamingConvention._to_snake_case(normalized_ident), identifier, max_length
        )

    @classmethod
    def _to_snake_case(cls, identifier: str) -> str:
        # then convert to snake case
        identifier = cls._SNAKE_CASE_BREAK_1.sub(r"\1_\2", identifier)
        identifier = cls._SNAKE_CASE_BREAK_2.sub(r"\1_\2", identifier).lower()

        # leading digits will be prefixed (if regex is defined)
        if cls.RE_LEADING_DIGITS and cls.RE_LEADING_DIGITS.match(identifier):
            identifier = "_" + identifier

        # replace trailing _ with x
        stripped_ident = identifier.rstrip("_")
        strip_count = len(identifier) - len(stripped_ident)
        stripped_ident += "x" * strip_count

        # identifier = cls._RE_ENDING_UNDERSCORES.sub("x", identifier)
        # replace consecutive underscores with single one to prevent name collisions with PATH_SEPARATOR
        return cls.RE_UNDERSCORES.sub("_", stripped_ident)
