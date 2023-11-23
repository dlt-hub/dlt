import re
from typing import Any, List, Sequence
from functools import lru_cache

from dlt.common.normalizers.naming.naming import NamingConvention as BaseNamingConvention


class NamingConvention(BaseNamingConvention):
    _RE_UNDERSCORES = re.compile("__+")
    _RE_LEADING_DIGITS = re.compile(r"^\d+")
    # _RE_ENDING_UNDERSCORES = re.compile(r"_+$")
    _RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d_]+")
    _SNAKE_CASE_BREAK_1 = re.compile("([^_])([A-Z][a-z]+)")
    _SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")
    _REDUCE_ALPHABET = ("+-*@|", "x_xal")
    _TR_REDUCE_ALPHABET = str.maketrans(_REDUCE_ALPHABET[0], _REDUCE_ALPHABET[1])

    # subsequent nested fields will be separated with the string below, applies both to field and table names
    PATH_SEPARATOR = "__"

    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        # print(f"{identifier} -> {self.shorten_identifier(identifier, self.max_length)} ({self.max_length})")
        return self._normalize_identifier(identifier, self.max_length)

    def make_path(self, *identifiers: str) -> str:
        # only non empty identifiers participate
        return self.PATH_SEPARATOR.join(filter(lambda x: x.strip(), identifiers))

    def break_path(self, path: str) -> Sequence[str]:
        return [ident for ident in path.split(self.PATH_SEPARATOR) if ident.strip()]

    @staticmethod
    @lru_cache(maxsize=None)
    def _normalize_identifier(identifier: str, max_length: int) -> str:
        """Normalizes the identifier according to naming convention represented by this function"""
        # all characters that are not letters digits or a few special chars are replaced with underscore
        normalized_ident = identifier.translate(NamingConvention._TR_REDUCE_ALPHABET)
        normalized_ident = NamingConvention._RE_NON_ALPHANUMERIC.sub("_", normalized_ident)

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
        if cls._RE_LEADING_DIGITS and cls._RE_LEADING_DIGITS.match(identifier):
            identifier = "_" + identifier

        # replace trailing _ with x
        stripped_ident = identifier.rstrip("_")
        strip_count = len(identifier) - len(stripped_ident)
        stripped_ident += "x" * strip_count

        # identifier = cls._RE_ENDING_UNDERSCORES.sub("x", identifier)
        # replace consecutive underscores with single one to prevent name clashes with PATH_SEPARATOR
        return cls._RE_UNDERSCORES.sub("_", stripped_ident)
