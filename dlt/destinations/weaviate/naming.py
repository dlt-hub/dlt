import re

from dlt.common.normalizers.naming import NamingConvention as BaseNamingConvention
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention


class NamingConvention(SnakeCaseNamingConvention):
    """Normalizes identifiers according to Weaviate documentation: https://weaviate.io/developers/weaviate/config-refs/schema#class"""

    RESERVED_PROPERTIES = {
        "id": "__id",
        "_id": "___id",
        "_additional": "__additional"
    }
    _RE_UNDERSCORES = re.compile("([^_])__+")
    _STARTS_DIGIT = re.compile("^[0-9]")
    _STARTS_NON_LETTER = re.compile("^[0-9_]")

    def normalize_identifier(self, identifier: str) -> str:
        """Normalizes Weaviate property name by removing not allowed characters, replacing them by _ and contracting multiple _ into single one"""
        identifier = BaseNamingConvention.normalize_identifier(self, identifier)
        if identifier in self.RESERVED_PROPERTIES:
            return self.RESERVED_PROPERTIES[identifier]
        norm_identifier = self._base_normalize(identifier)
        if self._STARTS_DIGIT.match(norm_identifier):
            norm_identifier = "p_" + norm_identifier
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)

    def normalize_table_identifier(self, identifier: str) -> str:
        """Creates Weaviate class name. Runs property normalization and then creates capitalized case name by splitting on _"""
        identifier = BaseNamingConvention.normalize_identifier(self, identifier)
        norm_identifier = self._base_normalize(identifier)
        norm_identifier = "".join(s[0].upper() + s[1:] if s else "" for s in norm_identifier.split("_"))
        if self._STARTS_NON_LETTER.match(norm_identifier):
            norm_identifier = "C" + norm_identifier
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)

    def _base_normalize(self, identifier: str) -> str:
        # all characters that are not letters digits or a few special chars are replaced with underscore
        normalized_ident = identifier.translate(self._TR_REDUCE_ALPHABET)
        normalized_ident = self._RE_NON_ALPHANUMERIC.sub("_", normalized_ident)
        # replace trailing _ with x
        stripped_ident = normalized_ident.rstrip("_")
        strip_count = len(normalized_ident) - len(stripped_ident)
        stripped_ident += "x" * strip_count

        # replace consecutive underscores with single one to prevent name clashes with PATH_SEPARATOR
        return self._RE_UNDERSCORES.sub(r"\1_", stripped_ident)
