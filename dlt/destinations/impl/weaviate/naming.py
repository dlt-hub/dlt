import re

from dlt.common.normalizers.naming import NamingConvention as BaseNamingConvention
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention


class NamingConvention(SnakeCaseNamingConvention):
    """Normalizes identifiers according to Weaviate documentation: https://weaviate.io/developers/weaviate/config-refs/schema#class"""

    RESERVED_PROPERTIES = {"id": "__id", "_id": "___id", "_additional": "__additional"}
    _RE_UNDERSCORES = re.compile("([^_])__+")
    _STARTS_DIGIT = re.compile("^[0-9]")
    _STARTS_NON_LETTER = re.compile("^[0-9_]")
    _SPLIT_UNDERSCORE_NON_CAP = re.compile("(_[^A-Z])")

    def normalize_identifier(self, identifier: str) -> str:
        """Normalizes Weaviate property name by removing not allowed characters, replacing them by _ and contracting multiple _ into single one
        and lowercasing the first character.

        """
        identifier = BaseNamingConvention.normalize_identifier(self, identifier)
        if identifier in self.RESERVED_PROPERTIES:
            return self.RESERVED_PROPERTIES[identifier]
        norm_identifier = self._base_normalize(identifier)
        if self._STARTS_DIGIT.match(norm_identifier):
            norm_identifier = "p_" + norm_identifier
        norm_identifier = self._lowercase_property(norm_identifier)
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)

    def normalize_table_identifier(self, identifier: str) -> str:
        """Creates Weaviate class name. Runs property normalization and then creates capitalized case name by splitting on _

        https://weaviate.io/developers/weaviate/configuration/schema-configuration#create-a-class
        """
        identifier = BaseNamingConvention.normalize_identifier(self, identifier)
        norm_identifier = self._base_normalize(identifier)
        # norm_identifier = norm_identifier.strip("_")
        norm_identifier = "".join(
            s[1:2].upper() + s[2:] if s and s[0] == "_" else s
            for s in self._SPLIT_UNDERSCORE_NON_CAP.split(norm_identifier)
        )
        norm_identifier = norm_identifier[0].upper() + norm_identifier[1:]
        if self._STARTS_NON_LETTER.match(norm_identifier):
            norm_identifier = "C" + norm_identifier
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)

    def _lowercase_property(self, identifier: str) -> str:
        # lowercase the first letter to follow Weaviate guidelines on properties
        return identifier[0].lower() + identifier[1:]

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
