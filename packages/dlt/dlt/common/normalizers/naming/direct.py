from typing import ClassVar

from dlt.common.normalizers.naming.naming import NamingConvention as BaseNamingConvention


class NamingConvention(BaseNamingConvention):
    """Case sensitive naming convention that maps source identifiers to destination identifiers with
    only minimal changes. New line characters, double and single quotes are replaced with underscores.

    Uses ▶ as path separator.
    """

    PATH_SEPARATOR: ClassVar[str] = "▶"
    _CLEANUP_TABLE = str.maketrans("\n\r'\"▶", "_____")

    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        norm_identifier = identifier.translate(self._CLEANUP_TABLE)
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)

    @property
    def is_case_sensitive(self) -> bool:
        return True
