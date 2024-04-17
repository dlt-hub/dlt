from typing import Any, Sequence

from dlt.common.normalizers.naming.naming import NamingConvention as BaseNamingConvention


class NamingConvention(BaseNamingConvention):
    PATH_SEPARATOR = "▶"

    _CLEANUP_TABLE = str.maketrans(".\n\r'\"▶", "______")

    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        norm_identifier = identifier.translate(self._CLEANUP_TABLE)
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)

    def make_path(self, *identifiers: Any) -> str:
        return self.PATH_SEPARATOR.join(filter(lambda x: x.strip(), identifiers))

    def break_path(self, path: str) -> Sequence[str]:
        return [ident for ident in path.split(self.PATH_SEPARATOR) if ident.strip()]
