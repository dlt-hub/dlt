from dlt.common.normalizers.naming.naming import NamingConvention as BaseNamingConvention


class NamingConvention(BaseNamingConvention):
    PATH_SEPARATOR = "__"

    _CLEANUP_TABLE = str.maketrans(".\n\r'\"▶", "______")

    @property
    def is_case_sensitive(self) -> bool:
        return True

    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        norm_identifier = identifier.translate(self._CLEANUP_TABLE).upper()
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)
