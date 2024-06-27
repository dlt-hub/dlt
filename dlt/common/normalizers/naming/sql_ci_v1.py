from dlt.common.normalizers.naming.sql_cs_v1 import NamingConvention as SqlCsNamingConvention


class NamingConvention(SqlCsNamingConvention):
    """A variant of sql_cs which lower cases all identifiers."""

    def normalize_identifier(self, identifier: str) -> str:
        return super().normalize_identifier(identifier).lower()

    @property
    def is_case_sensitive(self) -> bool:
        return False
