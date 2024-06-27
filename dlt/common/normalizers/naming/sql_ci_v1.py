from dlt.common.normalizers.naming.sql_cs_v1 import NamingConvention as SqlCsNamingConvention


class NamingConvention(SqlCsNamingConvention):
    def __init__(self, max_length: int = None) -> None:
        """A variant of sql_cs which lower cases all identifiers."""

        super().__init__(max_length)
        self.is_case_sensitive = False

    def normalize_identifier(self, identifier: str) -> str:
        return super().normalize_identifier(identifier).lower()
