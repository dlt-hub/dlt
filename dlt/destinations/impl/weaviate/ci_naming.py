from .naming import NamingConvention as WeaviateNamingConvention


class NamingConvention(WeaviateNamingConvention):
    def __init__(self, max_length: int = None, is_case_sensitive: bool = False) -> None:
        super().__init__(max_length, is_case_sensitive)

    def _lowercase_property(self, identifier: str) -> str:
        """Lowercase the whole property to become case insensitive"""
        return identifier.lower()
