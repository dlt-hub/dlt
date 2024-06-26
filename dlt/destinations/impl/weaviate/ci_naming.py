from .naming import NamingConvention as WeaviateNamingConvention


class NamingConvention(WeaviateNamingConvention):
    def __init__(self, max_length: int = None) -> None:
        """Case insensitive naming convention for Weaviate. Lower cases all identifiers"""
        super().__init__(max_length)
        self.is_case_sensitive = False

    def _lowercase_property(self, identifier: str) -> str:
        """Lowercase the whole property to become case insensitive"""
        return identifier.lower()
