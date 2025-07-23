from .naming import NamingConvention as WeaviateNamingConvention


class NamingConvention(WeaviateNamingConvention):
    """Case insensitive naming convention for Weaviate. Lower cases all identifiers"""

    @property
    def is_case_sensitive(self) -> bool:
        return False

    def _lowercase_property(self, identifier: str) -> str:
        """Lowercase the whole property to become case insensitive"""
        return identifier.lower()
