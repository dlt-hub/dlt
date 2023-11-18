from .naming import NamingConvention as WeaviateNamingConvention

class NamingConvention(WeaviateNamingConvention):
    def _lowercase_property(self, identifier: str) -> str:
        """Lowercase the whole property to become case insensitive"""
        return identifier.lower()
