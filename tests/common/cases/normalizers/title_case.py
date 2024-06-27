from typing import ClassVar
from dlt.common.normalizers.naming.direct import NamingConvention as DirectNamingConvention


class NamingConvention(DirectNamingConvention):
    """Test case sensitive naming that capitalizes first and last letter and leaves the rest intact"""

    PATH_SEPARATOR: ClassVar[str] = "__"

    def normalize_identifier(self, identifier: str) -> str:
        # keep prefix
        if identifier == "_dlt":
            return "_dlt"
        identifier = super().normalize_identifier(identifier)
        return identifier[0].upper() + identifier[1:-1] + identifier[-1].upper()
