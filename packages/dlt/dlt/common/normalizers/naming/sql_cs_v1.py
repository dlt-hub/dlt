import re
from typing import ClassVar

from dlt.common.typing import REPattern
from dlt.common.normalizers.naming.naming import NamingConvention as BaseNamingConvention


RE_UNDERSCORES = re.compile("__+")
RE_LEADING_DIGITS = re.compile(r"^\d+")
RE_ENDING_UNDERSCORES = re.compile(r"_+$")
RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d_]+")


class NamingConvention(BaseNamingConvention):
    """Generates case sensitive SQL safe identifiers, preserving the source casing.

    - Spaces around identifier are trimmed
    - Removes all ascii characters except ascii alphanumerics and underscores
    - Prepends `_` if name starts with number.
    - Removes all trailing underscores.
    - Multiples of `_` are converted into single `_`.
    """

    RE_NON_ALPHANUMERIC: ClassVar[REPattern] = RE_NON_ALPHANUMERIC
    RE_UNDERSCORES: ClassVar[REPattern] = RE_UNDERSCORES
    RE_ENDING_UNDERSCORES: ClassVar[REPattern] = RE_ENDING_UNDERSCORES

    def normalize_identifier(self, identifier: str) -> str:
        identifier = super().normalize_identifier(identifier)
        # remove non alpha characters
        norm_identifier = self.RE_NON_ALPHANUMERIC.sub("_", identifier)
        # remove leading digits
        if RE_LEADING_DIGITS.match(norm_identifier):
            norm_identifier = "_" + norm_identifier
        # remove trailing underscores to not mess with how we break paths
        if norm_identifier != "_":
            norm_identifier = self.RE_ENDING_UNDERSCORES.sub("", norm_identifier)
        # contract multiple __
        norm_identifier = self.RE_UNDERSCORES.sub("_", norm_identifier)
        return self.shorten_identifier(norm_identifier, identifier, self.max_length)

    @property
    def is_case_sensitive(self) -> bool:
        return True
