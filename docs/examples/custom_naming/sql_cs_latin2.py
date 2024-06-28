from typing import ClassVar

# NOTE: we use regex library that supports unicode
import regex as re

from dlt.common.normalizers.naming.sql_cs_v1 import NamingConvention as SqlNamingConvention
from dlt.common.typing import REPattern


class NamingConvention(SqlNamingConvention):
    """Case sensitive naming convention which allows basic unicode characters, including latin 2 characters"""

    RE_NON_ALPHANUMERIC: ClassVar[REPattern] = re.compile(r"[^\p{Latin}\d_]+")  # type: ignore

    def normalize_identifier(self, identifier: str) -> str:
        # typically you'd change how a single
        return super().normalize_identifier(identifier)

    @property
    def is_case_sensitive(self) -> bool:
        return True
