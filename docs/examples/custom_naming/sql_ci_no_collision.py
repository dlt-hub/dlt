from typing import ClassVar

from dlt.common.normalizers.naming.sql_cs_v1 import NamingConvention as SqlNamingConvention
from dlt.common.schema.typing import DLT_NAME_PREFIX


class NamingConvention(SqlNamingConvention):
    """Case insensitive naming convention with all identifiers lowercases but with unique short tag added"""

    # we will reuse the code we use for shortening
    # 1 in 100 prob of collision for identifiers identical after normalization
    _DEFAULT_COLLISION_PROB: ClassVar[float] = 0.01

    def normalize_identifier(self, identifier: str) -> str:
        # compute unique tag on original (not normalized) identifier
        # NOTE: you may wrap method below in lru_cache if you often normalize the same names
        tag = self._compute_tag(identifier, self._DEFAULT_COLLISION_PROB)
        # lower case
        norm_identifier = identifier.lower()
        # add tag if (not a dlt identifier) and tag was not added before (simple heuristics)
        if "_4" in norm_identifier:
            _, existing_tag = norm_identifier.rsplit("_4", 1)
            has_tag = len(existing_tag) == len(tag)
        else:
            has_tag = False
        if not norm_identifier.startswith(DLT_NAME_PREFIX) and not has_tag:
            norm_identifier = norm_identifier + "_4" + tag
        # run identifier through standard sql cleaning and shortening
        return super().normalize_identifier(norm_identifier)

    @property
    def is_case_sensitive(self) -> bool:
        # switch the naming convention to case insensitive
        return False
