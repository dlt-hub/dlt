from typing import ClassVar, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, known_sections
from dlt.common.normalizers.typing import TNamingConventionReferenceArg
from dlt.common.typing import DictStrAny


@configspec
class NormalizersConfiguration(BaseConfiguration):
    # always in section
    __section__: ClassVar[str] = known_sections.SCHEMA

    naming: Optional[TNamingConventionReferenceArg] = None  # Union[str, NamingConvention]
    json_normalizer: Optional[DictStrAny] = None
    allow_identifier_change_on_table_with_data: Optional[bool] = None
