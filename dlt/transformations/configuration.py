from typing import Optional, ClassVar, Tuple

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.configuration.specs import known_sections


@configspec
class TransformConfiguration(BaseConfiguration):
    """Configuration for a transformation"""

    buffer_max_items: Optional[int] = 5000
    """
    The chunk size to use for the transformation. If not provided, the chunk size
    will be inferred from the transformations.
    """

    __known_sections__: ClassVar[Tuple[str, ...]] = (known_sections.EXTRACT,)
