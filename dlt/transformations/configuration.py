from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration


@configspec
class TransformConfiguration(BaseConfiguration):
    """Configuration for a transformation"""

    chunk_size: Optional[int] = None
    """
    The chunk size to use for the transformation. If not provided, the chunk size
    will be inferred from the transformations.
    """
