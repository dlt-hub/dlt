from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration


@configspec
class TransformationConfiguration(BaseConfiguration):
    """Configuration for a transformation"""

    buffer_max_items: Optional[int] = 5000
    """
    The chunk size to use for the transformation. If not provided, the chunk size
    will be inferred from the transformations.
    """
    always_materialize: Optional[bool] = False
    """
    If True, the transformation will always be materialized and not executed as a query
    """
