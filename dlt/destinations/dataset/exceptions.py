from typing import TYPE_CHECKING
from dlt.common.warnings import DltDeprecationWarning

if not TYPE_CHECKING:
    from dlt.common.warnings import DltDeprecationWarning

    DltDeprecationWarning(
        """Content from this module was moved to `dlt._dataset.exceptions`, which is internal. \
        You can catch dlt exceptions using `dlt.common.exceptions.DltException.""",
        since="1.15.0",
        expected_due="2.0.0",
    )

from dlt._dataset.exceptions import (
    DatasetException,
    RelationHasQueryException,
    RelationUnknownColumnException,
)

ReadableRelationHasQueryException = RelationHasQueryException
ReadableRelationUnknownColumnException = RelationUnknownColumnException
