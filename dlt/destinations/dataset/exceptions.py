from typing import TYPE_CHECKING

# NOTE this warning is only trigger when type-checking.
# Move it outside the type checking block when closer to deprecation date.
if not TYPE_CHECKING:
    from dlt.common.warnings import DltDeprecationWarning

    DltDeprecationWarning(
        """Content from this module was moved to `dlt._dataset.exceptions`, which is internal. \
        You can catch dlt exceptions using `dlt.common.exceptions.DltException.""",
        since="1.16.0",
        expected_due="2.0.0",
    )

from dlt.dataset.exceptions import (
    DatasetException,
    RelationHasQueryException,
    RelationUnknownColumnException,
)

ReadableRelationHasQueryException = RelationHasQueryException
ReadableRelationUnknownColumnException = RelationUnknownColumnException
