from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from dlt.common.warnings import DltDeprecationWarning

    DltDeprecationWarning(
        """Content from this module was moved to `dlt._dataset.relation`, which is internal. \
        If you need to import `ReadableDBAPIRelation`, import `dlt.Relation` instead.""",
        since="1.15.0",
        expected_due="2.0.0",
    )

from dlt._dataset.relation import Relation

ReadableDBAPIRelation = Relation
