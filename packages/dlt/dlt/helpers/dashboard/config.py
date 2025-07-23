import dataclasses

from typing import List

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration


@configspec
class DashboardConfiguration(BaseConfiguration):
    table_list_fields: List[str] = dataclasses.field(
        default_factory=lambda: ["parent", "resource", "write_disposition", "description"]
    )
    """The fields to show in the table lists, name is always present and cannot be removed"""

    column_type_hints: List[str] = dataclasses.field(
        default_factory=lambda: [
            "data_type",
            "nullable",
            "precision",
            "scale",
            "timezone",
            "variant",
        ]
    )
    """Which column hints to show in the column list if type hints are enabled"""

    column_other_hints: List[str] = dataclasses.field(
        default_factory=lambda: ["primary_key", "merge_key", "unique"]
    )
    """Which column hints to show in the column list if other hints are enabled"""

    datetime_format: str = "YYYY-MM-DD HH:mm:ss Z"
    """The format of the datetime strings"""

    # this is needed for using this as a param in the cache
    def __hash__(self) -> int:
        return hash(
            ", ".join(self.table_list_fields)
            + ", ".join(self.column_type_hints)
            + ", ".join(self.column_other_hints)
            + self.datetime_format
        )
