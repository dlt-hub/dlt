from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List

from dlt.common.schema import TTableSchema

IDENTIFIER_RE = re.compile(r"[^a-zA-Z0-9_]")


def normalize_identifier(value: str) -> str:
    normalized = IDENTIFIER_RE.sub("_", value).strip("_").lower()
    return normalized


@dataclass(frozen=True)
class TableContract:
    database_name: str
    schema: str
    table_name: str

    @property
    def qualified_target(self) -> str:
        return f"{self.database_name}.{self.schema}.{self.table_name}"

    @classmethod
    def from_table_schema(
        cls,
        table: TTableSchema,
        *,
        database_name: str,
        schema: str,
    ) -> "TableContract":
        table_name = normalize_identifier(table["name"])
        parent_name = table.get("parent")
        parent_table_name = normalize_identifier(parent_name) if parent_name else ""

        if parent_table_name:
            normalized_table_name = f"{parent_table_name}__{table_name}"
        else:
            normalized_table_name = table_name

        return cls(
            database_name=normalize_identifier(database_name),
            schema=normalize_identifier(schema),
            table_name=normalized_table_name,
        )

    @classmethod
    def declared_table_names(
        cls,
        *,
        database_name: str,
        schema: str,
        table_names: List[str],
    ) -> List[str]:
        return sorted(
            {
                cls.from_table_schema(
                    {"name": table_name},
                    database_name=database_name,
                    schema=schema,
                ).table_name
                for table_name in table_names
            }
        )
