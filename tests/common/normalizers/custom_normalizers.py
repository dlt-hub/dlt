from dlt.common.normalizers.json import TNormalizedRowIterator
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from dlt.common.typing import TDataItem


class NamingConvention(SnakeCaseNamingConvention):
    def normalize_identifier(self, identifier: str) -> str:
        if identifier.startswith("column_"):
            return identifier
        return "column_" + identifier.lower()


class ColumnNamingConvention(SnakeCaseNamingConvention):
    def normalize_identifier(self, identifier: str) -> str:
        if identifier.startswith("column_"):
            return identifier
        return "column_" + identifier.lower()


class DataItemNormalizer(RelationalNormalizer):
    def extend_schema(self) -> None:
        json_config = self.schema._normalizers_config["json"]["config"]
        d_h = self.schema._settings.setdefault("default_hints", {})
        d_h["not_null"] = json_config["not_null"]

    def normalize_data_item(
        self, source_event: TDataItem, load_id: str, table_name
    ) -> TNormalizedRowIterator:
        yield (table_name, None), source_event
