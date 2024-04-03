from dlt.common.normalizers.json import TNormalizedRowIterator
from dlt.common.normalizers.json.relational import (
    DataItemNormalizer as RelationalNormalizer,
)
from dlt.common.schema import Schema
from dlt.common.typing import TDataItem


class DataItemNormalizer(RelationalNormalizer):
    def normalize_data_item(
        self, source_event: TDataItem, load_id: str, table_name: str
    ) -> TNormalizedRowIterator:
        if self.schema.name == "event":
            # this emulates rasa parser on standard parser
            event = {
                "sender_id": source_event["sender_id"],
                "timestamp": source_event["timestamp"],
                "type": source_event["event"],
            }
            yield from super().normalize_data_item(event, load_id, table_name)
            # add table name which is "event" field in RASA OSS
            yield from super().normalize_data_item(
                source_event, load_id, table_name + "_" + source_event["event"]
            )
        else:
            # will generate tables properly
            yield from super().normalize_data_item(source_event, load_id, table_name)
