from dlt.common.normalizers.json import TNormalizedRowIterator
from dlt.common.schema import Schema
from dlt.common.normalizers.json.relational import normalize_data_item as relational_normalize, extend_schema
from dlt.common.typing import TDataItem


def normalize_data_item(schema: Schema, source_event: TDataItem, load_id: str, table_name: str) -> TNormalizedRowIterator:
    print(f"CUSTOM NORM: {schema.name} {table_name}")
    if schema.name == "event":
        # this emulates rasa parser on standard parser
        event = {"sender_id": source_event["sender_id"], "timestamp": source_event["timestamp"], "type": source_event["event"]}
        yield from relational_normalize(schema, event, load_id, table_name)
        # add table name which is "event" field in RASA OSS
        yield from relational_normalize(schema, source_event, load_id, table_name + "_" + source_event["event"])
    else:
        # will generate tables properly
        yield from relational_normalize(schema, source_event, load_id, table_name)
