import dlt

from dlt.common.storages.schema_storage import SchemaStorage

from docs.examples.sources.singer_tap import singer_raw_stream
from docs.examples.sources.jsonl import jsonl_file


# load hubspot schema stub - it converts all field names with `timestamp` into timestamp type
schema = SchemaStorage.load_schema_file("docs/examples/schemas/", "hubspot", ("yaml",))

p = dlt.pipeline(destination="postgres", dev_mode=True)
# now load a pipeline created from jsonl resource that feeds messages into singer tap transformer
pipe = jsonl_file("docs/examples/data/singer_taps/tap_hubspot.jsonl") | singer_raw_stream()
# provide hubspot schema
info = p.run(pipe, schema=schema, credentials="postgres://loader@localhost:5432/dlt_data")
print(info)
