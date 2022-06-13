from dlt.common.schema import Schema

from dlt.pipeline import Pipeline


def discover_schema() -> Schema:
    # makes all 'timestamp' fields of type timestamps and filters out response selectors
    return Pipeline.load_schema_from_file("examples/schemas/rasa_schema.yml")
