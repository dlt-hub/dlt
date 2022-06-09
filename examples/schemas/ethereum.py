from dlt.common.schema import Schema

from dlt.pipeline import Pipeline


def discover_schema() -> Schema:
    # all Ethereum compatible blockchains have the same schema so we just provide a nice yaml file
    return Pipeline.load_schema_from_file("examples/schemas/ethereum_schema.yml")
