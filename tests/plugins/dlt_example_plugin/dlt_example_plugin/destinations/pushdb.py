import dlt
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems


@dlt.destination(batch_size=250, name="pushdb")
def push_destination(items: TDataItems, table: TTableSchema) -> None:
    pass
