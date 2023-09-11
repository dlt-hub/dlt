from typing import Sequence, Iterable
import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests

@dlt.resource(write_disposition="replace")
def dummy_data() -> Iterable[TDataItem]:
    """
    Returns a list of berries.
    Yields:
        dict: The berries data.
    """
    yield from [{
        "id": 1,
        "name": "one"
    }, {
        "id": 2,
        "name": "two"
    }]

@dlt.source
def source() -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [dummy_data]

pipeline = dlt.pipeline(
    pipeline_name="incremental", destination="duckdb", dataset_name="incremental_data"
)
load_info = pipeline.run(source())
print(load_info)