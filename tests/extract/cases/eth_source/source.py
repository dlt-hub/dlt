from typing import Any
import dlt


@dlt.source
def ethereum() -> Any:
    # this just tests if the schema "ethereum" was loaded
    return dlt.resource([1, 2, 3], name="data")
