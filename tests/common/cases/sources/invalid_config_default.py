import dlt
from typing import List


@dlt.resource(config_defaults={"sample_config": False})
def resource(
    sample_config: str = dlt.config.value,
) -> List[int]:
    return [1, 2, 3]
