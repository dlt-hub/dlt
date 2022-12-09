from typing import Any, Iterator

import dlt

from dlt.common import json
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.runners.venv import Venv
from dlt.common.runners.stdout import iter_stdout
from dlt.common.typing import DictStrAny


# create a standalone resource from a pipe to another process stdout. deselect the source by default as it will be used mostly as a data source to a transformer
@dlt.resource(selected=False, spec=BaseConfiguration)
def json_stdout(venv: Venv, command: str, *script_args: Any) -> Iterator[DictStrAny]:
    """Create a standalone resource from a pipe to another process stdout. Yields line by line. Parses lines as json."""
    # use pipe iterator and mapping function to get dict iterator from pipe
    yield from map(lambda s: json.loads(s), iter_stdout(venv, command, *script_args))  # type: ignore
