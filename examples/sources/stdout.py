from typing import Any, Iterator

from dlt.common import json
from dlt.common.runners.venv import Venv
from dlt.common.runners.stdout import iter_stdout
from dlt.common.typing import DictStrAny


def get_source(venv: Venv, command: str, *script_args: Any) -> Iterator[DictStrAny]:
    # use pipe iterator and mapping function to get dict iterator from pipe
    yield from map(lambda s: json.loads(s), iter_stdout(venv, command, *script_args))  # type: ignore
