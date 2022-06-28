from subprocess import PIPE, CalledProcessError
from typing import Any, Iterator

from dlt.common.runners.venv import Venv


def iter_stdout(venv: Venv, command: str, *script_args: Any) -> Iterator[str]:
    # start a process in virtual environment, assume that text comes from stdout
    with venv.start_command(command, *script_args, stdout=PIPE, stderr=PIPE, text=True) as process:
        exit_code: int = None

        # read all the lines until empty marker is returned
        for line in iter(process.stdout.readline, ''):
            yield line[:-1]

        # we fail iterator if exit code is not 0
        exit_code = process.wait()
        if exit_code != 0:
            raise CalledProcessError(exit_code, command)
