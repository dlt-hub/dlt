import sys
from contextlib import contextmanager
from subprocess import PIPE, CalledProcessError
from typing import Any, Generator, Iterator

from dlt.common.runners.venv import Venv
from dlt.common.runners.synth_pickle import decode_obj, decode_last_obj, encode_obj
from dlt.common.typing import AnyFun


@contextmanager
def exec_to_stdout(f: AnyFun) -> Iterator[Any]:
    """Executes parameter-less function f and encodes the pickled return value to stdout. In case of exceptions, encodes the pickled exceptions to stderr"""
    rv: Any = None
    try:
        rv = f()
        yield rv
    except Exception as ex:
        print(encode_obj(ex), file=sys.stderr, flush=True)
        raise
    finally:
        if rv is not None:
            print(encode_obj(rv), flush=True)


def iter_stdout(venv: Venv, command: str, *script_args: Any) -> Iterator[str]:
    # start a process in virtual environment, assume that text comes from stdout
    with venv.start_command(command, *script_args, stdout=PIPE, stderr=PIPE, text=True) as process:
        exit_code: int = None

        # read all the lines until empty marker is returned
        line: str = ""
        for line in iter(process.stdout.readline, ''):
            yield line[:-1]

        # we fail iterator if exit code is not 0
        exit_code = process.wait()
        if exit_code != 0:
            raise CalledProcessError(exit_code, command, output=line, stderr=process.stderr.read())


def iter_stdout_with_result(venv: Venv, command: str, *script_args: Any) -> Generator[str, None, Any]:
    """Yields stdout lines coming from remote process and returns the last result decoded with decode_obj. In case of exit code != 0 if exception is decoded
       it will be raised, otherwise CalledProcessError is raised"""
    last_result: Any = None
    try:
        for line in iter_stdout(venv, command, *script_args):
            # attempt to decode line
            result = decode_obj(line, ignore_pickle_errors=True)
            # keep last decoded result
            if result is not None:
                last_result = result
            else:
                # yield other lines
                yield line
        return last_result
    except CalledProcessError as cpe:
        # try to find last object in stderr
        if cpe.stderr:
            # if exception was decoded from stderr
            exception = decode_last_obj(cpe.stderr.split("\n"), ignore_pickle_errors=False)
            if isinstance(exception, Exception):
                raise exception from cpe
        # otherwise reraise cpe
        raise
