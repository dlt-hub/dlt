import sys
import queue
from contextlib import contextmanager
from subprocess import PIPE, CalledProcessError
from threading import Thread
from typing import Any, Generator, Iterator, List, Tuple, Literal

from dlt.common.runners.venv import Venv
from dlt.common.runners.synth_pickle import decode_obj, decode_last_obj, encode_obj
from dlt.common.typing import AnyFun

# file number of stdout (1) and stderr (2)
OutputStdStreamNo = Literal[1, 2]


@contextmanager
def exec_to_stdout(f: AnyFun) -> Iterator[Any]:
    """Executes parameter-less function f and encodes the pickled return value to stdout. In case of exceptions, encodes the pickled exceptions to stderr"""
    rv: Any = None
    try:
        rv = f()
        yield rv
    except Exception as ex:
        print(encode_obj(ex), file=sys.stderr, flush=True)  # noqa
        raise
    finally:
        if rv is not None:
            print(encode_obj(rv), flush=True)  # noqa


def iter_std(
    venv: Venv, command: str, *script_args: Any
) -> Iterator[Tuple[OutputStdStreamNo, str]]:
    """Starts a process `command` with `script_args` in environment `venv` and returns iterator
    of (filno, line) tuples where `fileno` is 1 for stdout and 2 for stderr. `line` is
    a content of a line with stripped new line character.

    Use -u in scripts_args for unbuffered python execution
    """
    with venv.start_command(
        command,
        *script_args,
        stdout=PIPE,
        stderr=PIPE,
        bufsize=1,
        text=True,
        errors="backslashreplace",
    ) as process:
        exit_code: int = None
        q_: queue.Queue[Tuple[OutputStdStreamNo, str]] = queue.Queue()

        def _r_q(std_: OutputStdStreamNo) -> None:
            stream_ = process.stderr if std_ == 2 else process.stdout
            for line in iter(stream_.readline, ""):
                q_.put((std_, line.rstrip("\n")))
            # close queue
            q_.put(None)

        # read stderr with a thread, selectors do not work on windows
        t_out = Thread(target=_r_q, args=(1,), daemon=True)
        t_out.start()
        t_err = Thread(target=_r_q, args=(2,), daemon=True)
        t_err.start()
        while line := q_.get():
            yield line

        # get exit code
        exit_code = process.wait()
        # wait till stderr is received
        t_out.join()
        t_err.join()

        # we fail iterator if exit code is not 0
        if exit_code != 0:
            raise CalledProcessError(exit_code, command, output="", stderr="")


def iter_stdout(venv: Venv, command: str, *script_args: Any) -> Iterator[str]:
    # start a process in virtual environment, assume that text comes from stdout
    with venv.start_command(
        command,
        *script_args,
        stdout=PIPE,
        stderr=PIPE,
        bufsize=1,
        text=True,
        errors="backslashreplace",
    ) as process:
        exit_code: int = None
        line = ""
        stderr: List[str] = []

        def _r_stderr() -> None:
            nonlocal stderr
            for line in iter(process.stderr.readline, ""):
                stderr.append(line)

        # read stderr with a thread, selectors do not work on windows
        t = Thread(target=_r_stderr, daemon=True)
        t.start()

        # read stdout with
        for line in iter(process.stdout.readline, ""):
            yield line.rstrip("\n")

        # get exit code
        exit_code = process.wait()
        # wait till stderr is received
        t.join()

        # we fail iterator if exit code is not 0
        if exit_code != 0:
            raise CalledProcessError(exit_code, command, output=line, stderr="".join(stderr))


def iter_stdout_with_result(
    venv: Venv, command: str, *script_args: Any
) -> Generator[str, None, Any]:
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
            else:
                sys.stderr.write(cpe.stderr)
        # otherwise reraise cpe
        raise
