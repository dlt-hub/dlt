from subprocess import CalledProcessError
import tempfile
from typing import Any, Iterator, NamedTuple
import pytest
from dlt.common.exceptions import UnsupportedProcessStartMethodException

from dlt.common.runners import TRunMetrics, Venv
from dlt.common.runners.stdout import iter_std, iter_stdout, iter_stdout_with_result
from dlt.common.runners.synth_pickle import encode_obj, decode_obj, decode_last_obj

from dlt.common.utils import digest128b


class _TestPickler(NamedTuple):
    val_str: str
    val_int: int


# this is our unknown NamedTuple
# NOTE: do not remove commented out code
# class _TestPicklex(NamedTuple):
#     val_str: str
#     val_int: int


# class _TestClass:
#     def __init__(self, s1: _TestPicklex, s2: str) -> None:
#         self.s1 = s1
#         self.s2 = s2


class _TestClassUnkField:
    pass
    # def __init__(self, s1: _TestPicklex, s2: str) -> None:
    #     self.s1 = s1
    #     self.s2 = s2


def test_pickle_encoder() -> None:
    obj = [_TestPickler("A", 1), _TestPickler("B", 2), {"C": 3}]
    encoded = encode_obj(obj)
    assert decode_obj(encoded) == obj
    # break the encoding
    assert decode_obj(";" + encoded) is None
    assert decode_obj(digest128b(b"DONT")) is None
    assert decode_obj(digest128b(b"DONT") + "DONT") is None
    # encode unpicklable object
    with open("tests/common/scripts/counter.py", "r", encoding="utf-8") as s:
        assert encode_obj([s]) is None
        with pytest.raises(TypeError):
            assert encode_obj([s], ignore_pickle_errors=False) is None


def test_pickle_encoder_none() -> None:
    assert decode_obj(encode_obj(None)) is None


def test_synth_pickler_unknown_types() -> None:
    # synth unknown tuple
    obj = decode_obj(
        "ITcR+B7x+XYsddD8ws1cgASVRAAAAAAAAACMI3Rlc3RzLmNvbW1vbi5ydW5uZXJzLnRlc3Rfc3RkX3BpcGVzlIwMX1Rlc3RQaWNrbGV4lJOUjANYWVqUS3uGlIGULg=="
    )
    assert type(obj).__name__.endswith("_TestPicklex")
    # this is completely different type
    assert not isinstance(obj, tuple)

    # synth unknown class containing other unknown types
    obj = decode_obj(
        "G5nqyni0vOTdqmmd58izgASVcQAAAAAAAACMI3Rlc3RzLmNvbW1vbi5ydW5uZXJzLnRlc3Rfc3RkX3BpcGVzlIwKX1Rlc3RDbGFzc5STlCmBlH2UKIwCczGUaACMDF9UZXN0UGlja2xleJSTlIwBWZRLF4aUgZSMAnMylIwBVZSMA19zM5RLA3ViLg=="
    )
    assert type(obj).__name__.endswith("_TestClass")
    # tuple inside will be synthesized as well
    assert type(obj.s1).__name__.endswith("_TestPicklex")

    # known class containing unknown types
    obj = decode_obj(
        "9Ob27Bf1H05E48gxbOJZgASVcQAAAAAAAACMI3Rlc3RzLmNvbW1vbi5ydW5uZXJzLnRlc3Rfc3RkX3BpcGVzlIwSX1Rlc3RDbGFzc1Vua0ZpZWxklJOUKYGUfZQojAJzMZRoAIwMX1Rlc3RQaWNrbGV4lJOUjAFZlEsXhpSBlIwCczKUjAFVlHViLg=="
    )
    assert isinstance(obj, _TestClassUnkField)
    assert type(obj.s1).__name__.endswith("_TestPicklex")  # type: ignore[attr-defined]

    # commented out code that created encodings
    # print(encode_obj(_TestPicklex("XYZ", 123)))
    # obj = _TestClass(_TestPicklex("Y", 23), "U")
    # obj._s3 = 3
    # print(encode_obj(obj))
    # obj = _TestClassUnkField(_TestPicklex("Y", 23), "U")
    # print(encode_obj(obj))


def test_iter_stdout() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        expected = ["0", "1", "2", "3", "4", "exit"]
        for i, l in enumerate(iter_stdout(venv, "python", "tests/common/scripts/counter.py")):
            assert expected[i] == l
        lines = list(iter_stdout(venv, "python", "tests/common/scripts/empty.py"))
        assert lines == []
        with pytest.raises(CalledProcessError) as cpe:
            list(
                iter_stdout(venv, "python", "tests/common/scripts/no_stdout_no_stderr_with_fail.py")
            )
        # empty stdout
        assert cpe.value.output == ""
        assert cpe.value.stderr == ""
        # three lines with 1 MB size + newline
        for _i, l in enumerate(iter_stdout(venv, "python", "tests/common/scripts/long_lines.py")):
            assert len(l) == 1024 * 1024
        assert _i == 2


def test_iter_stdout_raises() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        expected = ["0", "1", "2"]
        with pytest.raises(CalledProcessError) as cpe:
            for i, line in enumerate(
                iter_stdout(venv, "python", "tests/common/scripts/raising_counter.py")
            ):
                assert expected[i] == line
        assert cpe.value.returncode == 1
        # the last output line is available
        assert cpe.value.output.strip() == "2"
        # the stderr is available
        assert 'raise Exception("end")' in cpe.value.stderr
        # we actually consumed part of the iterator up until "2"
        assert i == 2
        with pytest.raises(CalledProcessError) as cpe:
            list(iter_stdout(venv, "python", "tests/common/scripts/no_stdout_exception.py"))
        # empty stdout
        assert cpe.value.output == ""
        assert "no stdout" in cpe.value.stderr

        # three lines with 1 MB size + newline
        _i = -1
        with pytest.raises(CalledProcessError) as cpe:
            for _i, line in enumerate(
                iter_stdout(venv, "python", "tests/common/scripts/long_lines_fails.py")
            ):
                assert len(line) == 1024 * 1024
                assert line == "a" * 1024 * 1024
        # there were 3 lines
        assert _i == 2
        # stderr contains 3 lines
        _i = -1
        for _i, line in enumerate(cpe.value.stderr.splitlines()):
            assert len(line) == 1024 * 1024
            assert line == "b" * 1024 * 1024
        assert _i == 2


def test_std_iter() -> None:
    # even -> stdout, odd -> stderr
    expected = [(1, "0"), (2, "1"), (1, "2"), (2, "3")]
    with pytest.raises(CalledProcessError) as cpe:
        for i, line in enumerate(
            iter_std(
                Venv.restore_current(), "python", "-u", "tests/common/scripts/stderr_counter.py"
            )
        ):
            assert expected[i] == line
    assert cpe.value.returncode == 1


def test_stdout_encode_result() -> None:
    # use current venv to execute so we have dlt
    venv = Venv.restore_current()
    lines = list(iter_stdout(venv, "python", "tests/common/scripts/stdout_encode_result.py"))
    # last line contains results
    assert decode_obj(lines[-1]) == ("this is string", TRunMetrics(True, 300))

    # stderr will contain pickled exception somewhere
    with pytest.raises(CalledProcessError) as cpe:
        list(iter_stdout(venv, "python", "tests/common/scripts/stdout_encode_exception.py"))
    assert isinstance(decode_last_obj(cpe.value.stderr.split("\n")), Exception)

    # this script returns something that it cannot pickle
    lines = list(iter_stdout(venv, "python", "tests/common/scripts/stdout_encode_unpicklable.py"))
    assert decode_last_obj(lines) is None


def test_iter_stdout_with_result() -> None:
    venv = Venv.restore_current()
    i = iter_stdout_with_result(venv, "python", "tests/common/scripts/stdout_encode_result.py")
    assert iter_until_returns(i) == ("this is string", TRunMetrics(True, 300))
    i = iter_stdout_with_result(venv, "python", "tests/common/scripts/stdout_encode_unpicklable.py")
    assert iter_until_returns(i) is None
    # it just excepts without encoding exception
    with pytest.raises(CalledProcessError):
        i = iter_stdout_with_result(
            venv, "python", "tests/common/scripts/no_stdout_no_stderr_with_fail.py"
        )
        iter_until_returns(i)
    # this raises a decoded exception: UnsupportedProcessStartMethodException
    with pytest.raises(UnsupportedProcessStartMethodException):
        i = iter_stdout_with_result(
            venv, "python", "tests/common/scripts/stdout_encode_exception.py"
        )
        iter_until_returns(i)


def iter_until_returns(i: Iterator[Any]) -> Any:
    try:
        while True:
            next(i)
    except StopIteration as si:
        return si.value
