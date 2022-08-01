from subprocess import CalledProcessError
import tempfile
import pytest

from dlt.common.runners.stdout import iter_stdout

from dlt.common.runners.venv import Venv


def test_iter_stdout() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        expected = ["0", "1", "2", "3", "4", "exit"]
        for i, l in enumerate(iter_stdout(venv, "python", "tests/common/scripts/counter.py")):
            assert expected[i] == l


def test_iter_stdout_raises() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        expected = ["0", "1", "2"]
        with pytest.raises(CalledProcessError) as cpe:
            for i, l in enumerate(iter_stdout(venv, "python", "tests/common/scripts/raising_counter.py")):
                assert expected[i] == l
        assert cpe.value.returncode == 1
        # we actually consumed part of the iterator up until "2"
        assert i == 2
