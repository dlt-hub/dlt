import sys
import os
from subprocess import CalledProcessError, PIPE
import tempfile
import pytest
import shutil

from dlt.common.exceptions import CannotInstallDependencies
from dlt.common.runners import Venv, VenvNotFound
from dlt.common.utils import custom_environ

from tests.utils import preserve_environ


def test_create_venv() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        assert venv.current is False
        assert os.path.isdir(venv.context.env_dir)
        assert os.path.isfile(venv.context.env_exe)
        # issue python command
        script = "print('success')"
        assert venv.run_command(venv.context.env_exe, "-c", script) == "success\n"
    # venv should be deleted
    assert not os.path.isdir(venv.context.env_dir)


def test_restore_venv() -> None:
    env_dir = tempfile.mkdtemp()
    with Venv.create(env_dir) as venv:
        restored_venv = Venv.restore(env_dir)
        assert restored_venv.current is False
        # compare contexts
        assert venv.context.env_dir == restored_venv.context.env_dir
        assert venv.context.env_exe == restored_venv.context.env_exe
        script = "print('success')"
        assert restored_venv.run_command(venv.context.env_exe, "-c", script) == "success\n"
    # restored env will fail - venv deleted
    with pytest.raises(FileNotFoundError):
        restored_venv.run_command(venv.context.env_exe, "-c", script)


def test_restore_wrong_root() -> None:
    # over existing dir
    env_dir = tempfile.mkdtemp()
    with pytest.raises(VenvNotFound) as v_exc:
        Venv.restore(env_dir)
    assert v_exc.value.interpreter.startswith(env_dir)
    # over non existing dir
    shutil.rmtree(env_dir)
    with pytest.raises(VenvNotFound) as v_exc:
        Venv.restore(env_dir)
    assert v_exc.value.interpreter == env_dir


def test_create_with_dependency() -> None:
    with Venv.create(tempfile.mkdtemp(), ["six"]) as venv:
        freeze = venv.run_command("pip", "freeze")
        assert "six" in freeze
        script = """
import six

print('success')
        """
        assert venv.run_command(venv.context.env_exe, "-c", script) == "success\n"


def test_create_with_wrong_dependency() -> None:
    with pytest.raises(CannotInstallDependencies) as cid:
        Venv.create(tempfile.mkdtemp(), ["six", "_six_"])
    assert cid.value.dependencies == ["six", "_six_"]


def test_add_dependency() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        freeze = venv.run_command("pip", "freeze")
        assert "six" not in freeze
        venv.add_dependencies(["six"])
        freeze = venv.run_command("pip", "freeze")
        assert "six" in freeze


def test_create_with_dependencies() -> None:
    with Venv.create(tempfile.mkdtemp(), ["six==1.16.0", "python-dateutil"]) as venv:
        script = """
import six
import dateutil

print('success')
        """
        assert venv.run_command(venv.context.env_exe, "-c", script) == "success\n"


def test_venv_working_dir() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        assert venv.run_script("tests/common/scripts/cwd.py").strip() == os.getcwd()
        script = """
import os

print(os.getcwd())
        """
        assert venv.run_command(venv.context.env_exe, "-c", script).strip() == os.getcwd()


def test_run_command_with_error() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        # non existing command
        with pytest.raises(FileNotFoundError):
            venv.run_command("_not_existing_command_")
        # command returns wrong status code
        with pytest.raises(CalledProcessError) as cpe:
            venv.run_command("pip", "wrong_param")
        assert cpe.value.returncode == 1
        script = """
raise Exception("always raises")
        """
        with pytest.raises(CalledProcessError) as cpe:
            venv.run_command("python", "-c", script)
        assert cpe.value.returncode == 1
        assert "always raises" in cpe.value.stdout


def test_run_module() -> None:
    with Venv.create(tempfile.mkdtemp(), ["six"]) as venv:
        freeze = venv.run_module("pip", "freeze", "--all")
        assert "six" in freeze
        assert "pip" in freeze

        # call non existing module
        with pytest.raises(CalledProcessError) as cpe:
            venv.run_module("blip")
        assert cpe.value.returncode == 1
        assert "blip" in cpe.value.stdout

        # call module with wrong params
        with pytest.raises(CalledProcessError) as cpe:
            venv.run_module("pip", "wrong_param")
        assert cpe.value.returncode == 1


def test_run_script() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        # relpath
        result = venv.run_script("tests/common/scripts/counter.py")
        lines = result.splitlines()
        assert lines[-1] == "exit"

        # abspath
        result = venv.run_script(os.path.abspath("tests/common/scripts/counter.py"))
        lines = result.splitlines()
        assert lines[-1] == "exit"

        # argv
        result = venv.run_script(os.path.abspath("tests/common/scripts/args.py"), "--with-arg")
        lines = result.splitlines()
        assert lines[0] == "2"
        assert "'--with-arg'" in lines[1]

        # custom environ
        with custom_environ({"_CUSTOM_ENV_VALUE": "uniq"}):
            result = venv.run_script("tests/common/scripts/environ.py")
            assert "_CUSTOM_ENV_VALUE=uniq" in result
        result = venv.run_script("tests/common/scripts/environ.py")
        assert "_CUSTOM_ENV_VALUE=uniq" not in result

        # non exiting script
        with pytest.raises(FileNotFoundError):
            venv.run_script(os.path.abspath("tests/common/scripts/_non_existing_.py"), "--with-arg")

        # raising script
        with pytest.raises(CalledProcessError) as cpe:
            venv.run_script("tests/common/scripts/raises.py")
        assert cpe.value.returncode == 1
        assert "always raises" in cpe.value.stdout

        # script with several long lines
        result = venv.run_script("tests/common/scripts/long_lines.py")
        lines = result.splitlines()
        # stdin and stdout are mangled but the number of character matches
        assert sum(len(line) for line in lines) == 6 * 1024 * 1024


def test_create_over_venv() -> None:
    # we always wipe out previous env
    env_dir = tempfile.mkdtemp()
    venv = Venv.create(env_dir, ["six"])
    freeze = venv.run_module("pip", "freeze", "--all")
    assert "six" in freeze

    # create over without dependency
    with Venv.create(env_dir) as venv:
        freeze = venv.run_module("pip", "freeze", "--all")
        assert "six" not in freeze


def test_current_venv() -> None:
    venv = Venv.restore_current()
    assert venv.current is True

    # use python to run module
    freeze = venv.run_module("pip", "freeze", "--all")
    # we are in current venv so dlt package is here
    assert "dlt" in freeze

    # use command
    with venv.start_command("pip", "freeze", "--all", stdout=PIPE, text=True) as process:
        output, _ = process.communicate()
        assert process.poll() == 0
        assert "pip" in output


def test_current_base_python() -> None:
    # remove VIRTUAL_ENV variable to fallback to currently executing python interpreter
    del os.environ["VIRTUAL_ENV"]
    venv = Venv.restore_current()
    assert venv.context.env_exe == sys.executable

    # use python to run module
    freeze = venv.run_module("pip", "freeze", "--all")
    # we are still in poetry virtual env but directly
    assert "dlt" in freeze

    # use command
    with venv.start_command("pip", "freeze", "--all", stdout=PIPE, text=True) as process:
        output, _ = process.communicate()
        assert process.poll() == 0
        assert "pip" in output


def test_start_command() -> None:
    with Venv.create(tempfile.mkdtemp()) as venv:
        with venv.start_command("pip", "freeze", "--all", stdout=PIPE, text=True) as process:
            output, _ = process.communicate()
            assert process.poll() == 0
            assert "pip" in output

        # custom environ
        with custom_environ({"_CUSTOM_ENV_VALUE": "uniq"}):
            with venv.start_command("python", "tests/common/scripts/environ.py", stdout=PIPE, text=True) as process:
                output, _ = process.communicate()
                assert process.poll() == 0
                assert "_CUSTOM_ENV_VALUE" in output

        # command not found
        with pytest.raises(FileNotFoundError):
            venv.start_command("blip", "freeze", "--all", stdout=PIPE, text=True)

        # command exit code
        with venv.start_command("pip", "wrong_command", stdout=PIPE, text=True) as process:
            assert process.wait() == 1
