import sys
import os
import shutil
import venv
import types
import subprocess
from typing import Any, List, Type

from dlt.common.exceptions import CannotInstallDependencies, VenvNotFound


class DLTEnvBuilder(venv.EnvBuilder):
    context: types.SimpleNamespace

    def __init__(self) -> None:
        super().__init__(with_pip=True, clear=True)

    def post_setup(self, context: types.SimpleNamespace) -> None:
        self.context = context


class Venv:
    """Creates and wraps the Python Virtual Environment to allow for code execution"""

    def __init__(self, context: types.SimpleNamespace, current: bool = False) -> None:
        """Please use `Venv.create`, `Venv.restore` or `Venv.restore_current` methods to create Venv instance"""
        self.context = context
        self.current = current

    @classmethod
    def create(cls, venv_dir: str, dependencies: List[str] = None) -> "Venv":
        """Creates a new Virtual Environment at the location specified in `venv_dir` and installs `dependencies` via pip. Deletes partially created environment on failure."""
        b = DLTEnvBuilder()
        try:
            b.create(os.path.abspath(venv_dir))
            if dependencies:
                Venv._install_deps(b.context, dependencies)
        except Exception:
            if os.path.isdir(venv_dir):
                shutil.rmtree(venv_dir)
            raise
        return cls(b.context)

    @classmethod
    def restore(cls, venv_dir: str, current: bool = False) -> "Venv":
        """Restores Virtual Environment at `venv_dir`"""
        if not os.path.isdir(venv_dir):
            raise VenvNotFound(venv_dir)
        b = venv.EnvBuilder(clear=False, upgrade=False)
        c = b.ensure_directories(os.path.abspath(venv_dir))
        if not os.path.isfile(c.env_exe):
            raise VenvNotFound(c.env_exe)
        return cls(c, current)

    @classmethod
    def restore_current(cls) -> "Venv":
        """Wraps the current Python environment."""
        try:
            venv = cls.restore(os.environ["VIRTUAL_ENV"], current=True)
        except KeyError:
            import sys

            # do not set bin path because it is not known
            context = types.SimpleNamespace(bin_path="", env_exe=sys.executable)
            venv = cls(context, current=True)
        return venv

    def __enter__(self) -> "Venv":
        if self.current:
            raise NotImplementedError("Context manager does not work with current venv")
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: types.TracebackType
    ) -> None:
        self.delete_environment()

    def delete_environment(self) -> None:
        """Deletes the Virtual Environment."""
        if self.current:
            raise NotImplementedError("Context manager does not work with current venv")
        # delete venv
        if self.context.env_dir and os.path.isdir(self.context.env_dir):
            shutil.rmtree(self.context.env_dir)

    def start_command(
        self, entry_point: str, *script_args: Any, **popen_kwargs: Any
    ) -> "subprocess.Popen[str]":
        command = os.path.join(self.context.bin_path, entry_point)
        cmd = [command, *script_args]
        return subprocess.Popen(cmd, **popen_kwargs)

    def run_command(self, entry_point: str, *script_args: Any) -> str:
        """Runs any `command` with specified `script_args`. Current `os.environ` and cwd is passed to executed process"""
        # runs one of installed entry points typically CLIs coming with packages and installed into PATH
        command = os.path.join(self.context.bin_path, entry_point)
        cmd = [command, *script_args]
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)

    def run_script(self, script_path: str, *script_args: Any) -> str:
        """Runs a python `script` source with specified `script_args`. Current `os.environ` and cwd is passed to executed process"""
        # os.environ is passed to executed process
        cmd = [self.context.env_exe, os.path.abspath(script_path), *script_args]
        try:
            return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        except subprocess.CalledProcessError as cpe:
            if cpe.returncode == 2:
                raise FileNotFoundError(script_path)
            else:
                raise

    def run_module(self, module: str, *module_args: Any) -> str:
        """Runs a python `module` with specified `module_args`. Current `os.environ` and cwd is passed to executed process"""
        cmd = [self.context.env_exe, "-m", module, *module_args]
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)

    def add_dependencies(self, dependencies: List[str] = None) -> None:
        Venv._install_deps(self.context, dependencies)

    @staticmethod
    def _install_deps(context: types.SimpleNamespace, dependencies: List[str]) -> None:
        cmd = [context.env_exe, "-Im", "pip", "install"]
        try:
            subprocess.check_output(cmd + dependencies, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as exc:
            raise CannotInstallDependencies(dependencies, context.env_exe, exc.output)

    @staticmethod
    def is_virtual_env() -> bool:
        """Checks if we are running in virtual environment"""
        return "VIRTUAL_ENV" in os.environ

    @staticmethod
    def is_venv_activated() -> bool:
        """Checks if virtual environment is activated in the shell"""
        return sys.prefix != sys.base_prefix
