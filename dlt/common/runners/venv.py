import os
import shutil
import venv
import types
import subprocess
from typing import Any, List, Type

from dlt.common.exceptions import CannotInstallDependency, VenvNotFound


class DLTEnvBuilder(venv.EnvBuilder):
    context: types.SimpleNamespace

    def __init__(self) -> None:
        super().__init__(with_pip=True, clear=True)

    def post_setup(self, context: types.SimpleNamespace) -> None:
        self.context = context


class Venv():
    def __init__(self, context: types.SimpleNamespace, current: bool = False) -> None:
        self.context = context
        self.current = current

    @classmethod
    def create(cls, venv_dir: str, dependencies: List[str] = None) -> "Venv":
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
        if not os.path.isdir(venv_dir):
            raise VenvNotFound(venv_dir)
        b = venv.EnvBuilder(clear=False, upgrade=False)
        c = b.ensure_directories(os.path.abspath(venv_dir))
        if not os.path.isfile(c.env_exe):
            raise VenvNotFound(c.env_exe)
        return cls(c, current)

    @classmethod
    def restore_current(cls) -> "Venv":
        try:
            venv = cls.restore(os.environ["VIRTUAL_ENV"], current=True)
        except KeyError:
            import sys
            bin_path, _ = os.path.split(sys.executable)
            context = types.SimpleNamespace(bin_path=bin_path, env_exe=sys.executable)
            venv = cls(context, current=True)
        return venv

    def __enter__(self) -> "Venv":
        if self.current:
            raise NotImplementedError("Context manager does not work with current venv")
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: types.TracebackType) -> None:
        if self.current:
            raise NotImplementedError("Context manager does not work with current venv")
        # delete venv
        if self.context.env_dir and os.path.isdir(self.context.env_dir):
            shutil.rmtree(self.context.env_dir)

    def start_command(self, entry_point: str, *script_args: Any, **popen_kwargs: Any) -> "subprocess.Popen[str]":
        command = os.path.join(self.context.bin_path, entry_point)
        cmd = [command, *script_args]
        return subprocess.Popen(cmd, **popen_kwargs)

    def run_command(self, entry_point: str, *script_args: Any) -> str:
        # runs one of installed entry points typically CLIS coming with packages and installed into PATH
        command = os.path.join(self.context.bin_path, entry_point)
        cmd = [command, *script_args]
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)

    def run_script(self, script_path: str, *script_args: Any) -> str:
        # os.environ is passed to executed process
        cmd = [self.context.env_exe, "-I", os.path.abspath(script_path), *script_args]
        try:
            return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        except subprocess.CalledProcessError as cpe:
            if cpe.returncode == 2:
                raise FileNotFoundError(script_path)
            else:
                raise

    def run_module(self, module: str, *module_args: Any) -> str:
        cmd = [self.context.env_exe, "-Im", module, *module_args]
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)

    def add_dependencies(self, dependencies: List[str] = None) -> None:
        Venv._install_deps(self.context, dependencies)

    @staticmethod
    def _install_deps(context: types.SimpleNamespace, dependencies: List[str]) -> None:
        for dep in dependencies:
            cmd = [context.env_exe, "-Im", "pip", "install", dep]
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                raise CannotInstallDependency(dep, context.env_exe, exc.output)
