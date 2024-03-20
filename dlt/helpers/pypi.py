import importlib
import subprocess
import sys
from types import ModuleType

from dlt.cli import echo as fmt
from dlt.common.exceptions import MissingDependencyException


class Importer:
    @staticmethod
    def import_module(caller: str, package: str) -> ModuleType:
        if package not in sys.modules:
            if Importer.yes_please(package):
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            else:
                raise MissingDependencyException(caller, [package])

        return importlib.import_module(package)

    @staticmethod
    def yes_please(package: str) -> bool:
        return fmt.confirm(f"Required package {package} is missing, should we install it?")
