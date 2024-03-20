import subprocess
import sys

from dlt.cli import echo as fmt


class Installer:
    @staticmethod
    def install(package: str):
        if package not in sys.modules:
            if Installer.yes_please(package):
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])

    @staticmethod
    def yes_please(package: str) -> bool:
        return fmt.confirm(f"Required package {package} is missing, should we install it?")
