from typing import Dict, Any
from abc import abstractmethod, ABC


class WithPyicebergConfig(ABC):
    @abstractmethod
    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        """Returns `pyiceberg` FileIO configuration dictionary.

        https://py.iceberg.apache.org/configuration/#fileio
        """
        pass
