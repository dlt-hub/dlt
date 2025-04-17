from typing import Dict, Any
from abc import abstractmethod, ABC

from dlt.common.typing import Self


class WithObjectStoreRsCredentials(ABC):
    @abstractmethod
    def to_object_store_rs_credentials(self) -> Dict[str, Any]:
        """Returns credentials dictionary for object_store Rust crate.

        Can be used for libraries that build on top of the object_store crate, such as `deltalake`.

        https://docs.rs/object_store/latest/object_store/
        """


class WithPyicebergConfig(ABC):
    @abstractmethod
    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        """Returns `pyiceberg` FileIO configuration dictionary.

        https://py.iceberg.apache.org/configuration/#fileio
        """

    @classmethod
    @abstractmethod
    def from_pyiceberg_fileio_config(cls, file_io: Dict[str, Any]) -> Self:
        """Returns credentials instance from FileIO. If `file_io` does
        not fit into given credential type, credentials remain unresolved.
        """
