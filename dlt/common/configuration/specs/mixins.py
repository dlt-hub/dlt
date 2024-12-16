from typing import Dict, Any
from abc import abstractmethod, ABC


class WithObjectStoreRsCredentials(ABC):
    @abstractmethod
    def to_object_store_rs_credentials(self) -> Dict[str, Any]:
        """Returns credentials dictionary for object_store Rust crate.

        Can be used for libraries that build on top of the object_store crate, such as `deltalake`.

        https://docs.rs/object_store/latest/object_store/
        """
        pass


class WithPyicebergConfig(ABC):
    @abstractmethod
    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        """Returns `pyiceberg` FileIO configuration dictionary.

        https://py.iceberg.apache.org/configuration/#fileio
        """
        pass
