
from dataclasses import dataclass
from typing import Literal
from dlt.common import json

from dlt.common.typing import StrAny
from dlt.common.configuration.utils import TSecretValue

TLoaderType = Literal["bigquery", "redshift"]
TPipelineStage = Literal["extract", "normalize", "load"]

# extractor generator yields functions that returns list of items of the type (table) when called
# this allows generator to implement retry logic
# TExtractorItem = Callable[[], Iterator[StrAny]]
# # extractor generator yields tuples: (type of the item (table name), function defined above)
# TExtractorItemWithTable = Tuple[str, TExtractorItem]
# TExtractorGenerator = Callable[[DictStrAny], Iterator[TExtractorItemWithTable]]


@dataclass
class PipelineCredentials:
    CLIENT_TYPE: TLoaderType

    @property
    def default_dataset(self) -> str:
        pass

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        pass

@dataclass
class GCPPipelineCredentials(PipelineCredentials):
    PROJECT_ID: str
    DEFAULT_DATASET: str
    CLIENT_EMAIL: str
    PRIVATE_KEY: TSecretValue = None
    HTTP_TIMEOUT: float = 15.0
    RETRY_DEADLINE: float = 600

    @property
    def default_dataset(self) -> str:
        return self.DEFAULT_DATASET

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        self.DEFAULT_DATASET = new_value

    @classmethod
    def from_services_dict(cls, services: StrAny, dataset_prefix: str) -> "GCPPipelineCredentials":
        assert dataset_prefix is not None

        return cls("bigquery", services["project_id"], dataset_prefix, services["client_email"], services["private_key"])

    @classmethod
    def from_services_file(cls, services_path: str, dataset_prefix: str) -> "GCPPipelineCredentials":
        with open(services_path, "r", encoding="utf-8") as f:
            services = json.load(f)
        return GCPPipelineCredentials.from_services_dict(services, dataset_prefix)

    @classmethod
    def default_credentials(cls, dataset_prefix: str) -> "GCPPipelineCredentials":
        return cls("bigquery", None, dataset_prefix, None)


@dataclass
class PostgresPipelineCredentials(PipelineCredentials):
    DBNAME: str
    DEFAULT_DATASET: str
    USER: str
    HOST: str
    PASSWORD: TSecretValue = None
    PORT: int = 5439
    CONNECT_TIMEOUT: int = 15

    @property
    def default_dataset(self) -> str:
        return self.DEFAULT_DATASET

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        self.DEFAULT_DATASET = new_value
