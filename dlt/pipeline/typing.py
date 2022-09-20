
from typing import Literal, Type, Any
from dataclasses import dataclass, fields as dtc_fields
from dlt.common import json

from dlt.common.typing import StrAny, TSecretValue

TLoaderType = Literal["bigquery", "redshift", "dummy"]
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
    PROJECT_ID: str = None
    DEFAULT_DATASET: str = None
    CLIENT_EMAIL: str = None
    PRIVATE_KEY: TSecretValue = None
    LOCATION: str = "US"
    CRED_TYPE: str = "service_account"
    TOKEN_URI: str = "https://oauth2.googleapis.com/token"
    HTTP_TIMEOUT: float = 15.0
    RETRY_DEADLINE: float = 600

    @property
    def default_dataset(self) -> str:
        return self.DEFAULT_DATASET

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        self.DEFAULT_DATASET = new_value

    @classmethod
    def from_services_dict(cls, services: StrAny, dataset_prefix: str, location: str = "US") -> "GCPPipelineCredentials":
        assert dataset_prefix is not None
        return cls("bigquery", services["project_id"], dataset_prefix, services["client_email"], services["private_key"], location or cls.LOCATION)

    @classmethod
    def from_services_file(cls, services_path: str, dataset_prefix: str, location: str = "US") -> "GCPPipelineCredentials":
        with open(services_path, "r", encoding="utf-8") as f:
            services = json.load(f)
        return GCPPipelineCredentials.from_services_dict(services, dataset_prefix, location)

    @classmethod
    def default_credentials(cls, dataset_prefix: str, project_id: str = None, location: str = None) -> "GCPPipelineCredentials":
        return cls("bigquery", project_id, dataset_prefix, None, None, location or cls.LOCATION)


@dataclass
class PostgresPipelineCredentials(PipelineCredentials):
    DBNAME: str = None
    DEFAULT_DATASET: str = None
    USER: str = None
    HOST: str = None
    PASSWORD: TSecretValue = None
    PORT: int = 5439
    CONNECT_TIMEOUT: int = 15

    @property
    def default_dataset(self) -> str:
        return self.DEFAULT_DATASET

    @default_dataset.setter
    def default_dataset(self, new_value: str) -> None:
        self.DEFAULT_DATASET = new_value


def credentials_from_dict(credentials: StrAny) -> PipelineCredentials:

    def ignore_unknown_props(typ_: Type[Any], props: StrAny) -> StrAny:
        fields = {f.name: f for f in dtc_fields(typ_)}
        return {k:v for k,v in props.items() if k in fields}

    client_type = credentials.get("CLIENT_TYPE")
    if client_type == "bigquery":
        return GCPPipelineCredentials(**ignore_unknown_props(GCPPipelineCredentials, credentials))
    elif client_type == "redshift":
        return PostgresPipelineCredentials(**ignore_unknown_props(PostgresPipelineCredentials, credentials))
    else:
        raise ValueError(f"CLIENT_TYPE: {client_type}")
