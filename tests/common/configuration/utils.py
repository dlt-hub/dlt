import pytest
from os import environ
import datetime  # noqa: I251
from typing import (
    Any,
    ClassVar,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Dict,
    MutableMapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
)

from dlt.common import Decimal, pendulum
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import ConfigProvider, EnvironProvider
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.configuration.utils import get_resolved_traces
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.typing import TSecretValue, StrAny
from tests.utils import _inject_providers, _reset_providers, inject_providers


@configspec
class WrongConfiguration(BaseConfiguration):
    pipeline_name: str = "Some Name"
    NoneConfigVar: str = None
    log_color: bool = True


@configspec
class CoercionTestConfiguration(BaseConfiguration):
    pipeline_name: str = "Some Name"
    str_val: str = None
    int_val: int = None
    bool_val: bool = None
    list_val: list = None  # type: ignore
    dict_val: dict = None  # type: ignore
    bytes_val: bytes = None
    float_val: float = None
    tuple_val: Tuple[int, int, StrAny] = None
    any_val: Any = None
    none_val: str = None
    NESTED_VAL: Dict[str, Tuple[int, List[str], List[str]]] = None
    date_val: datetime.datetime = None
    dec_val: Decimal = None
    sequence_val: Sequence[str] = None
    gen_list_val: List[str] = None
    mapping_val: StrAny = None
    mutable_mapping_val: MutableMapping[str, str] = None


@configspec
class SecretConfiguration(BaseConfiguration):
    secret_value: TSecretValue = None


@configspec
class SecretCredentials(CredentialsConfiguration):
    secret_value: TSecretValue = None


@configspec
class WithCredentialsConfiguration(BaseConfiguration):
    credentials: SecretCredentials = None


@configspec
class SectionedConfiguration(BaseConfiguration):
    __section__: ClassVar[str] = "DLT_TEST"

    password: str = None


@configspec
class ConnectionStringCompatCredentials(ConnectionStringCredentials):
    database: str = None
    username: str = None


@pytest.fixture(scope="function")
def environment() -> Any:
    saved_environ = environ.copy()
    environ.clear()
    yield environ
    environ.clear()
    environ.update(saved_environ)


@pytest.fixture(autouse=True)
def reset_resolved_traces() -> None:
    get_resolved_traces().clear()


@pytest.fixture(scope="function")
def mock_provider() -> Iterator["MockProvider"]:
    mock_provider = MockProvider()
    # replace all providers with MockProvider that does not support secrets
    with inject_providers([mock_provider]):
        yield mock_provider


@pytest.fixture(scope="function")
def env_provider() -> Iterator[ConfigProvider]:
    env_provider = EnvironProvider()
    # inject only env provider
    with inject_providers([env_provider]):
        yield env_provider


@pytest.fixture
def toml_providers() -> Iterator[ConfigProvidersContainer]:
    yield from _reset_providers("./tests/common/cases/configuration/.dlt")


class MockProvider(ConfigProvider):
    def __init__(self) -> None:
        self.value: Any = None
        self.return_value_on: Tuple[str, ...] = ()
        self.reset_stats()

    def reset_stats(self) -> None:
        self.last_section: Tuple[str, ...] = None
        self.last_sections: List[Tuple[str, ...]] = []

    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        if pipeline_name:
            sections = (pipeline_name,) + sections
        self.last_section = sections
        self.last_sections.append(sections)
        if sections == self.return_value_on:
            rv = self.value
        else:
            rv = None
        return rv, "|".join(sections) + "-" + key

    @property
    def supports_secrets(self) -> bool:
        return False

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def name(self) -> str:
        return "Mock Provider"


class SecretMockProvider(MockProvider):
    @property
    def supports_secrets(self) -> bool:
        return True


COERCIONS = {
    "str_val": "test string",
    "int_val": 12345,
    "bool_val": True,
    "list_val": [1, "2", [3]],
    "dict_val": {"a": 1, "b": "2"},
    "bytes_val": b"Hello World!",
    "float_val": 1.18927,
    "tuple_val": (1, 2, {"1": "complicated dicts allowed in literal eval"}),
    "any_val": "function() {}",
    "none_val": "none",
    "NESTED_VAL": {"_": [1440, ["*"], []], "change-email": [560, ["*"], []]},
    "date_val": pendulum.now(),
    "dec_val": Decimal("22.38"),
    "sequence_val": ["A", "B", "KAPPA"],
    "gen_list_val": ["C", "Z", "N"],
    "mapping_val": {"FL": 1, "FR": {"1": 2}},
    "mutable_mapping_val": {"str": "str"},
}
