import pytest
from os import environ
import datetime  # noqa: I251
from typing import Any, Iterator, List, Optional, Tuple, Type, Dict, MutableMapping, Optional, Sequence

from dlt.common import Decimal, pendulum
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import ConfigProvider, EnvironProvider, ConfigTomlProvider, SecretsTomlProvider
from dlt.common.configuration.utils import get_resolved_traces
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.typing import TSecretValue, StrAny


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
    COMPLEX_VAL: Dict[str, Tuple[int, List[str], List[str]]] = None
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
    credentials: SecretCredentials


@configspec
class SectionedConfiguration(BaseConfiguration):
    __section__ = "DLT_TEST"

    password: str = None


@pytest.fixture(scope="function")
def environment() -> Any:
    saved_environ = environ.copy()
    environ.clear()
    yield environ
    environ.clear()
    environ.update(saved_environ)


@pytest.fixture(autouse=True)
def reset_resolved_traces() -> None:
    get_resolved_traces().clear()  # type: ignore


@pytest.fixture(scope="function")
def mock_provider() -> "MockProvider":
    container = Container()
    with container.injectable_context(ConfigProvidersContext()) as providers:
        # replace all providers with MockProvider that does not support secrets
        mock_provider = MockProvider()
        providers.providers = [mock_provider]
        yield mock_provider


@pytest.fixture
def toml_providers() -> Iterator[ConfigProvidersContext]:
    pipeline_root = "./tests/common/cases/configuration/.dlt"
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    ctx.add_provider(EnvironProvider())
    ctx.add_provider(SecretsTomlProvider(project_dir=pipeline_root))
    ctx.add_provider(ConfigTomlProvider(project_dir=pipeline_root))
    with Container().injectable_context(ctx):
        yield ctx


class MockProvider(ConfigProvider):

    def __init__(self) -> None:
        self.value: Any = None
        self.return_value_on: Tuple[str] = ()
        self.reset_stats()

    def reset_stats(self) -> None:
        self.last_section: Tuple[str] = None
        self.last_sections: List[Tuple[str]] = []

    def get_value(self, key: str, hint: Type[Any], pipeline_name: str, *sections: str) -> Tuple[Optional[Any], str]:
        if pipeline_name:
            sections = (pipeline_name, ) + sections
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
    'str_val': 'test string',
    'int_val': 12345,
    'bool_val': True,
    'list_val': [1, "2", [3]],
    'dict_val': {
        'a': 1,
        "b": "2"
    },
    'bytes_val': b'Hello World!',
    'float_val': 1.18927,
    "tuple_val": (1, 2, {"1": "complicated dicts allowed in literal eval"}),
    'any_val': "function() {}",
    'none_val': "none",
    'COMPLEX_VAL': {
        "_": [1440, ["*"], []],
        "change-email": [560, ["*"], []]
    },
    "date_val": pendulum.now(),
    "dec_val": Decimal("22.38"),
    "sequence_val": ["A", "B", "KAPPA"],
    "gen_list_val": ["C", "Z", "N"],
    "mapping_val": {"FL": 1, "FR": {"1": 2}},
    "mutable_mapping_val": {"str": "str"}
}