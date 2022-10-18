import pytest
from os import environ
from typing import Any, List, Optional, Tuple, Type
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersListContext

from dlt.common.typing import TSecretValue
from dlt.common.configuration import configspec
from dlt.common.configuration.providers import Provider
from dlt.common.configuration.specs import BaseConfiguration, CredentialsConfiguration, RunConfiguration


@configspec
class WrongConfiguration(RunConfiguration):
    pipeline_name: str = "Some Name"
    NoneConfigVar: str = None
    log_color: bool = True


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
class NamespacedConfiguration(BaseConfiguration):
    __namespace__ = "DLT_TEST"

    password: str = None


@pytest.fixture(scope="function")
def environment() -> Any:
    environ.clear()
    return environ


@pytest.fixture(scope="function")
def mock_provider() -> "MockProvider":
    container = Container()
    with container.injectable_context(ConfigProvidersListContext()) as providers:
        # replace all providers with MockProvider that does not support secrets
        mock_provider = MockProvider()
        providers.providers = [mock_provider]
        yield mock_provider


class MockProvider(Provider):

    def __init__(self) -> None:
        self.value: Any = None
        self.return_value_on: Tuple[str] = ()
        self.reset_stats()

    def reset_stats(self) -> None:
        self.last_namespace: Tuple[str] = None
        self.last_namespaces: List[Tuple[str]] = []

    def get_value(self, key: str, hint: Type[Any], *namespaces: str) -> Tuple[Optional[Any], str]:
        self.last_namespace = namespaces
        self.last_namespaces.append(namespaces)
        print("|".join(namespaces) + "-" + key)
        if namespaces == self.return_value_on:
            rv = self.value
        else:
            rv = None
        return rv, "|".join(namespaces) + "-" + key

    @property
    def supports_secrets(self) -> bool:
        return False

    @property
    def supports_namespaces(self) -> bool:
        return True

    @property
    def name(self) -> str:
        return "Mock Provider"


class SecretMockProvider(MockProvider):
    @property
    def supports_secrets(self) -> bool:
        return True
