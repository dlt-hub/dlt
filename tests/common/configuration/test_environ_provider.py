import pytest
from typing import Any

from dlt.common.typing import TSecretValue
from dlt.common.configuration import (
    configspec,
    ConfigFieldMissingException,
    ConfigFileNotFoundException,
    resolve,
)
from dlt.common.configuration.specs import RuntimeConfiguration, BaseConfiguration
from dlt.common.configuration.providers import environ as environ_provider

from tests.utils import preserve_environ
from tests.common.configuration.utils import WrongConfiguration, SecretConfiguration, environment


@configspec
class SimpleRunConfiguration(RuntimeConfiguration):
    pipeline_name: str = "Some Name"
    test_bool: bool = False


@configspec
class SecretKubeConfiguration(BaseConfiguration):
    pipeline_name: str = "secret kube"
    secret_kube: TSecretValue = None


@configspec
class MockProdRunConfigurationVar(RuntimeConfiguration):
    pipeline_name: str = "comp"


def test_resolves_from_environ(environment: Any) -> None:
    environment["NONECONFIGVAR"] = "Some"

    C = WrongConfiguration()
    resolve._resolve_config_fields(
        C, explicit_values=None, explicit_sections=(), embedded_sections=(), accept_partial=False
    )
    assert not C.is_partial()

    assert C.NoneConfigVar == environment["NONECONFIGVAR"]


def test_resolves_from_environ_with_coercion(environment: Any) -> None:
    environment["RUNTIME__TEST_BOOL"] = "yes"

    C = SimpleRunConfiguration()
    resolve._resolve_config_fields(
        C, explicit_values=None, explicit_sections=(), embedded_sections=(), accept_partial=False
    )
    assert not C.is_partial()

    # value will be coerced to bool
    assert C.test_bool is True


def test_secret(environment: Any) -> None:
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(SecretConfiguration())
    environment["SECRET_VALUE"] = "1"
    C = resolve.resolve_configuration(SecretConfiguration())
    assert C.secret_value == "1"
    # mock the path to point to secret storage
    # from dlt.common.configuration import config_utils
    path = environ_provider.SECRET_STORAGE_PATH
    del environment["SECRET_VALUE"]
    try:
        # must read a secret file
        environ_provider.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = resolve.resolve_configuration(SecretConfiguration())
        assert C.secret_value == "BANANA"

        # set some weird path, no secret file at all
        del environment["SECRET_VALUE"]
        environ_provider.SECRET_STORAGE_PATH = "!C:\\PATH%s"
        with pytest.raises(ConfigFieldMissingException):
            resolve.resolve_configuration(SecretConfiguration())

        # set env which is a fallback for secret not as file
        environment["SECRET_VALUE"] = "1"
        C = resolve.resolve_configuration(SecretConfiguration())
        assert C.secret_value == "1"
    finally:
        environ_provider.SECRET_STORAGE_PATH = path


def test_secret_kube_fallback(environment: Any) -> None:
    path = environ_provider.SECRET_STORAGE_PATH
    try:
        environ_provider.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = resolve.resolve_configuration(SecretKubeConfiguration())
        # all unix editors will add x10 at the end of file, it will be preserved
        assert C.secret_kube == "kube\n"
        # we propagate secrets back to environ and strip the whitespace
        assert environment["SECRET_KUBE"] == "kube"
    finally:
        environ_provider.SECRET_STORAGE_PATH = path


def test_configuration_files(environment: Any) -> None:
    # overwrite config file paths
    environment["RUNTIME__CONFIG_FILES_STORAGE_PATH"] = "./tests/common/cases/schemas/ev1/"
    C = resolve.resolve_configuration(MockProdRunConfigurationVar())
    assert C.config_files_storage_path == environment["RUNTIME__CONFIG_FILES_STORAGE_PATH"]
    assert C.has_configuration_file("hasn't") is False
    assert C.has_configuration_file("event.schema.json") is True
    assert (
        C.get_configuration_file_path("event.schema.json")
        == "./tests/common/cases/schemas/ev1/event.schema.json"
    )
    with C.open_configuration_file("event.schema.json", "r") as f:
        f.read()
    with pytest.raises(ConfigFileNotFoundException):
        C.open_configuration_file("hasn't", "r")
