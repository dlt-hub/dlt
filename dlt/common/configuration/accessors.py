import abc
import contextlib
from typing import Any, ClassVar, Sequence, Type, TypeVar
from dlt.common import json
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import LookupTrace

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs import is_base_configuration_hint
from dlt.common.configuration.utils import deserialize_value, log_traces
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.schema.utils import coerce_value
from dlt.common.typing import AnyType, ConfigValue, TSecretValue

DLT_SECRETS_VALUE = "secrets.value"
DLT_CONFIG_VALUE = "config.value"
TConfigAny = TypeVar("TConfigAny", bound=Any)

class _Accessor(abc.ABC):

    def __getitem__(self, field: str) -> Any:
        value = self._get_value(field)
        if value is None:
            raise KeyError(field)
        if isinstance(value, str):
            return self._auto_cast(value)
        else:
            return value


    def get(self, field: str, expected_type: Type[TConfigAny] = None) -> TConfigAny:
        value: TConfigAny = self._get_value(field, expected_type)
        if value is None:
            return None
        # cast to required type
        if expected_type:
            return deserialize_value(field, value, expected_type)
        else:
            return value


    @property
    @abc.abstractmethod
    def config_providers(self) -> Sequence[ConfigProvider]:
        pass

    @property
    @abc.abstractmethod
    def default_type(self) -> AnyType:
        pass

    def _get_providers_from_context(self) -> Sequence[ConfigProvider]:
        return Container()[ConfigProvidersContext].providers

    def _auto_cast(self, value: str) -> Any:
        # try to cast to bool, int, float and complex (via JSON)
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        with contextlib.suppress(ValueError):
            return coerce_value("bigint", "text", value)
        with contextlib.suppress(ValueError):
            return coerce_value("double", "text", value)
        with contextlib.suppress(ValueError):
            c_v = json.loads(value)
            # only lists and dictionaries count
            if isinstance(c_v, (list, dict)):
                return c_v
        return value

    def _get_value(self, field: str, type_hint: Type[Any] = None) -> Any:
        # get default hint type, in case of dlt.secrets it it TSecretValue
        type_hint = type_hint or self.default_type
        # split field into namespaces and a key
        namespaces = field.split(".")
        key = namespaces.pop()
        value = None
        for provider in self.config_providers:
            value, _ = provider.get_value(key, type_hint, *namespaces)
            if value is not None:
                # log trace
                trace = LookupTrace(provider.name, namespaces, field, value)
                log_traces(None, key, type_hint, value, None, [trace])
                break
        return value


class _ConfigAccessor(_Accessor):
    """Provides direct access to configured values that are not secrets."""

    @property
    def config_providers(self) -> Sequence[ConfigProvider]:
        """Return a list of config providers, in lookup order"""
        return [p for p in self._get_providers_from_context()]

    @property
    def default_type(self) -> AnyType:
        return AnyType

    value: ClassVar[None] = ConfigValue
    "A placeholder that tells dlt to replace it with actual config value during the call to a source or resource decorated function."


class _SecretsAccessor(_Accessor):
    """Provides direct access to secrets."""

    @property
    def config_providers(self) -> Sequence[ConfigProvider]:
        """Return a list of config providers that can hold secrets, in lookup order"""
        return [p for p in self._get_providers_from_context() if p.supports_secrets]

    @property
    def default_type(self) -> AnyType:
        return TSecretValue

    value: ClassVar[None] = ConfigValue
    "A placeholder that tells dlt to replace it with actual secret during the call to a source or resource decorated function."


config = _ConfigAccessor()
"""Dictionary-like access to all config values to dlt"""

secrets = _SecretsAccessor()
"""Dictionary-like access to all secrets known known to dlt"""
