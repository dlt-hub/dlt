import contextlib
import dataclasses
import io
from typing import ClassVar, List

from dlt.common.configuration.exceptions import DuplicateConfigProviderException
from dlt.common.configuration.providers import (
    ConfigProvider,
    ContextProvider,
)
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    BaseConfiguration,
    configspec,
    known_sections,
)


@configspec
class VaultProviderConfiguration(BaseConfiguration):
    only_secrets: bool = True
    "Only checks for secrets, ignoring all other field types"
    only_toml_fragments: bool = True
    "Only checks for toml fragments like sources or destinations"
    list_secrets: bool = False
    "Lists all secrets to eliminate non-existing keys upfront"


@configspec
class ConfigProvidersConfiguration(BaseConfiguration):
    enable_airflow_secrets: bool = True
    enable_google_secrets: bool = False

    airflow_secrets: VaultProviderConfiguration = VaultProviderConfiguration(
        only_secrets=False, only_toml_fragments=False, list_secrets=False
    )
    google_secrets: VaultProviderConfiguration = VaultProviderConfiguration(
        only_secrets=True, only_toml_fragments=True, list_secrets=False
    )  # None  # dataclasses.field(default_factory=lambda: dict(only_secrets=True, only_toml_fragments=True, list_secrets=False))
    # VaultProviderConfiguration(only_secrets=True, only_toml_fragments=True, list_secrets=False)
    # always look in providers
    __section__: ClassVar[str] = known_sections.PROVIDERS


class ConfigProvidersContainer:
    """Injectable list of providers used by the configuration `resolve` module"""

    providers: List[ConfigProvider] = None
    context_provider: ConfigProvider = None

    def __init__(self, initial_providers: List[ConfigProvider]) -> None:
        super().__init__()
        # add default providers
        self.providers = initial_providers
        # ContextProvider will provide contexts when embedded in configurations
        self.context_provider = ContextProvider()

    def add_extras(self) -> None:
        """Adds extra providers. Extra providers may use initial providers when setting up"""
        for provider in _extra_providers():
            self[provider.name] = provider

    def __getitem__(self, name: str) -> ConfigProvider:
        try:
            return next(p for p in self.providers if p.name == name)
        except StopIteration:
            raise KeyError(name)

    def __setitem__(self, name: str, provider: ConfigProvider) -> None:
        idx = next((i for i, p in enumerate(self.providers) if p.name == name), -1)
        if idx == -1:
            self.providers.append(provider)
        else:
            self.providers[idx] = provider

    def __contains__(self, name: object) -> bool:
        try:
            self.__getitem__(name)  # type: ignore
            return True
        except KeyError:
            return False

    def add_provider(self, provider: ConfigProvider) -> None:
        if provider.name in self:
            raise DuplicateConfigProviderException(provider.name)
        self.providers.append(provider)


def _extra_providers() -> List[ConfigProvider]:
    """Providers that require initial providers to be instantiated as the are enabled via config"""
    from dlt.common.configuration.resolve import resolve_configuration

    providers_config = resolve_configuration(ConfigProvidersConfiguration())
    extra_providers = []
    if providers_config.enable_airflow_secrets:
        extra_providers.extend(_airflow_providers(providers_config.airflow_secrets))
    if providers_config.enable_google_secrets:
        extra_providers.append(_google_secrets_provider(providers_config.google_secrets))
    return extra_providers


def _google_secrets_provider(settings: VaultProviderConfiguration) -> ConfigProvider:
    from dlt.common.configuration.resolve import resolve_configuration
    from dlt.common.configuration.providers.google_secrets import GoogleSecretsProvider

    c = resolve_configuration(
        GcpServiceAccountCredentials(), sections=(known_sections.PROVIDERS, "google_secrets")
    )
    return GoogleSecretsProvider(c, **dict(settings))


def _airflow_providers(settings: VaultProviderConfiguration) -> List[ConfigProvider]:
    """Returns a list of configuration providers for an Airflow environment.

    This function attempts to import Airflow to determine whether it
    is running in an Airflow environment. If Airflow is not installed,
    an empty list is returned. If Airflow is installed, the function
    returns a list containing the Airflow providers.

    Depending on how DAG is defined this function may be called outside of task and
    task context will not be available. Still we want the provider to function so
    we just test if Airflow can be imported.
    """

    providers: List[ConfigProvider] = []

    try:
        # hide stdio. airflow typically dumps tons of warnings and deprecations to stdout and stderr
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            # try to get dlt secrets variable. many broken Airflow installations break here. in that case do not create
            from airflow.models import Variable, TaskInstance  # noqa
            from dlt.common.configuration.providers.airflow import AirflowSecretsTomlProvider

            # probe if Airflow variable containing all secrets is present
            from dlt.common.configuration.providers.vault import SECRETS_TOML_KEY

            secrets_toml_var = Variable.get(SECRETS_TOML_KEY, default_var=None)

            # providers can be returned - mind that AirflowSecretsTomlProvider() requests the variable above immediately
            providers = [AirflowSecretsTomlProvider(**dict(settings))]

            # check if we are in task context and provide more info
            from airflow.operators.python import get_current_context  # noqa

            ti: TaskInstance = get_current_context()["ti"]  # type: ignore[assignment,unused-ignore]

        # log outside of stderr/out redirect
        if secrets_toml_var is None:
            message = (
                f"Airflow variable '{SECRETS_TOML_KEY}' was not found. "
                + "This Airflow variable is a recommended place to hold the content of"
                " secrets.toml."
                + "If you do not use Airflow variables to hold dlt configuration or use variables"
                " with other names you can ignore this warning."
            )
            ti.log.warning(message)

    except Exception:
        # do not probe variables when not in task context
        pass

    # airflow not detected
    return providers
