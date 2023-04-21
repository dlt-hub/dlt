import tomlkit
from airflow.models import Variable

from .toml import BaseTomlProvider

AIRFLOW_SECRETS_TOML_VARIABLE_KEY = 'dlt_secrets_toml'


class AirflowSecretsTomlProvider(BaseTomlProvider):
    def __init__(
        self, variable_key: str = AIRFLOW_SECRETS_TOML_VARIABLE_KEY
    ) -> None:
        """Reads TOML configuration data from an Airflow variable specified
        by the `variable_key` and initializes a BaseTomlProvider with the
        parsed content.

        Args:
            variable_key (str, optional): The Airflow variable key to read secrets from.
                Defaults to AIRFLOW_SECRETS_TOML_VARIABLE_KEY.
        """
        toml_content = self._read_toml_from_airflow(variable_key)
        super().__init__(toml_content)

    def _read_toml_from_airflow(
        self, variable_key: str
    ) -> tomlkit.TOMLDocument:
        toml_string = Variable.get(variable_key)
        return tomlkit.parse(toml_string)

    @property
    def name(self) -> str:
        return 'Airflow Secrets TOML Provider'

    @property
    def supports_secrets(self) -> bool:
        return True
