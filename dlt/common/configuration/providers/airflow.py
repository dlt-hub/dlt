import tomlkit
from airflow.hooks.base import BaseHook

from .toml import BaseTomlProvider

AIRFLOW_SECRETS_TOML_CONNECTION_ID = 'dlt_secrets_toml'


class AirflowSecretsTomlProvider(BaseTomlProvider):
    def __init__(
        self, connection_id: str = AIRFLOW_SECRETS_TOML_CONNECTION_ID
    ) -> None:
        """Reads TOML configuration data from an Airflow connection
        specified by the `connection_id` and initializes a BaseTomlProvider
        with the parsed content.

        Args:
            connection_id (str, optional): The Airflow connection ID to read secrets from.
                Defaults to AIRFLOW_SECRETS_TOML_CONNECTION_ID.
        """
        toml_content = self._read_toml_from_airflow(connection_id)
        super().__init__(toml_content)

    def _read_toml_from_airflow(
        self, connection_id: str
    ) -> tomlkit.TOMLDocument:
        conn = BaseHook.get_connection(connection_id)
        return tomlkit.parse(conn.password)

    @property
    def name(self) -> str:
        return 'Airflow Secrets TOML Provider'

    @property
    def supports_secrets(self) -> bool:
        return True
