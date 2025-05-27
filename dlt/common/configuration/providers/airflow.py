import io
import contextlib
from typing import Set

from .vault import VaultDocProvider


class AirflowSecretsTomlProvider(VaultDocProvider):
    def __init__(
        self,
        only_secrets: bool = False,
        only_toml_fragments: bool = False,
        list_secrets: bool = False,
    ) -> None:
        super().__init__(only_secrets, only_toml_fragments, list_secrets)

    @property
    def name(self) -> str:
        return "Airflow Secrets TOML Provider"

    def _look_vault(self, full_key: str, hint: type) -> str:
        """Get Airflow Variable with given `full_key`, return None if not found"""
        from airflow.models import Variable

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return Variable.get(full_key, default_var=None)  # type: ignore

    def _list_vault(self) -> Set[str]:
        """Lists all available variables in Airflow

        Returns:
            Set[str]: A set of available variable names in Airflow
        """
        from airflow.models import Variable
        from dlt.common import logger

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            # Use Airflow's session to query all variables
            session = Variable.get_session()
            available_keys = {var.key for var in session.query(Variable.key).all()}
            logger.info(f"Listed {len(available_keys)} variables from Airflow")
            return available_keys

    @property
    def supports_secrets(self) -> bool:
        return True
