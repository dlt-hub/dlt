import io
import contextlib
from typing import Optional, Set

from .vault import VaultDocProvider


def get_airflow_variable(key: str) -> Optional[str]:
    """Get Airflow Variable `key` or None if not found.

    Uses the Task SDK (`airflow.sdk.Variable`) on Airflow 3.x and falls back to
    `airflow.models.Variable` on Airflow 2.x. The `airflow.models` path still works on
    3.x but emits a DeprecationWarning on every call.
    """
    try:
        from airflow.sdk import Variable
    except ImportError:
        from airflow.models import Variable

        return Variable.get(key, default_var=None)  # type: ignore[no-any-return,unused-ignore]
    return Variable.get(key, default=None)  # type: ignore[no-any-return,unused-ignore]


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
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return get_airflow_variable(full_key)

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
