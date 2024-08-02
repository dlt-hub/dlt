import io
import contextlib

from .vault import VaultDocProvider


class AirflowSecretsTomlProvider(VaultDocProvider):
    def __init__(self, only_secrets: bool = False, only_toml_fragments: bool = False) -> None:
        super().__init__(only_secrets, only_toml_fragments)

    @property
    def name(self) -> str:
        return "Airflow Secrets TOML Provider"

    def _look_vault(self, full_key: str, hint: type) -> str:
        """Get Airflow Variable with given `full_key`, return None if not found"""
        from airflow.models import Variable

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return Variable.get(full_key, default_var=None)  # type: ignore

    @property
    def supports_secrets(self) -> bool:
        return True
