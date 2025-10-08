import os
from pathlib import Path

from dlt._workspace.providers import ProfileSecretsTomlProvider

TESTS_CASES_DIR = os.path.join("tests", "workspace", "cases", "provider")


def test_secrets_toml() -> None:
    provider = ProfileSecretsTomlProvider(os.path.join(TESTS_CASES_DIR, ".dlt"), "access")
    # first access profile, global comes second
    assert provider.locations == [
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/access.secrets.toml")),
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/secrets.toml")),
    ]
    # overrides secrets.toml with profile
    assert provider.get_value("api_key", str, None) == ("PASS", "api_key")
    # still has secrets.toml keys
    assert provider.get_value("log_level", str, None, "runtime") == ("WARNING", "runtime.log_level")

    # dev profile will load just secrets.toml
    provider = ProfileSecretsTomlProvider(os.path.join(TESTS_CASES_DIR, ".dlt"), "dev")
    assert provider.get_value("api_key", str, None) == ("X", "api_key")
