import os
from pathlib import Path

from dlt._workspace.providers import ProfileSecretsTomlProvider
from dlt.common.configuration.providers import SECRETS_TOML

TESTS_CASES_DIR = os.path.join("tests", "workspace", "cases", "provider")


def test_secrets_toml() -> None:
    provider = ProfileSecretsTomlProvider(os.path.join(TESTS_CASES_DIR, ".dlt"), "access")
    # first access profile, global comes second
    assert provider.locations == [
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/access.secrets.toml")),
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/secrets.toml")),
    ]
    assert provider.present_locations == [
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/access.secrets.toml")),
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/secrets.toml")),
    ]
    # overrides secrets.toml with profile
    assert provider.get_value("api_key", str, None) == ("PASS", "api_key")
    # still has secrets.toml keys
    assert provider.get_value("log_level", str, None, "runtime") == ("WARNING", "runtime.log_level")
    # both files are merged into one provider so only the location tells them apart
    assert provider.get_value_location("api_key", None) == "access." + SECRETS_TOML
    assert provider.get_value_location("log_level", None, "runtime") == SECRETS_TOML

    # dev profile will load just secrets.toml
    provider = ProfileSecretsTomlProvider(os.path.join(TESTS_CASES_DIR, ".dlt"), "dev")
    assert provider.get_value("api_key", str, None) == ("X", "api_key")
    assert provider.get_value_location("api_key", None) == SECRETS_TOML


def test_secrets_not_present() -> None:
    provider = ProfileSecretsTomlProvider(os.path.join(TESTS_CASES_DIR, ".dlt"), "unknown")
    # first access profile, global comes second
    assert provider.locations == [
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/unknown.secrets.toml")),
        str(Path(TESTS_CASES_DIR).joinpath(".dlt/secrets.toml")),
    ]
    assert provider.present_locations == [str(Path(TESTS_CASES_DIR).joinpath(".dlt/secrets.toml"))]
