from dlt.common.configuration import inject_section
from dlt.common.configuration.providers import ConfigProvider
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common.configuration.specs import config_providers_context
from tests.common.configuration.utils import MockProvider, environment  # noqa: F401


def test_extra_providers_ignore_injected_source_section(environment, monkeypatch) -> None:
    environment["PROVIDERS__ENABLE_AIRFLOW_SECRETS"] = "false"
    environment["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "true"
    environment["PROVIDERS__GOOGLE_SECRETS__ONLY_TOML_FRAGMENTS"] = "false"

    captured_settings = []

    def fake_google_provider(settings) -> ConfigProvider:
        captured_settings.append(dict(settings))
        return MockProvider()

    monkeypatch.setattr(config_providers_context, "_google_secrets_provider", fake_google_provider)

    with inject_section(
        ConfigSectionContext(sections=("sources", "sample_source")), merge_existing=False
    ):
        providers = config_providers_context._extra_providers()

    assert len(providers) == 1
    assert captured_settings == [
        {"only_secrets": True, "only_toml_fragments": False, "list_secrets": False}
    ]
