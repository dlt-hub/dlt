import pytest

from dlt.common.configuration.providers.environ import EnvironProvider


@pytest.mark.skip("Not implemented")
def test_providers_order() -> None:
    pass


@pytest.mark.skip("Not implemented")
def test_add_remove_providers() -> None:
    # TODO: we should be able to add and remove providers
    pass


@pytest.mark.skip("Not implemented")
def test_providers_autodetect_and_config() -> None:
    # TODO: toml based and remote vaults should be configured and/or autodetected
    pass


@pytest.mark.skip("Not implemented")
def test_providers_value_getter() -> None:
    # TODO: it should be possible to get a value from providers' chain via `config` and `secrets` objects via indexer (nested) or explicit key, *sections getter
    pass


def test_providers_repr() -> None:
    # NOTE we're not directly testing the base class `ConfigProvider`
    # though `EnvironProvider().__repr__()` is inherited from it
    sentinel = object()
    provider = EnvironProvider()

    repr_ = provider.__repr__()
    assert isinstance(repr_, str)
    assert "EnvironProvider(" in repr_

    # check that properties used by `__repr__` exist
    assert getattr(provider, "is_empty", sentinel) is not sentinel
    assert getattr(provider, "supports_secrets", sentinel) is not sentinel
    assert getattr(provider, "supports_sections", sentinel) is not sentinel
    assert getattr(provider, "is_writable", sentinel) is not sentinel
    assert getattr(provider, "locations", sentinel) is not sentinel
