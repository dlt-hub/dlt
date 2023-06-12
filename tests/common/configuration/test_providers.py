import pytest
from dlt.common.configuration.providers import MemoryTomlProvider

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


def test_toml_memory_provider() -> None:

    # test basic reading
    provider = MemoryTomlProvider("""
[section1.subsection]
key1 = "value1"

[section2.subsection]
key2 = "value2"
""")

    assert provider.get_value("key1", "", "section1", "subsection") ==  ("value1", "section1.subsection.key1")
    assert provider.get_value("key2", "", "section2", "subsection") ==  ("value2", "section2.subsection.key2")

    # test basic writing
    provider = MemoryTomlProvider("")
    assert provider.dumps() == ""

    provider.set_value("key1", "value1", "section1", "subsection")
    assert provider.dumps() == """[section1.subsection]
key1 = \"value1\"
"""

    provider.set_value("key1", "other_value", "section1", "subsection")
    assert provider.dumps() == """[section1.subsection]
key1 = \"other_value\"
"""
    provider.set_value("key1", "other_value", "section2", "subsection")
    assert provider.dumps() == """[section1.subsection]
key1 = \"other_value\"

[section2.subsection]
key1 = \"other_value\"
"""