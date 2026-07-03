from dlt.common.destination.client import DestinationClientConfiguration


class _PhysicalDestinationConfig(DestinationClientConfiguration):
    def __init__(self, physical_location: str) -> None:
        super().__init__()
        self._physical_location = physical_location

    def physical_location(self) -> str:
        return self._physical_location


def test_base_fingerprint_ignores_physical_location() -> None:
    config = _PhysicalDestinationConfig("test-host:5432")

    assert config.fingerprint() == ""


def test_base_fingerprint_returns_empty_string_without_physical_location() -> None:
    config = DestinationClientConfiguration()

    assert config.physical_location() == ""
    assert config.fingerprint() == ""
