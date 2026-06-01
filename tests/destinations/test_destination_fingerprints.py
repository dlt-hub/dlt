from dlt.common.destination.client import DestinationClientConfiguration
from dlt.common.utils import digest128


class _PhysicalDestinationConfig(DestinationClientConfiguration):
    def __init__(self, physical_location: str) -> None:
        super().__init__()
        self._physical_location = physical_location

    def physical_location(self) -> str:
        return self._physical_location


def test_base_fingerprint_hashes_non_empty_physical_location() -> None:
    config = _PhysicalDestinationConfig("test-host:5432")

    assert config.fingerprint() == digest128("test-host:5432")


def test_base_fingerprint_returns_empty_string_without_physical_location() -> None:
    config = DestinationClientConfiguration()

    assert config.physical_location() == ""
    assert config.fingerprint() == ""
