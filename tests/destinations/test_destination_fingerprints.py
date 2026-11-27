import pytest

from dlt.common.destination.client import DestinationClientConfiguration


class _PhysicalDestinationConfig(DestinationClientConfiguration):
    def __init__(self, data_location: str) -> None:
        super().__init__()
        self._data_location = data_location

    def data_location(self) -> str:
        return self._data_location


def test_base_fingerprint_ignores_data_location() -> None:
    config = _PhysicalDestinationConfig("test-host:5432")

    assert config.fingerprint() == ""


def test_base_destination_identifies_no_data_location() -> None:
    """A destination that names no place reports `None`. It never reports a blank location, because
    a blank location compares equal to the next blank one. The fingerprint stays optional."""
    config = DestinationClientConfiguration()

    assert config.data_location() is None
    assert config.fingerprint() == ""
