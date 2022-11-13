import pytest

from dlt.common.destination import DestinationReference
from dlt.common.exceptions import InvalidDestinationReference, UnknownDestinationModule

from tests.utils import ALL_DESTINATIONS


def test_import_unknown_destination() -> None:
    # standard destination
    with pytest.raises(UnknownDestinationModule):
        DestinationReference.from_name("meltdb")
    # custom module
    with pytest.raises(UnknownDestinationModule):
        DestinationReference.from_name("melt.db")


def test_invalid_destination_reference() -> None:
    with pytest.raises(InvalidDestinationReference):
        DestinationReference.from_name("tests.load.cases.fake_destination")


def test_import_all_destinations() -> None:
    # this must pass without the client dependencies being imported
    for module in ALL_DESTINATIONS:
        dest = DestinationReference.from_name(module)
        assert dest.__name__ == "dlt.destinations." + module
        dest.spec()
        dest.capabilities()
