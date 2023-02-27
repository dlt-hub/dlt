import pytest

from dlt.common.destination.reference import DestinationReference, JobClientBase
from dlt.common.exceptions import InvalidDestinationReference, UnknownDestinationModule
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import InvalidDatasetName

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


def test_normalize_make_dataset_name() -> None:
    # use schema with default naming convention

    # schema name is not normalized, a proper schema name is assumed to be used
    assert JobClientBase.make_dataset_name(Schema("BANANA", normalize_name=True), "ban_ana_dataset", "default") == "ban_ana_dataset_banana"
    assert JobClientBase.make_dataset_name(Schema("default"), "ban_ana_dataset", "default") == "ban_ana_dataset"

    # also the dataset name is not normalized. it is verified if it is properly normalizes though
    with pytest.raises(InvalidDatasetName):
        JobClientBase.make_dataset_name(Schema("banana"), "BaNaNa", "default")

    # empty schemas are invalid
    with pytest.raises(ValueError):
        JobClientBase.make_dataset_name(Schema(None), "banana_dataset", None)
    with pytest.raises(ValueError):
        JobClientBase.make_dataset_name(Schema(""), "banana_dataset", "")

    # empty dataset names are invalid
    with pytest.raises(ValueError):
        JobClientBase.make_dataset_name(Schema("schema_ana"), "", "ban_schema")
    with pytest.raises(ValueError):
        JobClientBase.make_dataset_name(Schema("BAN_ANA", normalize_name=True), None, "BAN_ANA")


def test_normalize_make_dataset_name_none_default_schema() -> None:
    # if default schema is None, suffix is not added
    assert JobClientBase.make_dataset_name(Schema("default"), "ban_ana_dataset", None) == "ban_ana_dataset"
