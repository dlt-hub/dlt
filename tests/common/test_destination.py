import pytest

from dlt.common.destination.reference import DestinationClientDwhConfiguration, DestinationReference
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


def test_normalize_dataset_name() -> None:
    # with schema name appended

    assert DestinationClientDwhConfiguration(dataset_name="ban_ana_dataset", default_schema_name="default").normalize_dataset_name(Schema("banana")) == "ban_ana_dataset_banana"
    # without schema name appended
    assert DestinationClientDwhConfiguration(dataset_name="ban_ana_dataset", default_schema_name="default").normalize_dataset_name(Schema("default")) == "ban_ana_dataset"

    # also the dataset name is not normalized. it is verified if it is properly normalizes though
    with pytest.raises(InvalidDatasetName):
        DestinationClientDwhConfiguration(dataset_name="BaNaNa", default_schema_name="default").normalize_dataset_name(Schema("banana"))

    # empty schemas are invalid
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration(dataset_name="banana_dataset", default_schema_name=None).normalize_dataset_name(Schema(None))
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration(dataset_name="banana_dataset", default_schema_name="").normalize_dataset_name(Schema(""))

    # empty dataset names are invalid
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration(dataset_name="", default_schema_name="ban_schema").normalize_dataset_name(Schema("schema_ana"))
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration(dataset_name=None, default_schema_name="BAN_ANA").normalize_dataset_name(Schema("BAN_ANA"))

    # now mock the schema name to make sure that it is normalized
    schema = Schema("barbapapa")
    schema._schema_name = "BarbaPapa"
    assert DestinationClientDwhConfiguration(dataset_name="set", default_schema_name="default").normalize_dataset_name(schema) == "set_barba_papa"


def test_normalize_dataset_name_none_default_schema() -> None:
    # if default schema is None, suffix is not added
    assert DestinationClientDwhConfiguration(dataset_name="ban_ana_dataset", default_schema_name=None).normalize_dataset_name(Schema("default")) == "ban_ana_dataset"
