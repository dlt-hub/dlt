import pytest

from dlt.common.destination.reference import DestinationClientDwhConfiguration, Destination
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.exceptions import InvalidDestinationReference, UnknownDestinationModule
from dlt.common.schema import Schema

from tests.utils import ACTIVE_DESTINATIONS


def test_import_unknown_destination() -> None:
    # standard destination
    with pytest.raises(UnknownDestinationModule):
        Destination.from_reference("meltdb")
    # custom module
    with pytest.raises(UnknownDestinationModule):
        Destination.from_reference("melt.db")


def test_invalid_destination_reference() -> None:
    with pytest.raises(InvalidDestinationReference):
        Destination.from_reference("tests.load.cases.fake_destination.not_a_destination")


def test_import_all_destinations() -> None:
    # this must pass without the client dependencies being imported
    for dest_type in ACTIVE_DESTINATIONS:
        dest = Destination.from_reference(dest_type, None, dest_type + "_name", "production")
        assert dest.destination_type == dest_type
        assert dest.destination_name == dest_type + "_name"
        assert dest.config_params["environment"] == "production"
        assert dest.config_params["destination_name"] == dest_type + "_name"
        dest.spec()
        assert isinstance(dest.capabilities(), DestinationCapabilitiesContext)


def test_import_destination_config() -> None:
    duck_destination = "duckdb"

    # importing destination by type will work
    dest = Destination.from_reference(ref=duck_destination, environment="stage")
    assert dest.destination_type == duck_destination
    assert dest.config_params["environment"] == "stage"
    config = dest.configuration(dest.spec(dataset_name="dataset"))  # type: ignore
    assert config.destination_type == duck_destination
    assert config.destination_name == duck_destination
    assert config.environment == "stage"

    # importing destination by will work
    dest = Destination.from_reference(
        ref=None, destination_name=duck_destination, environment="production"
    )
    assert dest.destination_type == duck_destination
    assert dest.config_params["environment"] == "production"
    config = dest.configuration(dest.spec(dataset_name="dataset"))  # type: ignore
    assert config.destination_type == duck_destination
    assert config.destination_name == duck_destination
    assert config.environment == "production"

    # importing with different name will propagate name
    dest = Destination.from_reference(
        ref=duck_destination, destination_name="my_destination", environment="devel"
    )
    assert dest.destination_type == duck_destination
    assert dest.config_params["environment"] == "devel"
    config = dest.configuration(dest.spec(dataset_name="dataset"))  # type: ignore
    assert config.destination_type == duck_destination
    assert config.destination_name == "my_destination"
    assert config.environment == "devel"

    # incorrect name will fail with correct error
    with pytest.raises(UnknownDestinationModule):
        Destination.from_reference(ref=None, destination_name="balh")


def test_normalize_dataset_name() -> None:
    # with schema name appended

    assert (
        DestinationClientDwhConfiguration(
            dataset_name="ban_ana_dataset", default_schema_name="default"
        ).normalize_dataset_name(Schema("banana"))
        == "ban_ana_dataset_banana"
    )
    # without schema name appended
    assert (
        DestinationClientDwhConfiguration(
            dataset_name="ban_ana_dataset", default_schema_name="default"
        ).normalize_dataset_name(Schema("default"))
        == "ban_ana_dataset"
    )

    # dataset name will be normalized (now it is up to destination to normalize this)
    assert (
        DestinationClientDwhConfiguration(
            dataset_name="BaNaNa", default_schema_name="default"
        ).normalize_dataset_name(Schema("banana"))
        == "ba_na_na_banana"
    )

    # empty schemas are invalid
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration(
            dataset_name="banana_dataset", default_schema_name=None
        ).normalize_dataset_name(Schema(None))
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration(
            dataset_name="banana_dataset", default_schema_name=""
        ).normalize_dataset_name(Schema(""))

    # empty dataset name is valid!
    assert (
        DestinationClientDwhConfiguration(
            dataset_name="", default_schema_name="ban_schema"
        ).normalize_dataset_name(Schema("schema_ana"))
        == "_schema_ana"
    )
    # empty dataset name is valid!
    assert (
        DestinationClientDwhConfiguration(
            dataset_name="", default_schema_name="schema_ana"
        ).normalize_dataset_name(Schema("schema_ana"))
        == ""
    )
    # None dataset name is valid!
    assert (
        DestinationClientDwhConfiguration(
            dataset_name=None, default_schema_name="ban_schema"
        ).normalize_dataset_name(Schema("schema_ana"))
        == "_schema_ana"
    )
    # None dataset name is valid!
    assert (
        DestinationClientDwhConfiguration(
            dataset_name=None, default_schema_name="schema_ana"
        ).normalize_dataset_name(Schema("schema_ana"))
        is None
    )

    # now mock the schema name to make sure that it is normalized
    schema = Schema("barbapapa")
    schema._schema_name = "BarbaPapa"
    assert (
        DestinationClientDwhConfiguration(
            dataset_name="set", default_schema_name="default"
        ).normalize_dataset_name(schema)
        == "set_barba_papa"
    )


def test_normalize_dataset_name_none_default_schema() -> None:
    # if default schema is None, suffix is not added
    assert (
        DestinationClientDwhConfiguration(
            dataset_name="ban_ana_dataset", default_schema_name=None
        ).normalize_dataset_name(Schema("default"))
        == "ban_ana_dataset"
    )
