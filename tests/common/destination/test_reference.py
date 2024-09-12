from typing import Dict
import pytest

from dlt.common.destination.reference import DestinationClientDwhConfiguration, Destination
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import InvalidDestinationReference, UnknownDestinationModule
from dlt.common.schema import Schema
from dlt.common.typing import is_subclass
from dlt.common.normalizers.naming import sql_ci_v1, sql_cs_v1

from tests.common.configuration.utils import environment
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


def test_custom_destination_module() -> None:
    destination = Destination.from_reference(
        "tests.common.cases.destinations.null", destination_name="null-test"
    )
    assert destination.destination_name == "null-test"
    assert (
        destination.destination_type == "tests.common.cases.destinations.null.null"
    )  # a full type name


def test_arguments_propagated_to_config() -> None:
    dest = Destination.from_reference(
        "dlt.destinations.duckdb", create_indexes=None, unknown_param="A"
    )
    # None for create_indexes is not a default and it is passed on, unknown_param is removed because it is unknown
    assert dest.config_params == {"create_indexes": None}
    assert dest.caps_params == {}

    # test explicit config value being passed
    import dlt

    dest = Destination.from_reference(
        "dlt.destinations.duckdb", create_indexes=dlt.config.value, unknown_param="A"
    )
    assert dest.config_params == {"create_indexes": dlt.config.value}
    assert dest.caps_params == {}

    dest = Destination.from_reference(
        "dlt.destinations.weaviate", naming_convention="duck_case", create_indexes=True
    )
    # create indexes are not known
    assert dest.config_params == {}

    # create explicit caps
    dest = Destination.from_reference(
        "dlt.destinations.dummy",
        naming_convention="duck_case",
        recommended_file_size=4000000,
        loader_file_format="parquet",
    )
    from dlt.destinations.impl.dummy.configuration import DummyClientConfiguration

    assert dest.config_params == {"loader_file_format": "parquet"}
    # loader_file_format is a legacy param that is duplicated as preferred_loader_file_format
    assert dest.caps_params == {
        "naming_convention": "duck_case",
        "recommended_file_size": 4000000,
    }
    # instantiate configs
    caps = dest.capabilities()
    assert caps.naming_convention == "duck_case"
    assert caps.preferred_loader_file_format == "parquet"
    assert caps.recommended_file_size == 4000000
    init_config = DummyClientConfiguration()
    config = dest.configuration(init_config)
    assert config.loader_file_format == "parquet"  # type: ignore[attr-defined]


def test_factory_config_injection(environment: Dict[str, str]) -> None:
    environment["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"
    from dlt.destinations import dummy

    # caps will resolve from config without client
    assert dummy().capabilities().preferred_loader_file_format == "parquet"

    caps = dummy().client(Schema("client")).capabilities
    assert caps.preferred_loader_file_format == "parquet"

    environment.clear()
    caps = dummy().client(Schema("client")).capabilities
    assert caps.preferred_loader_file_format == "jsonl"

    environment["DESTINATION__DUMMY__LOADER_FILE_FORMAT"] = "parquet"
    environment["DESTINATION__DUMMY__FAIL_PROB"] = "0.435"

    # config will partially resolve without client
    config = dummy().configuration(None, accept_partial=True)
    assert config.fail_prob == 0.435
    assert config.loader_file_format == "parquet"

    dummy_ = dummy().client(Schema("client"))
    assert dummy_.capabilities.preferred_loader_file_format == "parquet"
    assert dummy_.config.fail_prob == 0.435

    # test named destination
    environment.clear()
    import os
    from dlt.destinations import filesystem
    from dlt.destinations.impl.filesystem.configuration import (
        FilesystemDestinationClientConfiguration,
    )

    filesystem_ = filesystem(destination_name="local")
    abs_path = os.path.abspath("_storage")
    environment["DESTINATION__LOCAL__BUCKET_URL"] = abs_path
    init_config = FilesystemDestinationClientConfiguration()._bind_dataset_name(dataset_name="test")
    configured_bucket_url = filesystem_.client(Schema("test"), init_config).config.bucket_url
    assert configured_bucket_url.endswith("_storage")


def test_import_module_by_path() -> None:
    # importing works directly from dlt destinations
    dest = Destination.from_reference("dlt.destinations.postgres")
    assert dest.destination_name == "postgres"
    assert dest.destination_type == "dlt.destinations.postgres"

    # try again directly with the output from the first dest
    dest2 = Destination.from_reference(dest.destination_type, destination_name="my_pg")
    assert dest2.destination_name == "my_pg"
    assert dest2.destination_type == "dlt.destinations.postgres"

    # try again with the path into the impl folder
    dest3 = Destination.from_reference(
        "dlt.destinations.impl.postgres.factory.postgres", destination_name="my_pg_2"
    )
    assert dest3.destination_name == "my_pg_2"
    assert dest3.destination_type == "dlt.destinations.postgres"


def test_import_all_destinations() -> None:
    # this must pass without the client dependencies being imported
    for dest_type in ACTIVE_DESTINATIONS:
        dest = Destination.from_reference(dest_type, None, dest_type + "_name", "production")
        assert dest.destination_type == "dlt.destinations." + dest_type
        assert dest.destination_name == dest_type + "_name"
        assert dest.config_params["environment"] == "production"
        assert dest.config_params["destination_name"] == dest_type + "_name"
        dest.spec()
        assert isinstance(dest.capabilities(), DestinationCapabilitiesContext)


def test_base_adjust_capabilities() -> None:
    # return without modifications
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps_props = dict(caps)
    adj_caps = Destination.adjust_capabilities(caps, None, None)
    assert caps is adj_caps
    assert dict(adj_caps) == caps_props

    # caps that support case sensitive idents may be put into case sensitive mode
    caps = DestinationCapabilitiesContext.generic_capabilities()
    assert caps.has_case_sensitive_identifiers is True
    assert caps.casefold_identifier is str
    # this one is already in case sensitive mode
    assert caps.generates_case_sensitive_identifiers() is True
    # applying cs naming has no effect
    caps = Destination.adjust_capabilities(caps, None, sql_cs_v1.NamingConvention())
    assert caps.generates_case_sensitive_identifiers() is True
    # same for ci naming, adjustment is only from case insensitive to sensitive
    caps = Destination.adjust_capabilities(caps, None, sql_ci_v1.NamingConvention())
    assert caps.generates_case_sensitive_identifiers() is True

    # switch to case sensitive if supported by changing case folding function
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.casefold_identifier = str.lower
    assert caps.generates_case_sensitive_identifiers() is False
    caps = Destination.adjust_capabilities(caps, None, sql_cs_v1.NamingConvention())
    assert caps.casefold_identifier is str
    assert caps.generates_case_sensitive_identifiers() is True
    # ci naming has no effect
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.casefold_identifier = str.upper
    caps = Destination.adjust_capabilities(caps, None, sql_ci_v1.NamingConvention())
    assert caps.casefold_identifier is str.upper
    assert caps.generates_case_sensitive_identifiers() is False

    # this one does not support case sensitive identifiers and is casefolding
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.has_case_sensitive_identifiers = False
    caps.casefold_identifier = str.lower
    assert caps.generates_case_sensitive_identifiers() is False
    caps = Destination.adjust_capabilities(caps, None, sql_cs_v1.NamingConvention())
    # no effect
    assert caps.casefold_identifier is str.lower
    assert caps.generates_case_sensitive_identifiers() is False


def test_instantiate_all_factories() -> None:
    from dlt import destinations

    impls = dir(destinations)
    for impl in impls:
        var_ = getattr(destinations, impl)
        if not is_subclass(var_, Destination):
            continue
        dest = var_()

        assert dest.destination_name
        assert dest.destination_type
        # custom destination is named after the callable
        if dest.destination_type != "dlt.destinations.destination":
            assert dest.destination_type.endswith(dest.destination_name)
        else:
            assert dest.destination_name == "dummy_custom_destination"
        assert dest.spec
        assert dest.spec()
        # partial configuration may always be created
        init_config = dest.spec.credentials_type()()
        init_config.__is_resolved__ = True
        assert dest.configuration(init_config, accept_partial=True)
        assert dest.capabilities()

        mod_dest = var_(
            destination_name="fake_name", environment="prod", naming_convention="duck_case"
        )
        assert (
            mod_dest.config_params.items()
            >= {"destination_name": "fake_name", "environment": "prod"}.items()
        )
        assert mod_dest.caps_params == {"naming_convention": "duck_case"}
        assert mod_dest.destination_name == "fake_name"
        caps = mod_dest.capabilities()
        assert caps.naming_convention == "duck_case"


def test_import_destination_config() -> None:
    # importing destination by type will work
    dest = Destination.from_reference(ref="dlt.destinations.duckdb", environment="stage")
    assert dest.destination_type == "dlt.destinations.duckdb"
    assert dest.config_params["environment"] == "stage"
    config = dest.configuration(dest.spec()._bind_dataset_name(dataset_name="dataset"))  # type: ignore
    assert config.destination_type == "duckdb"
    assert config.destination_name == "duckdb"
    assert config.environment == "stage"

    # importing destination by will work
    dest = Destination.from_reference(ref=None, destination_name="duckdb", environment="production")
    assert dest.destination_type == "dlt.destinations.duckdb"
    assert dest.config_params["environment"] == "production"
    config = dest.configuration(dest.spec()._bind_dataset_name(dataset_name="dataset"))  # type: ignore
    assert config.destination_type == "duckdb"
    assert config.destination_name == "duckdb"
    assert config.environment == "production"

    # importing with different name will propagate name
    dest = Destination.from_reference(
        ref="duckdb", destination_name="my_destination", environment="devel"
    )
    assert dest.destination_type == "dlt.destinations.duckdb"
    assert dest.destination_name == "my_destination"
    assert dest.config_params["environment"] == "devel"
    config = dest.configuration(dest.spec()._bind_dataset_name(dataset_name="dataset"))  # type: ignore
    assert config.destination_type == "duckdb"
    assert config.destination_name == "my_destination"
    assert config.environment == "devel"

    # incorrect name will fail with correct error
    with pytest.raises(UnknownDestinationModule):
        Destination.from_reference(ref=None, destination_name="balh")


def test_normalize_dataset_name() -> None:
    # with schema name appended

    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="ban_ana_dataset", default_schema_name="default")
        .normalize_dataset_name(Schema("banana"))
        == "ban_ana_dataset_banana"
    )
    # without schema name appended
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="ban_ana_dataset", default_schema_name="default")
        .normalize_dataset_name(Schema("default"))
        == "ban_ana_dataset"
    )

    # dataset name will be normalized (now it is up to destination to normalize this)
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="BaNaNa", default_schema_name="default")
        .normalize_dataset_name(Schema("banana"))
        == "ba_na_na_banana"
    )

    # empty schemas are invalid
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration()._bind_dataset_name(
            dataset_name="banana_dataset"
        ).normalize_dataset_name(Schema(None))
    with pytest.raises(ValueError):
        DestinationClientDwhConfiguration()._bind_dataset_name(
            dataset_name="banana_dataset", default_schema_name=""
        ).normalize_dataset_name(Schema(""))

    # empty dataset name is valid!
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="", default_schema_name="ban_schema")
        .normalize_dataset_name(Schema("schema_ana"))
        == "_schema_ana"
    )
    # empty dataset name is valid!
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="", default_schema_name="schema_ana")
        .normalize_dataset_name(Schema("schema_ana"))
        == ""
    )
    # None dataset name is valid!
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name=None, default_schema_name="ban_schema")
        .normalize_dataset_name(Schema("schema_ana"))
        == "_schema_ana"
    )
    # None dataset name is valid!
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name=None, default_schema_name="schema_ana")
        .normalize_dataset_name(Schema("schema_ana"))
        is None
    )

    # now mock the schema name to make sure that it is normalized
    schema = Schema("barbapapa")
    schema._schema_name = "BarbaPapa"
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="set", default_schema_name="default")
        .normalize_dataset_name(schema)
        == "set_barba_papa"
    )

    # test dataset_name_normalization false
    assert (
        DestinationClientDwhConfiguration(enable_dataset_name_normalization=False)
        ._bind_dataset_name(dataset_name="BarbaPapa__Ba", default_schema_name="default")
        .normalize_dataset_name(Schema("default"))
        == "BarbaPapa__Ba"
    )

    # test dataset_name_normalization default is true
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="BarbaPapa__Ba", default_schema_name="default")
        .normalize_dataset_name(Schema("default"))
        == "barba_papa_ba"
    )


def test_normalize_staging_dataset_name() -> None:
    # default normalized staging dataset
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="Dataset", default_schema_name="default")
        .normalize_staging_dataset_name(Schema("private"))
        == "dataset_private_staging"
    )
    # different layout
    assert (
        DestinationClientDwhConfiguration(staging_dataset_name_layout="%s__STAGING")
        ._bind_dataset_name(dataset_name="Dataset", default_schema_name="private")
        .normalize_staging_dataset_name(Schema("private"))
        == "dataset_staging"
    )
    # without placeholder
    assert (
        DestinationClientDwhConfiguration(staging_dataset_name_layout="static_staging")
        ._bind_dataset_name(dataset_name="Dataset", default_schema_name="default")
        .normalize_staging_dataset_name(Schema("private"))
        == "static_staging"
    )
    # empty dataset -> empty staging
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name=None, default_schema_name="private")
        .normalize_staging_dataset_name(Schema("private"))
        is None
    )
    assert (
        DestinationClientDwhConfiguration(staging_dataset_name_layout="static_staging")
        ._bind_dataset_name(dataset_name=None, default_schema_name="default")
        .normalize_staging_dataset_name(Schema("private"))
        == "static_staging"
    )

    # test dataset_name_normalization false
    assert (
        DestinationClientDwhConfiguration(
            enable_dataset_name_normalization=False, staging_dataset_name_layout="%s__Staging"
        )
        ._bind_dataset_name(dataset_name="BarbaPapa__Ba", default_schema_name="default")
        .normalize_staging_dataset_name(Schema("default"))
        == "BarbaPapa__Ba__Staging"
    )

    # test dataset_name_normalization default is true
    assert (
        DestinationClientDwhConfiguration(staging_dataset_name_layout="%s__Staging")
        ._bind_dataset_name(dataset_name="BarbaPapa__Ba", default_schema_name="default")
        .normalize_staging_dataset_name(Schema("default"))
        == "barba_papa_ba_staging"
    )


def test_normalize_dataset_name_none_default_schema() -> None:
    # if default schema is None, suffix is not added
    assert (
        DestinationClientDwhConfiguration()
        ._bind_dataset_name(dataset_name="ban_ana_dataset", default_schema_name=None)
        .normalize_dataset_name(Schema("default"))
        == "ban_ana_dataset"
    )
