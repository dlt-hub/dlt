import datetime
from typing import ClassVar, List, Optional, Final
import pytest
import tomlkit

from dlt._workspace.cli.config_toml_writer import (
    write_value,
    WritableConfigValue,
    write_values,
    TYPE_EXAMPLES,
)
from dlt.common.configuration.specs import configspec
from dlt.common.destination.client import DEFAULT_FILE_LAYOUT

EXAMPLE_COMMENT = "# fill this in!"


@pytest.fixture
def example_toml():
    return tomlkit.parse("")


def test_write_value(example_toml):
    toml_table = example_toml

    write_value(
        toml_table,
        "species",
        str,
        overwrite_existing=True,
        default_value="Homo sapiens",
        is_default_of_interest=True,
    )
    assert toml_table["species"] == "Homo sapiens"

    write_value(
        toml_table,
        "species",
        str,
        overwrite_existing=False,
        default_value="Mus musculus",
        is_default_of_interest=True,
    )
    assert toml_table["species"] == "Homo sapiens"

    # Test with is_default_of_interest=True and non-optional, non-final hint
    write_value(toml_table, "species", str, overwrite_existing=True, is_default_of_interest=True)
    assert toml_table["species"] == "<configure me>"

    # Test with is_default_of_interest=False and non-optional, non-final hint, and no default
    write_value(
        toml_table, "population", int, overwrite_existing=True, is_default_of_interest=False
    )
    # non default get typed example value
    assert "population" in toml_table

    # Test with optional hint
    write_value(
        toml_table, "habitat", Optional[str], overwrite_existing=True, is_default_of_interest=False
    )
    assert "habitat" not in toml_table

    # test with optional hint of interest
    write_value(
        toml_table, "habitat", Optional[str], overwrite_existing=True, is_default_of_interest=True
    )
    assert "habitat" in toml_table

    # Test with final hint
    write_value(
        toml_table,
        "immutable_trait",
        Final[str],
        overwrite_existing=True,
        is_default_of_interest=False,
    )
    assert "immutable_trait" not in toml_table

    # Test with final hint of interest
    write_value(
        toml_table,
        "immutable_trait",
        Final[str],
        overwrite_existing=True,
        is_default_of_interest=True,
    )
    assert "immutable_trait" in toml_table

    # Test for timestamp type
    write_value(toml_table, "event_time", datetime.datetime, overwrite_existing=True)
    assert "event_time" in toml_table

    # Test for date type
    write_value(toml_table, "event_date", datetime.date, overwrite_existing=True)
    assert "event_date" in toml_table


def test_write_values(example_toml):
    values = [
        WritableConfigValue("species", str, "Homo sapiens", ("taxonomy", "genus")),
        WritableConfigValue("species", str, "Mus musculus", ("taxonomy", "genus", "subgenus")),
        WritableConfigValue("genome_size", float, 3.2, ("genomic_info",)),
    ]
    write_values(example_toml, values, overwrite_existing=True)

    assert example_toml["taxonomy"]["genus"]["species"] == "Homo sapiens"
    assert example_toml["taxonomy"]["genus"]["subgenus"]["species"] == "Mus musculus"
    assert example_toml["genomic_info"]["genome_size"] == 3.2

    new_values = [
        WritableConfigValue("species", str, "Canis lupus", ("taxonomy", "genus")),
        WritableConfigValue(
            "species", str, "Canis lupus familiaris", ("taxonomy", "genus", "subgenus")
        ),
        WritableConfigValue("genome_size", float, 2.8, ("genomic_info",)),
    ]
    write_values(example_toml, new_values, overwrite_existing=False)

    assert example_toml["taxonomy"]["genus"]["species"] == "Homo sapiens"
    assert example_toml["taxonomy"]["genus"]["subgenus"]["species"] == "Mus musculus"
    assert example_toml["genomic_info"]["genome_size"] == 3.2

    write_values(example_toml, new_values, overwrite_existing=True)

    assert example_toml["taxonomy"]["genus"]["species"] == "Canis lupus"
    assert example_toml["taxonomy"]["genus"]["subgenus"]["species"] == "Canis lupus familiaris"
    assert example_toml["genomic_info"]["genome_size"] == 2.8


def test_write_value_without_defaults(example_toml):
    toml_table = example_toml

    write_value(toml_table, "species", str, overwrite_existing=True)
    assert toml_table["species"] == "<configure me>"
    assert toml_table["species"].trivia.comment == EXAMPLE_COMMENT

    write_value(toml_table, "genome_size", float, overwrite_existing=True)
    assert toml_table["genome_size"] == 1.0
    assert toml_table["genome_size"].trivia.comment == EXAMPLE_COMMENT

    write_value(toml_table, "is_animal", bool, overwrite_existing=True)
    assert toml_table["is_animal"] is True

    # json types without defaults of interest will not be written (issue#2531)
    write_value(toml_table, "chromosomes", list, overwrite_existing=True)
    assert "chromosomes" not in toml_table

    write_value(toml_table, "genes", dict, overwrite_existing=True)
    assert "genes" not in toml_table

    write_value(toml_table, "event_time", datetime.datetime, overwrite_existing=True)
    assert toml_table["event_time"] == TYPE_EXAMPLES["timestamp"]

    # Test for date type
    write_value(toml_table, "event_date", datetime.date, overwrite_existing=True)
    assert toml_table["event_date"] == TYPE_EXAMPLES["date"]


def test_write_value_with_defaults(example_toml):
    toml_table = example_toml

    # json types with defaults do not get skipped
    write_value(toml_table, "chromosomes", list, default_value=["1", "2"], overwrite_existing=True)
    assert "chromosomes" not in toml_table

    # ... iff they are of interest
    write_value(
        toml_table,
        "chromosomes",
        list,
        default_value=["1", "2"],
        overwrite_existing=True,
        is_default_of_interest=True,
    )
    assert toml_table["chromosomes"] == ["1", "2"]

    write_value(
        toml_table,
        "genes",
        dict,
        default_value={"a": "genome"},
        overwrite_existing=True,
        is_default_of_interest=True,
    )
    assert toml_table["genes"] == {"a": "genome"}


def test_write_values_without_defaults(example_toml):
    values = [
        WritableConfigValue("species", str, None, ("taxonomy", "genus")),
        WritableConfigValue("genome_size", float, None, ("genomic_info",)),
        WritableConfigValue("is_animal", bool, None, ("animal_info",)),
        WritableConfigValue("chromosomes", list, None, ("genomic_info", "chromosome_data")),
        WritableConfigValue("genes", dict, None, ("genomic_info", "gene_data")),
    ]
    write_values(example_toml, values, overwrite_existing=True)

    assert example_toml["taxonomy"]["genus"]["species"] == "<configure me>"
    assert example_toml["taxonomy"]["genus"]["species"].trivia.comment == EXAMPLE_COMMENT

    assert example_toml["genomic_info"]["genome_size"] == 1.0
    assert example_toml["genomic_info"]["genome_size"].trivia.comment == EXAMPLE_COMMENT

    assert example_toml["animal_info"]["is_animal"] is True

    assert "chromosomes" not in example_toml["genomic_info"]["chromosome_data"]
    assert "genes" not in example_toml["genomic_info"]["gene_data"]


def test_write_spec_without_defaults(example_toml) -> None:
    from dlt.destinations.impl.snowflake.configuration import SnowflakeClientConfiguration
    from dlt.destinations.impl.filesystem.configuration import (
        FilesystemDestinationClientConfiguration,
    )

    write_value(
        example_toml, "snowflake", SnowflakeClientConfiguration, False, is_default_of_interest=True
    )
    # nothing of interest in "snowflake"
    # host, database, username are required and will be included
    # "password", "warehouse", "role" are explicitly of interest
    assert example_toml.as_string() == """[snowflake.credentials]
database = "<configure me>" # fill this in!
password = "<configure me>" # fill this in!
username = "<configure me>" # fill this in!
host = "<configure me>" # fill this in!
warehouse = "<configure me>" # fill this in!
role = "<configure me>" # fill this in!
"""
    example_toml = tomlkit.parse("")
    write_value(
        example_toml,
        "filesystem",
        FilesystemDestinationClientConfiguration,
        False,
        is_default_of_interest=True,
    )

    # bucket_url is mandatory, same for aws credentials
    assert example_toml.as_string() == """[filesystem]
bucket_url = "<configure me>" # fill this in!

[filesystem.credentials]
aws_access_key_id = "<configure me>" # fill this in!
aws_secret_access_key = "<configure me>" # fill this in!
"""

    @configspec
    class SnowflakeDatabaseConfiguration(SnowflakeClientConfiguration):
        database: str = "dlt_db"

        __config_gen_annotations__: ClassVar[List[str]] = ["database"]

    example_toml = tomlkit.parse("")
    write_value(
        example_toml,
        "snowflake",
        SnowflakeDatabaseConfiguration,
        False,
        is_default_of_interest=True,
    )

    # uses default value
    assert example_toml["snowflake"]["database"] == "dlt_db"

    # use initial values
    example_toml = tomlkit.parse("")
    write_value(
        example_toml,
        "filesystem",
        FilesystemDestinationClientConfiguration,
        False,
        is_default_of_interest=True,
        default_value={
            "bucket_url": "az://test-az-bucket",
            "layout": DEFAULT_FILE_LAYOUT,
            "credentials": {"region_name": "eu"},
        },
    )
    assert example_toml["filesystem"]["bucket_url"] == "az://test-az-bucket"
    # TODO: choose right credentials based on bucket_url
    assert example_toml["filesystem"]["credentials"]["aws_access_key_id"] == "<configure me>"
    # if initial value is different from the default then it is included
    assert example_toml["filesystem"]["credentials"]["region_name"] == "eu"
    # this is same as default so not included
    assert "layout" not in example_toml["filesystem"]

    example_toml = tomlkit.parse("")
    write_value(
        example_toml,
        "snowflake",
        SnowflakeDatabaseConfiguration,
        False,
        is_default_of_interest=True,
        default_value={"database": "dlt_db"},
    )

    # still here because marked specifically as of interest
    assert example_toml["snowflake"]["database"] == "dlt_db"
