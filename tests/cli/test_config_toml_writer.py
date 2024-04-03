from typing import Optional, Final
import pytest
import tomlkit

from dlt.cli.config_toml_writer import write_value, WritableConfigValue, write_values

EXAMPLE_COMMENT = "# please set me up!"


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
    write_value(
        toml_table, "species", str, overwrite_existing=True, is_default_of_interest=True
    )
    assert toml_table["species"] == "species"

    # Test with is_default_of_interest=False and non-optional, non-final hint, and no default
    write_value(
        toml_table,
        "population",
        int,
        overwrite_existing=True,
        is_default_of_interest=False,
    )
    # non default get typed example value
    assert "population" in toml_table

    # Test with optional hint
    write_value(
        toml_table,
        "habitat",
        Optional[str],
        overwrite_existing=True,
        is_default_of_interest=False,
    )
    assert "habitat" not in toml_table

    # test with optional hint of interest
    write_value(
        toml_table,
        "habitat",
        Optional[str],
        overwrite_existing=True,
        is_default_of_interest=True,
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


def test_write_values(example_toml):
    values = [
        WritableConfigValue("species", str, "Homo sapiens", ("taxonomy", "genus")),
        WritableConfigValue(
            "species", str, "Mus musculus", ("taxonomy", "genus", "subgenus")
        ),
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
    assert (
        example_toml["taxonomy"]["genus"]["subgenus"]["species"]
        == "Canis lupus familiaris"
    )
    assert example_toml["genomic_info"]["genome_size"] == 2.8


def test_write_value_without_defaults(example_toml):
    toml_table = example_toml

    write_value(toml_table, "species", str, overwrite_existing=True)
    assert toml_table["species"] == "species"
    assert toml_table["species"].trivia.comment == EXAMPLE_COMMENT

    write_value(toml_table, "genome_size", float, overwrite_existing=True)
    assert toml_table["genome_size"] == 1.0
    assert toml_table["genome_size"].trivia.comment == EXAMPLE_COMMENT

    write_value(toml_table, "is_animal", bool, overwrite_existing=True)
    assert toml_table["is_animal"] is True

    write_value(toml_table, "chromosomes", list, overwrite_existing=True)
    assert toml_table["chromosomes"] == ["a", "b", "c"]

    write_value(toml_table, "genes", dict, overwrite_existing=True)
    assert toml_table["genes"] == {"key": "value"}
    assert toml_table["genes"].trivia.comment == EXAMPLE_COMMENT


def test_write_values_without_defaults(example_toml):
    values = [
        WritableConfigValue("species", str, None, ("taxonomy", "genus")),
        WritableConfigValue("genome_size", float, None, ("genomic_info",)),
        WritableConfigValue("is_animal", bool, None, ("animal_info",)),
        WritableConfigValue(
            "chromosomes", list, None, ("genomic_info", "chromosome_data")
        ),
        WritableConfigValue("genes", dict, None, ("genomic_info", "gene_data")),
    ]
    write_values(example_toml, values, overwrite_existing=True)

    assert example_toml["taxonomy"]["genus"]["species"] == "species"
    assert (
        example_toml["taxonomy"]["genus"]["species"].trivia.comment == EXAMPLE_COMMENT
    )

    assert example_toml["genomic_info"]["genome_size"] == 1.0
    assert example_toml["genomic_info"]["genome_size"].trivia.comment == EXAMPLE_COMMENT

    assert example_toml["animal_info"]["is_animal"] is True

    assert example_toml["genomic_info"]["chromosome_data"]["chromosomes"] == [
        "a",
        "b",
        "c",
    ]
    assert (
        example_toml["genomic_info"]["chromosome_data"]["chromosomes"].trivia.comment
        == EXAMPLE_COMMENT
    )

    assert example_toml["genomic_info"]["gene_data"]["genes"] == {"key": "value"}
    assert (
        example_toml["genomic_info"]["gene_data"]["genes"].trivia.comment
        == EXAMPLE_COMMENT
    )
