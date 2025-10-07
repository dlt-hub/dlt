import re

import dlt
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from dlt._workspace.mcp.tools import PipelineMCPTools

from tests.workspace.utils import pokemon_pipeline_context as pokemon_pipeline_context


def assert_valid_load_id(load_id: str) -> None:
    assert re.match(
        r"^\d+\.\d+$", load_id
    ), f"Invalid load format: {load_id}, expected timestamp like '1738348620.592284'"


def assert_valid_dlt_id(dlt_id: str) -> None:
    assert re.match(r"^[\w/+]{12,}$", dlt_id), f"Invalid dlt_id format: {dlt_id}"


def test_mcp_pipeline_tools(pokemon_pipeline_context: RunContextBase) -> None:
    pipeline = dlt.attach("rest_api_pokemon")
    pipeline_tools = PipelineMCPTools(pipeline)

    assert pipeline_tools.available_tables() == {
        "schemas": {"pokemon": ["pokemon", "berry", "location"]}
    }

    assert pipeline_tools.table_schema("pokemon") == {
        "columns": {
            "name": {
                "data_type": "text",
                "nullable": True,
                "normalized_name": '"name"',
                "arrow_data_type": "string",
            },
            "url": {
                "data_type": "text",
                "nullable": True,
                "normalized_name": '"url"',
                "arrow_data_type": "string",
            },
            "_dlt_load_id": {
                "data_type": "text",
                "nullable": False,
                "normalized_name": '"_dlt_load_id"',
                "arrow_data_type": "string",
            },
            "_dlt_id": {
                "data_type": "text",
                "nullable": False,
                "unique": True,
                "row_key": True,
                "normalized_name": '"_dlt_id"',
                "arrow_data_type": "string",
            },
        },
        "write_disposition": "append",
        "resource": "pokemon",
        "x-normalizer": {"seen-data": True},
        "sql_dialect": "duckdb",
        "normalized_name": '"pokemon"',
    }

    head = pipeline_tools.table_head("pokemon").splitlines()
    assert len(head) == 11
    assert head[0].split("|") == ['"name"', '"url"', '"_dlt_load_id"', '"_dlt_id"']

    # Check first row with regex patterns for dynamic values
    first_row = head[1].split("|")
    assert first_row[0] == '"bulbasaur"'
    assert first_row[1] == '"https://pokeapi.co/api/v2/pokemon/1/"'
    assert_valid_load_id(first_row[2].strip('"'))
    assert_valid_dlt_id(first_row[3].strip('"'))

    # Check last row with the same patterns
    last_row = head[10].split("|")
    assert last_row[0] == '"caterpie"'
    assert last_row[1] == '"https://pokeapi.co/api/v2/pokemon/10/"'
    assert_valid_load_id(last_row[2].strip('"'))
    assert_valid_dlt_id(last_row[3].strip('"'))

    output = pipeline_tools.query_sql("SELECT * FROM pokemon limit 5")
    expected_pattern = (
        r"Result with 5 row\(s\).*"  # Match header and everything after
        r'"bulbasaur"\|.*\n'  # Just check first pokemon name
        r'"ivysaur"\|.*\n'  # Basic structure check with pokemon names
        r'"venusaur"\|.*\n'
        r'"charmander"\|.*\n'
        r'"charmeleon"\|.*'
    )

    assert re.match(expected_pattern, output, re.DOTALL) is not None
