"""Tests for the workspace entities a job result points at."""

from typing import Any, Dict

import pytest

from dlt._workspace.deployment.entity import hub_entity, hub_objects
from dlt._workspace.deployment.exceptions import InvalidJobSchema
from dlt._workspace.deployment.reflection import model_schema
from dlt._workspace.deployment.typing import THubEntityType


@pytest.mark.parametrize(
    "entity_type,unique_id,expected",
    [
        ("job-run", "9ac2", "job-run/9ac2"),
        ("job", "agents.job_inspector", "job/agents.job_inspector"),
        ("pipeline", "github_actions", "pipeline/github_actions"),
        ("dataset", "duckdb_prod/github_events", "dataset/duckdb_prod/github_events"),
        ("workspace", "github_actions", "workspace/github_actions"),
    ],
    ids=["job-run", "job", "pipeline", "dataset", "workspace"],
)
def test_entity_id_is_type_slash_unique_id(
    entity_type: THubEntityType, unique_id: str, expected: str
) -> None:
    assert hub_entity(entity_type, unique_id) == {"type": entity_type, "id": expected}


INPUTS: Dict[str, Any] = {
    "properties": {
        "investigated_run_id": {"type": "string", "entity_type": "job-run"},
        "depth": {"type": "integer"},
        "dataset": {"type": "string", "entity_type": "dataset"},
    }
}
OUTPUT: Dict[str, Any] = {
    "properties": {
        "investigated_run_id": {"type": "string", "entity_type": "job-run"},
        "produced": {"type": "string", "entity_type": "dataset"},
        "status": {"type": "string"},
    }
}


def test_objects_come_from_inputs_and_outputs_overwrite() -> None:
    """Inputs first, in declaration order; an output of the same name replaces the input's value."""
    objects = hub_objects(
        INPUTS,
        {"investigated_run_id": "9ac2", "depth": 3, "dataset": "duckdb_prod/events"},
        OUTPUT,
        {"investigated_run_id": "b7e1", "produced": "duckdb_prod/costs", "status": "succeeded"},
        "jobs.x.y",
    )
    assert objects == [
        {"type": "job-run", "id": "job-run/b7e1"},
        {"type": "dataset", "id": "dataset/duckdb_prod/events"},
        {"type": "dataset", "id": "dataset/duckdb_prod/costs"},
    ]


def test_objects_skip_what_nobody_supplied_and_collapse_duplicates() -> None:
    # an unset input is not an entity, and a payload that is not a mapping contributes nothing
    assert hub_objects(INPUTS, {"depth": 3}, OUTPUT, "done", "jobs.x.y") == []
    # the same entity named twice is one entity
    twice = {"properties": {"other": {"type": "string", "entity_type": "job-run"}}}
    objects = hub_objects(INPUTS, {"investigated_run_id": "9ac2"}, twice, {"other": "9ac2"}, "j")
    assert objects == [{"type": "job-run", "id": "job-run/9ac2"}]


def test_model_schema_moves_entity_type_into_a_comment() -> None:
    """Strict validators refuse dlt's keyword; `$comment` is the standard slot a model still reads."""
    schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "run_id": {"type": "string", "entity_type": "job-run", "$comment": "the failed run"},
            "runs": {"type": "array", "items": {"type": "string", "entity_type": "job-run"}},
            # a property that happens to be called entity_type is data, not the keyword
            "entity_type": {"type": "string"},
        },
    }
    converted = model_schema(schema)
    assert converted["properties"]["run_id"] == {
        "type": "string",
        "$comment": "the failed run; entity_type: job-run",
    }
    assert converted["properties"]["runs"]["items"] == {
        "type": "string",
        "$comment": "entity_type: job-run",
    }
    assert converted["properties"]["entity_type"] == {"type": "string"}
    # the manifest keeps the keyword, only the model's copy changes
    assert schema["properties"]["run_id"]["entity_type"] == "job-run"


def test_an_unknown_entity_type_is_refused() -> None:
    with pytest.raises(InvalidJobSchema, match="pipline"):
        hub_objects({"properties": {"x": {"entity_type": "pipline"}}}, {"x": "1"}, None, {}, "j")
