"""Workspace entities a job acts on, addressed relative to the workspace."""

from typing import Any, Dict, List, Mapping, Optional

from dlt._workspace.deployment.reflection import entity_properties
from dlt._workspace.deployment.typing import THubEntity, THubEntityType


def hub_entity(entity_type: THubEntityType, unique_id: str) -> THubEntity:
    """An entity in shorthand: `{type}/{unique id}`."""
    return {"type": entity_type, "id": f"{entity_type}/{unique_id}"}


def hub_objects(
    inputs: Optional[Dict[str, Any]],
    input_values: Mapping[str, Any],
    output: Optional[Dict[str, Any]],
    result: Any,
    source: str,
) -> List[THubEntity]:
    """Entities a run acted on, from entity-typed inputs and outputs; same-named outputs win."""
    by_name: Dict[str, THubEntity] = {}
    for name, entity_type in entity_properties(inputs, source).items():
        if value := input_values.get(name):
            by_name[name] = hub_entity(entity_type, str(value))
    if isinstance(result, Mapping):
        for name, entity_type in entity_properties(output, source).items():
            if value := result.get(name):
                by_name[name] = hub_entity(entity_type, str(value))
    # distinct entities, in declaration order
    objects: List[THubEntity] = []
    for entity in by_name.values():
        if entity not in objects:
            objects.append(entity)
    return objects
