import json
import os
from typing import Any, Dict

import pytest

import dlt

HELPERS_CASES_PATH = os.path.join(os.path.dirname(__file__), "cases")


def load_json_case(name: str) -> Dict[str, Any]:
    with open(os.path.join(HELPERS_CASES_PATH, name + ".json"), "rb") as f:
        return json.load(f)


@pytest.fixture
def example_schema() -> dlt.Schema:
    """Schema shared by dbml, graphviz, and mermaid helper tests."""
    return dlt.Schema.from_dict(load_json_case("schemas/fruit_with_ref.schema"))
