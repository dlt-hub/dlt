from typing import Any, Callable

import pytest

from dlt._workspace.workspace_definition import (
    create_built_in_workspace_definition,
    get_environment_definition,
    get_profile_definition,
)

from tests.workspace.utils import full_environment


@pytest.mark.parametrize(
    "lookup,name",
    [(get_profile_definition, "ci"), (get_environment_definition, "ci-write")],
    ids=["profile-is-environment", "environment-is-profile"],
)
def test_lookup_of_undeclared_name(lookup: Callable[..., Any], name: str) -> None:
    """A lookup finds declared names only: no profile by the name of its environment, no
    environment by the name of its default write profile."""
    doc = create_built_in_workspace_definition()
    doc["environments"]["ci"] = full_environment("ci")
    with pytest.raises(KeyError):
        lookup(doc, name)
