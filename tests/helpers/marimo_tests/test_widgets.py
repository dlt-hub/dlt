import marimo
import mowidgets
import pytest

import dlt.helpers.marimo as marimo_helpers


@pytest.mark.parametrize("variable_name", [v for v in marimo_helpers.__all__ if v != "render"])
def test_public_names_are_marimo_app(variable_name: str) -> None:
    app = getattr(marimo_helpers, variable_name)
    assert isinstance(app, marimo.App)
