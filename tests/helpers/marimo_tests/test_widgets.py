import marimo
import pytest

import dlt.helpers.marimo as marimo_helpers


@pytest.mark.parametrize("variable_name", [v for v in marimo_helpers.__all__ if v != "render"])
def test_public_names_are_marimo_app(variable_name: str) -> None:
    app = getattr(marimo_helpers, variable_name)
    assert isinstance(app, marimo.App)


def test_render_unknown_app_raises() -> None:
    unknown_app = marimo.App()
    with pytest.raises(ValueError, match="Unknown app"):
        marimo_helpers.render(unknown_app)


def test_render_non_app_raises() -> None:
    with pytest.raises(ValueError, match="app must be an instance of marimo.App"):
        marimo_helpers.render("not an app")  # type: ignore[arg-type]
