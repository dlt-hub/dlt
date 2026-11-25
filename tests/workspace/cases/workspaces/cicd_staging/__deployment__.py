"""Production workspace: scheduled jobs and a notebook."""

from dlt.hub.run import job

from notebooks import review_notebook  # type: ignore[import-not-found]  # noqa: F401

__all__ = ["load_items", "transform_items", "review_notebook"]


@job(trigger="schedule:0 4 * * *")
def load_items() -> int:
    """Loads items into the warehouse."""
    return 1


@job(trigger="schedule:0 5 * * *")
def transform_items() -> int:
    """Transforms the loaded items."""
    return 2
