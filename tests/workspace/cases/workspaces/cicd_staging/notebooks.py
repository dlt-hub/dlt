"""Notebooks of the workspace, deployed as interactive jobs."""

from dlt.hub.run import interactive


@interactive
def review_notebook() -> None:
    """Reviews the loaded data."""


@interactive
def migration_notebook() -> None:
    """A one-off migration, deployed to staging only."""
