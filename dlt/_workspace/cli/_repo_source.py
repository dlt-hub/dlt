"""Location and branch of the repositories that dlt installs from, recorded between commands."""

import argparse
import os
from typing import Optional, Tuple

from dlt.common.typing import TypedDict

from dlt._workspace.cli import echo as fmt


class TRepoSource(TypedDict, total=False):
    """Location and branch that a command got last. It holds only values that differ from the
    defaults."""

    location: str
    branch: str


def add_repo_source_arguments(
    parser: argparse.ArgumentParser, repo_description: str, default_location: str
) -> None:
    """Adds `--location` and `--branch`, which default to the ones given last to a command."""
    parser.add_argument(
        "--location",
        default=None,
        help=(
            f"Advanced. Git URL or local path of the {repo_description}. Defaults to the last"
            f" location given to a command, else {default_location}. An empty value resets it to"
            " the default."
        ),
    )
    parser.add_argument(
        "--branch",
        default=None,
        help=(
            "Advanced. Branch to fetch from. Defaults to the last branch given with the same"
            " location, else the default branch. An empty value resets it to the default."
        ),
    )


def format_repo_source(location: str, branch: Optional[str]) -> str:
    """Formats `location` and `branch` for a message."""
    if branch:
        return "%s (branch %s)" % (fmt.bold(location), fmt.bold(branch))
    return fmt.bold(location)


def normalize_repo_location(location: str) -> str:
    """Makes the path of a local folder absolute, so that it works from any working directory."""
    return os.path.abspath(location) if os.path.isdir(location) else location


def resolve_repo_source(
    location: Optional[str],
    branch: Optional[str],
    recorded: TRepoSource,
    default_location: str,
    default_branch: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """Returns the location and branch to fetch from.

    A given value wins, then the recorded one, then the default. An empty value resets to the
    default. The recorded branch applies only to the recorded location.
    """
    recorded_location = recorded.get("location", default_location)
    if location is None:
        location = recorded_location
    elif location == "":
        location = default_location
    else:
        location = normalize_repo_location(location)
    if branch is None:
        branch = (
            recorded.get("branch", default_branch)
            if location == recorded_location
            else default_branch
        )
    elif branch == "":
        branch = default_branch
    return location, branch


def get_recorded_repo_source(
    location: str,
    branch: Optional[str],
    default_location: str,
    default_branch: Optional[str] = None,
) -> TRepoSource:
    """Returns the values of `location` and `branch` that differ from the defaults. A default
    value is not recorded, so a later release can change the default."""
    recorded: TRepoSource = {}
    if location != default_location:
        recorded["location"] = normalize_repo_location(location)
    if branch and branch != default_branch:
        recorded["branch"] = branch
    return recorded
