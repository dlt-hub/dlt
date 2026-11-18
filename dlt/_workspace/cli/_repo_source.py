"""Location and branch of the repositories that dlt installs from, remembered between installs."""

import argparse
import os
from typing import Optional, Tuple

from dlt.common.typing import TypedDict

from dlt._workspace.cli import echo as fmt


class TRepoSource(TypedDict, total=False):
    """Location and branch an install came from, holding only what differs from the defaults."""

    location: str
    branch: str


def add_repo_source_arguments(
    parser: argparse.ArgumentParser, repo_description: str, default_location: str
) -> None:
    """Adds `--location` and `--branch`, which default to the ones the last install used."""
    parser.add_argument(
        "--location",
        default=None,
        help=(
            f"Advanced. Git URL or local path of the {repo_description}. Defaults to the location"
            f" the last install used, else {default_location}. An empty value resets it."
        ),
    )
    parser.add_argument(
        "--branch",
        default=None,
        help=(
            "Advanced. Branch to fetch from. Defaults to the branch the last install from the"
            " same location used, else the default branch. An empty value resets it."
        ),
    )


def format_repo_source(location: str, branch: Optional[str]) -> str:
    """`location` and `branch` as messages show them."""
    if branch:
        return "%s (branch %s)" % (fmt.bold(location), fmt.bold(branch))
    return fmt.bold(location)


def normalize_repo_location(location: str) -> str:
    """Makes a local folder absolute, so it is found again from another working dir."""
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
    """The part of `location` and `branch` to record: what differs from the defaults, so an
    install from the defaults follows the defaults of later releases."""
    recorded: TRepoSource = {}
    if location != default_location:
        recorded["location"] = normalize_repo_location(location)
    if branch and branch != default_branch:
        recorded["branch"] = branch
    return recorded
