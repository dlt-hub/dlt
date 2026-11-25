from pathlib import Path
from typing import Optional, Tuple

import pytest

from dlt._workspace.cli._repo_source import (
    TRepoSource,
    get_recorded_repo_source,
    resolve_repo_source,
)

DEFAULT_LOCATION = "https://example.com/default.git"
DEFAULT_BRANCH = "main"
OTHER_LOCATION = "https://example.com/fork.git"


@pytest.mark.parametrize(
    "location,branch,recorded,expected",
    [
        (None, None, {}, (DEFAULT_LOCATION, DEFAULT_BRANCH)),
        (
            None,
            None,
            {"location": OTHER_LOCATION, "branch": "dev"},
            (OTHER_LOCATION, "dev"),
        ),
        (None, None, {"branch": "dev"}, (DEFAULT_LOCATION, "dev")),
        (OTHER_LOCATION, "feature", {"branch": "dev"}, (OTHER_LOCATION, "feature")),
        ("", "", {"location": OTHER_LOCATION, "branch": "dev"}, (DEFAULT_LOCATION, DEFAULT_BRANCH)),
        (
            "",
            None,
            {"location": OTHER_LOCATION, "branch": "dev"},
            (DEFAULT_LOCATION, DEFAULT_BRANCH),
        ),
        (None, "", {"location": OTHER_LOCATION, "branch": "dev"}, (OTHER_LOCATION, DEFAULT_BRANCH)),
        (OTHER_LOCATION, None, {"branch": "dev"}, (OTHER_LOCATION, DEFAULT_BRANCH)),
        (
            OTHER_LOCATION,
            None,
            {"location": OTHER_LOCATION, "branch": "dev"},
            (OTHER_LOCATION, "dev"),
        ),
    ],
    ids=[
        "nothing-recorded",
        "recorded",
        "recorded-branch-of-default-location",
        "explicit",
        "reset-both",
        "reset-location-drops-recorded-branch",
        "reset-branch",
        "other-location-drops-recorded-branch",
        "same-location-keeps-recorded-branch",
    ],
)
def test_resolve_repo_source(
    location: Optional[str],
    branch: Optional[str],
    recorded: TRepoSource,
    expected: Tuple[str, Optional[str]],
) -> None:
    assert (
        resolve_repo_source(location, branch, recorded, DEFAULT_LOCATION, DEFAULT_BRANCH)
        == expected
    )


def test_recorded_repo_source_keeps_what_differs_from_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert (
        get_recorded_repo_source(DEFAULT_LOCATION, DEFAULT_BRANCH, DEFAULT_LOCATION, DEFAULT_BRANCH)
        == {}
    )
    assert get_recorded_repo_source(DEFAULT_LOCATION, None, DEFAULT_LOCATION, DEFAULT_BRANCH) == {}
    assert get_recorded_repo_source(OTHER_LOCATION, "dev", DEFAULT_LOCATION, DEFAULT_BRANCH) == {
        "location": OTHER_LOCATION,
        "branch": "dev",
    }
    # a local folder is recorded absolute, so it is found from another working dir
    monkeypatch.chdir(tmp_path)
    (tmp_path / "harness").mkdir()
    assert get_recorded_repo_source("harness", None, DEFAULT_LOCATION) == {
        "location": str(tmp_path / "harness")
    }
