from typing import Optional

import pytest
from packaging.specifiers import SpecifierSet

from dlt._workspace.cli.requirements import SourceRequirements


@pytest.mark.filterwarnings("error::DeprecationWarning")
@pytest.mark.parametrize(
    "constraint,prereleases,default_accepts_prerelease",
    [
        (">=1.31,<1.32", None, False),
        (">=1.31,<1.32", False, False),
        (">=1.31,<1.32", True, True),
        (">=1.31.1rc1,<1.32", None, True),
    ],
)
@pytest.mark.parametrize(
    "installed_version,compatible",
    [
        ("1.31.1rc2", True),
        ("1.31.1", True),
        ("1.30.1rc1", False),
        ("1.30.1", False),
        ("1.32.1rc1", False),
        ("1.32.1", False),
    ],
)
def test_installed_dlt_compatible_preserves_prerelease_policy(
    monkeypatch: pytest.MonkeyPatch,
    constraint: str,
    prereleases: Optional[bool],
    default_accepts_prerelease: bool,
    installed_version: str,
    compatible: bool,
) -> None:
    requirements = SourceRequirements([f"dlt{constraint}"])
    spec = SpecifierSet(constraint, prereleases=prereleases)
    requirements.dlt_requirement.specifier = spec
    original_policy = spec.prereleases
    assert spec.contains("1.31.1rc2") is default_accepts_prerelease
    assert not spec.contains("1.31.1rc2", prereleases=False)
    monkeypatch.setattr(requirements, "current_dlt_version", lambda: installed_version)

    for _ in range(2):
        assert requirements.is_installed_dlt_compatible() is compatible
        assert requirements.dlt_requirement.specifier is spec
        assert requirements.parsed_requirements[0].specifier is spec
        assert spec.contains("1.31.1rc2") is default_accepts_prerelease
        assert spec.prereleases is original_policy
        assert "1.31.1" in spec
