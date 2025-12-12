#!/usr/bin/env python
# ruff: noqa: T201
# flake8: noqa: T201
"""Check that hub extras version requirements match the dlt version.

When dlt is bumped from X.Y to X.Z, the hub extras (dlthub, dlt-runtime) should
also be bumped to require version 0.Z.

This ensures that dlt and its hub plugins are released together with matching versions.
"""

import sys
from importlib.metadata import distribution as pkg_distribution
from typing import List, NamedTuple

from packaging.requirements import Requirement
from packaging.version import Version

from dlt.common.runtime.run_context import ensure_plugin_version_match
from dlt.version import __version__ as dlt_version, DLT_PKG_NAME


class HubExtraInfo(NamedTuple):
    """Information about a hub extra dependency."""

    pkg_name: str
    requirement: Requirement


def get_hub_extras() -> List[HubExtraInfo]:
    """Get all requirements from the hub extra from dlt distribution metadata."""
    dist = pkg_distribution(DLT_PKG_NAME)
    if dist.requires is None:
        return []

    extras = []
    for req_str in dist.requires:
        req = Requirement(req_str)
        # Check if this requirement is for the "hub" extra by looking for
        # 'extra == "hub"' in the marker string. We can't use marker.evaluate()
        # because on Python 3.9, markers like 'extra == "hub" and python_version >= "3.10"'
        # would evaluate to False due to the python_version constraint.
        if req.marker:
            marker_str = str(req.marker)
            # Check for extra == "hub" or "hub" == extra patterns
            if 'extra == "hub"' in marker_str or '"hub" == extra' in marker_str:
                extras.append(HubExtraInfo(pkg_name=req.name, requirement=req))

    # make sure some extras were found
    assert len(extras) >= 2, f"Expected at least 2 hub extras, found {len(extras)}"

    return extras


def dlt_version_to_plugin_version(dlt_ver: str) -> str:
    """Convert dlt version to expected plugin version.

    dlt 1.20.0 -> plugin 0.20.0
    dlt 1.19.0a5 -> plugin 0.19.0a5

    The major version is set to 0, and minor.patch are kept from dlt.
    """
    v = Version(dlt_ver)
    # Build version string with major=0
    parts = [f"0.{v.minor}.{v.micro}"]
    if v.pre:
        # Pre-release like a5, b1, rc2
        parts.append(f"{v.pre[0]}{v.pre[1]}")
    if v.dev is not None:
        parts.append(f".dev{v.dev}")
    return "".join(parts)


def check_hub_extras() -> int:
    """Check that all hub extras requirements allow the current dlt version.

    Returns:
        0 if all checks pass, 1 if any fail
    """
    plugin_version = dlt_version_to_plugin_version(dlt_version)

    print(f"dlt version: {dlt_version}")
    print(f"Expected plugin version: {plugin_version}")
    print()

    hub_extras = get_hub_extras()
    if not hub_extras:
        print("No hub extras found in dlt distribution metadata")
        return 0

    errors = []
    for extra in hub_extras:
        print(f"Checking {extra.pkg_name}: {extra.requirement.specifier}")

        try:
            # Use ensure_plugin_version_match to validate
            # This will raise MissingDependencyException if version doesn't match
            ensure_plugin_version_match(
                pkg_name=extra.pkg_name,
                dlt_version=dlt_version,
                plugin_version=plugin_version,
                plugin_module_name=extra.pkg_name,
                dlt_extra="hub",
                dlt_version_specifier=extra.requirement.specifier,
            )
            print(f"  ✓ {plugin_version} satisfies {extra.requirement.specifier}")
        except Exception as e:
            print(f"  ✗ {plugin_version} does NOT satisfy {extra.requirement.specifier}")
            errors.append(f"{extra.pkg_name}: {e}")

    print()
    if errors:
        print("FAILED: Hub extras version requirements do not match dlt version!")
        print()
        print("When bumping dlt version, also update hub extras in pyproject.toml.")
        print("For example, if dlt is 1.20.0, hub extras should allow 0.20.x:")
        print('  "dlthub>=0.20.0a0,<0.21"')
        print('  "dlt-runtime>=0.20.0a0,<0.21"')
        return 1
    else:
        print("SUCCESS: All hub extras version requirements match dlt version!")
        return 0


if __name__ == "__main__":
    sys.exit(check_hub_extras())
