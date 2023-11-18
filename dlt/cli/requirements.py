from typing import Sequence, List
from importlib.metadata import version as pkg_version
from packaging.requirements import Requirement

from dlt.version import DLT_PKG_NAME


class SourceRequirements:
    """Helper class to parse and manipulate entries in source's requirements.txt"""
    dlt_requirement: Requirement
    """Final dlt requirement that may be updated with destination extras"""
    dlt_requirement_base: Requirement
    """Original dlt requirement without extras"""

    def __init__(self, requirements: Sequence[str]) -> None:
        self.parsed_requirements = [Requirement(req) for req in requirements]
        self.dlt_requirement = self._ensure_dlt_requirement()
        # Dlt requirement without extras
        self.dlt_requirement_base = Requirement(str(self.dlt_requirement))
        self.dlt_requirement_base.extras.clear()

    @classmethod
    def from_string(cls, requirements: str) -> "SourceRequirements":
        """Initialize from requirements.txt string, one dependency per line"""
        return cls([line for line in requirements.splitlines() if line])

    def _ensure_dlt_requirement(self) -> Requirement:
        """Find or create dlt requirement"""
        for req in self.parsed_requirements:
            if req.name == DLT_PKG_NAME:
                return req
        req = Requirement(f"{DLT_PKG_NAME}>={self.current_dlt_version()}")
        self.parsed_requirements.append(req)
        return req

    def update_dlt_extras(self, destination_name: str) -> None:
        """Update the dlt requirement to include destination"""
        if not self.dlt_requirement:
            return
        self.dlt_requirement.extras.add(destination_name)

    def current_dlt_version(self) -> str:
        return pkg_version(DLT_PKG_NAME)

    def dlt_version_constraint(self) -> str:
        return str(self.dlt_requirement.specifier)

    def is_installed_dlt_compatible(self) -> bool:
        """Check whether currently installed version is compatible with dlt requirement

        For example, requirements.txt of the source may specify dlt>=0.3.5,<0.4.0
        and we check whether the installed dlt version (e.g. 0.3.6) falls within this range.
        """
        if not self.dlt_requirement:
            return True
        # Lets always accept pre-releases
        spec = self.dlt_requirement.specifier
        spec.prereleases = True
        return self.current_dlt_version() in spec

    def compiled(self) -> List[str]:
        return [str(req) for req in self.parsed_requirements]
