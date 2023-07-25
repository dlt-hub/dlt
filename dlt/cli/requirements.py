from typing import Sequence, List, Optional, Iterator
from importlib.metadata import version as pkg_version
from packaging.requirements import Requirement

from dlt.version import DLT_PKG_NAME


class SourceRequirements:
    """Helper class to parse and manipulate entries in source's requirements.txt"""
    dlt_requirement: Requirement

    def __init__(self, requirements: Sequence[str]) -> None:
        self.parsed_requirements = [Requirement(req) for req in requirements]
        self.dlt_requirement = self._ensure_dlt_requirement()

    def _ensure_dlt_requirement(self) -> Requirement:
        """Find or create dlt requirement"""
        for req in self.parsed_requirements:
            if req.name == DLT_PKG_NAME:
                return req
        req = Requirement(f"{DLT_PKG_NAME}>={pkg_version(DLT_PKG_NAME)}")
        self.parsed_requirements.append(req)
        return req

    def update_dlt_extras(self, destination_name: str) -> None:
        """Update the dlt requirement to include destination"""
        if not self.dlt_requirement:
            return
        self.dlt_requirement.extras.add(destination_name)

    def installed_dlt_is_compatible(self) -> bool:
        """Check whether currently installed version is compatible with dlt requirement

        For example, requirements.txt of the source may specifiy dlt>=0.3.5,<0.4.0
        and we check whether the installed dlt version (e.g. 0.3.6) falls within this range.
        """
        if not self.dlt_requirement:
            return True
        # Lets always accept pre-releases
        spec = self.dlt_requirement.specifier
        spec.prereleases = True
        return pkg_version(DLT_PKG_NAME) in spec

    def compiled(self) -> List[str]:
        return [str(req) for req in self.parsed_requirements]
