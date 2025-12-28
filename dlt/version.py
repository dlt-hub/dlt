from importlib.metadata import version as pkg_version, distribution as pkg_distribution
from typing import Optional
from urllib.request import url2pathname
from urllib.parse import urlparse
from packaging.requirements import Requirement

DLT_IMPORT_NAME = "dlt"
PKG_NAME = DLT_PKG_NAME = "dlt"
__version__ = pkg_version(DLT_PKG_NAME)
DLT_PKG_REQUIREMENT = f"{DLT_PKG_NAME}=={__version__}"


def get_installed_requirement_string(
    package: str = DLT_PKG_NAME, allow_earlier: bool = False
) -> str:
    """Gets the requirement string of currently installed dlt version"""
    dist = pkg_distribution(package)
    # PEP 610 https://packaging.python.org/en/latest/specifications/direct-url/#specification
    direct_url = dist.read_text("direct_url.json")
    if direct_url is not None:
        from dlt.common.json import json

        # `url` contain the location of the distribution
        url = urlparse(json.loads(direct_url)["url"])
        # we are interested only in file urls
        if url.scheme == "file":
            return url2pathname(url.path)

    ver_selector = "<=" if allow_earlier else "=="
    if package == DLT_PKG_NAME:
        package_requirement = f"{DLT_PKG_NAME}{ver_selector}{__version__}"
    else:
        package_requirement = f"{package}{ver_selector}{pkg_version(package)}"
    return package_requirement


def get_dependency_requirement(
    dependency_name: str, package: str = DLT_PKG_NAME
) -> Optional[Requirement]:
    """Find a specific dependency requirement from package metadata"""
    dist = pkg_distribution(package)

    if dist.requires is None:
        return None

    for req_str in dist.requires:
        req = Requirement(req_str)
        if req.name == dependency_name:
            return req
    return None
