from importlib.metadata import version as pkg_version, distribution as pkg_distribution
from urllib.request import url2pathname
from urllib.parse import urlparse

DLT_IMPORT_NAME = "dlt"
DLT_PKG_NAME = "dlt"
__version__ = pkg_version(DLT_PKG_NAME)
DLT_PKG_REQUIREMENT = f"{DLT_PKG_NAME}=={__version__}"


def get_installed_requirement_string(package: str = DLT_PKG_NAME) -> str:
    """Gets the requirement string of currently installed dlt version"""
    dist = pkg_distribution(package)
    # PEP 610 https://packaging.python.org/en/latest/specifications/direct-url/#specification
    direct_url = dist.read_text("direct_url.json")
    if direct_url is not None:
        from dlt.common import json

        # `url` contain the location of the distribution
        url = urlparse(json.loads(direct_url)["url"])
        # we are interested only in file urls
        if url.scheme == "file":
            return url2pathname(url.path)

    if package == DLT_PKG_NAME:
        package_requirement = DLT_PKG_REQUIREMENT
    else:
        package_requirement = f"{package}=={pkg_version(package)}"
    return package_requirement
