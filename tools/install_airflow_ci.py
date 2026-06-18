#!/usr/bin/env python
# ruff: noqa: T201
# flake8: noqa: T201

import argparse
import subprocess
import sys

import requests
from packaging.specifiers import SpecifierSet
from packaging.version import Version


PYPI_AIRFLOW_URL = "https://pypi.org/pypi/apache-airflow/json"
AIRFLOW_CONSTRAINTS_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/apache/airflow/"
    "constraints-{airflow_version}/constraints-{python_version}.txt"
)
REQUEST_TIMEOUT_SECONDS = 30


def is_supported_airflow_version(version: Version, specifier: SpecifierSet) -> bool:
    return version in specifier and not version.is_prerelease and not version.is_devrelease


def is_yanked_release(files: list[dict[str, object]]) -> bool:
    return bool(files) and all(bool(file.get("yanked")) for file in files)


def get_airflow_versions(specifier: SpecifierSet) -> list[Version]:
    response = requests.get(PYPI_AIRFLOW_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    releases = response.json()["releases"]
    versions = [
        Version(version)
        for version, files in releases.items()
        if is_supported_airflow_version(Version(version), specifier)
        and not is_yanked_release(files)
    ]
    versions.sort(reverse=True)
    return versions


def get_constraints_url(airflow_version: Version) -> str:
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    return AIRFLOW_CONSTRAINTS_URL_TEMPLATE.format(
        airflow_version=airflow_version,
        python_version=python_version,
    )


def constraints_exist(url: str) -> bool:
    response = requests.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT_SECONDS)
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return True


def select_airflow_release(specifier: SpecifierSet) -> tuple[Version, str]:
    for airflow_version in get_airflow_versions(specifier):
        constraints_url = get_constraints_url(airflow_version)
        if constraints_exist(constraints_url):
            return airflow_version, constraints_url

    raise RuntimeError(
        f"No apache-airflow release matching {specifier} has constraints for "
        f"Python {sys.version_info.major}.{sys.version_info.minor}"
    )


def install_airflow(airflow_version: Version, constraints_url: str) -> None:
    subprocess.check_call(
        [
            "uv",
            "pip",
            "install",
            f"apache-airflow=={airflow_version}",
            "--constraint",
            constraints_url,
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "specifier",
        help="Airflow version specifier, for example '>=2.8.0,<3' or '>=3.1,<4'",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    specifier = SpecifierSet(args.specifier)
    airflow_version, constraints_url = select_airflow_release(specifier)

    print(f"Installing apache-airflow=={airflow_version}")
    print(f"Using constraints {constraints_url}")

    install_airflow(airflow_version, constraints_url)


if __name__ == "__main__":
    main()
