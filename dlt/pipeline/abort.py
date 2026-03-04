from typing import Dict, List, Optional
from dataclasses import dataclass

from dlt.common.typing import TypedDict
from dlt.common.storages import LoadStorage, NormalizeStorage


class _AbortPackageJobInfo(TypedDict):
    """Information about jobs in a package to be aborted"""

    terminal_jobs: List[str]
    transient_jobs: List[str]


class _AbortInfo(TypedDict):
    """Collects all entities that will be affected by pipeline abort"""

    packages_to_abort: Dict[str, _AbortPackageJobInfo]
    packages_to_delete: List[str]
    extracted_packages_to_delete: List[str]


@dataclass
class _AbortDryRunResult:
    """Collects all modifications that will be executed as part of the pipeline abort."""

    info: _AbortInfo


def prepare_abort_packages(
    load_storage: LoadStorage,
    normalize_storage: NormalizeStorage,
    load_ids: Optional[List[str]] = None,
) -> _AbortDryRunResult:
    """Generate abort information for the requested packages.

    Args:
        load_storage: The load storage instance
        normalize_storage: The normalize storage instance
        load_ids: Load IDs to abort. If None, all pending normalized packages.

    Returns:
        _AbortDryRunResult containing information about the abort operation.
    """
    all_normalized = load_storage.normalized_packages.list_packages()

    if load_ids is None:
        load_ids = list(load_storage.normalized_packages.list_packages())
    else:
        for load_id in load_ids:
            if load_id not in all_normalized:
                raise ValueError(f"Package {load_id} not found in pending packages")

    packages_to_abort: Dict[str, _AbortPackageJobInfo] = {}
    for load_id in load_ids:
        terminal_jobs = list(
            load_storage.normalized_packages.list_pending_jobs(load_id, "terminal")
        )
        transient_jobs = list(
            load_storage.normalized_packages.list_pending_jobs(load_id, "transient")
        )
        packages_to_abort[load_id] = _AbortPackageJobInfo(
            terminal_jobs=terminal_jobs,
            transient_jobs=transient_jobs,
        )

    packages_to_delete = [pkg for pkg in all_normalized if pkg not in load_ids]
    extracted_packages_to_delete = list(normalize_storage.extracted_packages.list_packages())

    info: _AbortInfo = _AbortInfo(
        packages_to_abort=packages_to_abort,
        packages_to_delete=packages_to_delete,
        extracted_packages_to_delete=extracted_packages_to_delete,
    )

    return _AbortDryRunResult(info=info)
