import os
from typing import List, Optional
from dataclasses import dataclass

from dlt.common.typing import TypedDict
from dlt.common.pipeline import LoadInfo
from dlt.common.storages import LoadStorage, NormalizeStorage, PackageStorage
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.storages.load_package import ParsedLoadJobFileName


class TAbortPackageInfo(TypedDict):
    """The single package to be aborted with a record and how its jobs will be processed."""

    load_id: str
    terminal_jobs: List[str]
    """retried or failed-in-flight jobs with terminal errors, will be moved to failed_jobs"""
    transient_jobs: List[str]
    """retried jobs with transient errors, will be moved to failed_jobs"""
    interrupted_jobs: List[str]
    """started jobs without a recorded outcome, will be moved to failed_jobs"""
    committed_jobs: List[str]
    """started jobs already committed to the destination, will be moved to completed_jobs"""


class TAbortInfo(TypedDict):
    """Collects all entities that will be affected by pipeline abort"""

    package_to_abort: Optional[TAbortPackageInfo]
    """the oldest normalized package (the one being loaded), aborted with a record; None otherwise"""
    packages_to_delete: List[str]
    extracted_packages_to_delete: List[str]


@dataclass
class AbortPackagesResult:
    """Collects all modifications executed as part of the pipeline abort."""

    info: TAbortInfo
    load_info: Optional[LoadInfo] = None
    """info of the load run that processed the abort, None when nothing was being loaded"""

    @property
    def is_empty(self) -> bool:
        return not (
            self.info["package_to_abort"]
            or self.info["packages_to_delete"]
            or self.info["extracted_packages_to_delete"]
        )


def prepare_abort_packages(
    load_storage: LoadStorage,
    normalize_storage: NormalizeStorage,
    load_id: Optional[str] = None,
) -> AbortPackagesResult:
    """Generates abort information for packages starting at `load_id` (the oldest pending
    when None). Packages older than `load_id` are left intact. The oldest normalized package
    is assumed to be the one being loaded and is the only one aborted with a record; newer
    packages and all extracted ones are deleted. Jobs of the aborted package are classified
    the way the loader will process them, reading (but not resolving) pending transitions."""
    all_normalized: List[str] = []
    if load_storage.is_storage_ready():
        all_normalized = list(load_storage.normalized_packages.list_packages())
    if load_id is not None and load_id not in all_normalized:
        raise LoadPackageNotFound(load_id)

    packages = load_storage.normalized_packages
    package_to_abort: Optional[TAbortPackageInfo] = None
    packages_to_delete: List[str] = []
    if all_normalized:
        target = load_id or all_normalized[0]
        if target == all_normalized[0]:
            package_to_abort = _classify_jobs(packages, target)
            packages_to_delete = all_normalized[1:]
        else:
            # not being loaded, delete without a record together with newer packages
            packages_to_delete = all_normalized[all_normalized.index(target) :]

    extracted_packages_to_delete: List[str] = []
    if normalize_storage.is_storage_ready():
        extracted_packages_to_delete = list(normalize_storage.extracted_packages.list_packages())

    info: TAbortInfo = TAbortInfo(
        package_to_abort=package_to_abort,
        packages_to_delete=packages_to_delete,
        extracted_packages_to_delete=extracted_packages_to_delete,
    )

    return AbortPackagesResult(info=info)


def _classify_jobs(packages: PackageStorage, load_id: str) -> TAbortPackageInfo:
    job_info = TAbortPackageInfo(
        load_id=load_id,
        terminal_jobs=[],
        transient_jobs=[],
        interrupted_jobs=[],
        committed_jobs=[],
    )
    for job_file, folder in packages.list_retried_new_jobs(load_id):
        job_name = os.path.basename(job_file)
        if folder == "new_jobs":
            exc_type, _ = packages.get_last_job_exception(
                load_id, ParsedLoadJobFileName.parse(job_name)
            )
            job_info["terminal_jobs" if exc_type == "terminal" else "transient_jobs"].append(
                job_file
            )
        elif packages.load_pending_transition(load_id, job_name) is None:
            job_info["interrupted_jobs"].append(job_file)
        else:
            job_info["transient_jobs"].append(job_file)
    # started jobs with recorded outcomes are replayed by abort, not failed
    for job_file in packages.list_started_jobs(load_id):
        pending = packages.load_pending_transition(load_id, os.path.basename(job_file))
        if pending is None:
            continue
        if pending[0] == "completed":
            job_info["committed_jobs"].append(job_file)
        elif pending[0] == "failed":
            job_info["terminal_jobs"].append(job_file)
    return job_info
