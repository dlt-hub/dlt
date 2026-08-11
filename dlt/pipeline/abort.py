import os
from typing import Dict, List, Optional, Tuple

from dlt.common.typing import TypedDict
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


class TAbortPlan(TypedDict):
    """Plan of what a pipeline abort will remove and where the local state rewinds to."""

    package_to_abort: Optional[TAbortPackageInfo]
    """the oldest normalized package (the one being loaded), aborted with a record; None otherwise"""
    packages_to_delete: List[str]
    extracted_packages_to_delete: List[str]
    restore_from_load_id: str
    """oldest package being removed; local state and schemas rewind to its start snapshot"""


def prepare_abort_packages(
    load_storage: LoadStorage,
    normalize_storage: NormalizeStorage,
    load_id: Optional[str] = None,
) -> Optional[TAbortPlan]:
    """Plans an abort of packages starting at `load_id` (the oldest pending when None), or None
    when nothing is pending. Packages older than `load_id` are left intact; `load_id` and
    everything newer, extracted packages included, are removed and the local state rewinds to
    `load_id`'s start. The oldest pending normalized package (the one being loaded) is the only
    one aborted with a record; its jobs are classified the way the loader will process them.
    `load_id` may already be aborted (moved to loaded storage) so this planner can drive the
    post-abort cleanup too."""
    all_normalized: List[str] = []
    if load_storage.is_storage_ready():
        all_normalized = list(load_storage.normalized_packages.list_packages())
    # a passed load_id may already be aborted (moved to loaded storage): accept it so the same
    # planner drives the abort cleanup, deleting only the packages newer than it
    already_aborted = False
    if load_id is not None and load_id not in all_normalized:
        if load_storage.is_storage_ready() and load_id in load_storage.list_loaded_packages():
            already_aborted = True
        else:
            raise LoadPackageNotFound(load_id)

    packages = load_storage.normalized_packages
    package_to_abort: Optional[TAbortPackageInfo] = None
    packages_to_delete: List[str] = []
    target: Optional[str] = None
    if already_aborted:
        target = load_id
        packages_to_delete = [lid for lid in all_normalized if lid > target]
    elif all_normalized:
        target = load_id or all_normalized[0]
        if target == all_normalized[0]:
            package_to_abort = _classify_jobs(packages, target)
            packages_to_delete = all_normalized[1:]
        else:
            # not being loaded, delete without a record together with newer packages
            packages_to_delete = all_normalized[all_normalized.index(target) :]

    all_extracted: List[str] = []
    if normalize_storage.is_storage_ready():
        all_extracted = list(normalize_storage.extracted_packages.list_packages())
    # keep extracted packages older than the target, like normalized ones
    extracted_packages_to_delete = (
        all_extracted if target is None else [lid for lid in all_extracted if lid >= target]
    )

    # the restore anchor is the oldest package being removed; None means nothing is pending
    restore_from_load_id = target
    if restore_from_load_id is None and extracted_packages_to_delete:
        restore_from_load_id = min(extracted_packages_to_delete)
    if restore_from_load_id is None:
        return None

    return TAbortPlan(
        package_to_abort=package_to_abort,
        packages_to_delete=packages_to_delete,
        extracted_packages_to_delete=extracted_packages_to_delete,
        restore_from_load_id=restore_from_load_id,
    )


def execute_abort_plan(
    load_storage: LoadStorage,
    normalize_storage: NormalizeStorage,
    plan: TAbortPlan,
) -> Optional[Tuple[Optional[str], Dict[str, str]]]:
    """Deletes the packages the plan marked for deletion and returns the restore snapshot for
    the caller to install (or None). The snapshot is read before any deletion because the
    package that carries it may itself be deleted."""
    snapshot = _load_restore_snapshot(load_storage, normalize_storage, plan["restore_from_load_id"])
    for load_id in plan["extracted_packages_to_delete"]:
        normalize_storage.extracted_packages.delete_package(load_id)
    for load_id in plan["packages_to_delete"]:
        load_storage.normalized_packages.delete_package(load_id)
    return snapshot


def _load_restore_snapshot(
    load_storage: LoadStorage,
    normalize_storage: NormalizeStorage,
    restore_from_load_id: str,
) -> Optional[Tuple[Optional[str], Dict[str, str]]]:
    """Loads the (state blob, schema blobs) snapshot of `restore_from_load_id` from wherever the
    package currently lives - normalized, loaded (after an abort), or extracted. None if no
    snapshot."""
    for pkgs in (
        load_storage.normalized_packages,
        load_storage.loaded_packages,
        normalize_storage.extracted_packages,
    ):
        if pkgs.can_restore_pipeline_state(restore_from_load_id):
            return pkgs.load_pipeline_state(restore_from_load_id)
    return None


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
