import os
import glob
import semver
from typing import ClassVar, Dict, List, Mapping, Optional, Sequence

from semver import Version

from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.storages.exceptions import StorageMigrationError
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import PackageStorage, ParsedLoadJobFileName
from dlt.common.storages.configuration import NormalizeStorageConfiguration
from dlt.common.utils import set_working_dir


def group_worker_files(
    files: Sequence[str], no_groups: int, file_sizes: Optional[Mapping[str, int]] = None
) -> List[List[str]]:
    """Groups job `files` for `no_groups` workers, each gets a similar size of every file format.

    Files of a format are sorted by name and cut into contiguous groups, so the files of a table
    stay together. Without `file_sizes` all files have the same size.
    """
    groups: List[List[str]] = [[] for _ in range(min(no_groups, len(files)))]
    loads = [0] * len(groups)
    by_format: Dict[str, List[str]] = {}
    for file_name in sorted(files):
        file_format = ParsedLoadJobFileName.parse(file_name).file_format
        by_format.setdefault(file_format, []).append(file_name)
    # normalize may parse or just link a format, so formats are balanced separately
    for format_files in by_format.values():
        weights = {f: file_sizes[f] if file_sizes else 1 for f in format_files}
        if not any(weights.values()):
            weights = dict.fromkeys(format_files, 1)
        segments = _split_contiguous(format_files, [weights[f] for f in format_files], len(groups))
        segments.sort(key=lambda segment: -sum(weights[f] for f in segment))
        # the largest segment goes to the least loaded worker, empty workers first
        targets = sorted(range(len(groups)), key=lambda i: (loads[i], len(groups[i]), i))
        for segment, target in zip(segments, targets):
            groups[target].extend(segment)
            loads[target] += sum(weights[f] for f in segment)
    # each file goes to exactly one worker
    assert sorted(f for group in groups for f in group) == sorted(files)
    return groups


def _split_contiguous(files: List[str], sizes: List[int], no_groups: int) -> List[List[str]]:
    """Cuts `files` into `no_groups` contiguous segments with the smallest possible largest
    segment (the linear partition problem), each close to an equal share of the size not yet
    assigned.
    """
    no_groups = min(no_groups, len(files))
    # binary search on the largest segment, a greedy fill tells if it is feasible
    low, high = max(sizes), sum(sizes)
    while low < high:
        cap = (low + high) // 2
        if _segments_needed(sizes, cap)[0] <= no_groups:
            high = cap
        else:
            low = cap + 1
    needed = _segments_needed(sizes, low)
    total = sum(sizes)
    segments: List[List[str]] = [[]]
    segment_start = running = 0
    for idx, (file_name, size) in enumerate(zip(files, sizes)):
        segments_left = no_groups - len(segments)
        if segments[-1] and segments_left:
            target = segment_start + (total - segment_start) / (segments_left + 1)
            over_cap = running + size - segment_start > low
            balanced = running + size / 2 > target or len(files) - idx <= segments_left
            # cut early only if the rest still fits the remaining segments
            if over_cap or (balanced and needed[idx] <= segments_left):
                segments.append([])
                segment_start = running
        segments[-1].append(file_name)
        running += size
    return segments


def _segments_needed(sizes: List[int], cap: int) -> List[int]:
    """Segments needed for the sizes from each position on when segments are filled up to `cap`"""
    # end of the segment filled from each position, sizes are not negative so ends never decrease
    ends: List[int] = []
    end = filled = 0
    for start in range(len(sizes)):
        if end <= start:
            end, filled = start, 0
        while end < len(sizes) and (end == start or filled + sizes[end] <= cap):
            filled += sizes[end]
            end += 1
        ends.append(end)
        filled -= sizes[start]
    needed = [0] * (len(sizes) + 1)
    for start in range(len(sizes) - 1, -1, -1):
        needed[start] = 1 + needed[ends[start]]
    return needed


class NormalizeStorage(VersionedStorage):
    STORAGE_VERSION: ClassVar[str] = "1.0.1"
    EXTRACTED_FOLDER: ClassVar[str] = (
        "extracted"  # folder within the volume where extracted files to be normalized are stored
    )

    @with_config(spec=NormalizeStorageConfiguration, sections=(known_sections.NORMALIZE,))
    def __init__(
        self, is_owner: bool, config: NormalizeStorageConfiguration = config.value
    ) -> None:
        super().__init__(
            FileStorage(config.normalize_volume_path, "t", makedirs=is_owner),
        )
        self.config = config
        if is_owner or self.is_storage_ready():
            # allow migrations for non-owner when storage is already created
            self.ensure_migration(NormalizeStorage.STORAGE_VERSION, True)
        if is_owner:
            self.initialize_storage()
        self.extracted_packages = PackageStorage(
            FileStorage(os.path.join(self.storage.storage_path, NormalizeStorage.EXTRACTED_FOLDER)),
            "extracted",
        )

    def initialize_storage(self) -> None:
        self.storage.create_folder(NormalizeStorage.EXTRACTED_FOLDER, exists_ok=True)

    def is_storage_ready(self) -> bool:
        return self.storage.has_folder(NormalizeStorage.EXTRACTED_FOLDER)

    def list_files_to_normalize_sorted(self) -> Sequence[str]:
        """Gets all data files in extracted packages storage. This method is compatible with current and all past storages"""
        root_dir = os.path.join(self.storage.storage_path, NormalizeStorage.EXTRACTED_FOLDER)
        with set_working_dir(root_dir):
            files = glob.glob("**/*", recursive=True)
            # return all files that are not schema files
            return sorted(
                [
                    file
                    for file in files
                    if not file.endswith(PackageStorage.SCHEMA_FILE_NAME)
                    and os.path.isfile(file)
                    and not file.endswith(PackageStorage.LOAD_PACKAGE_STATE_FILE_NAME)
                ]
            )

    def migrate_storage(self, from_version: Version, to_version: Version) -> None:
        if from_version == "1.0.0" and from_version < to_version:
            # get files in storage
            if len(self.list_files_to_normalize_sorted()) > 0:
                raise StorageMigrationError(
                    self.storage.storage_path,
                    from_version,
                    to_version,
                    f"There are extract files in folder `{NormalizeStorage.EXTRACTED_FOLDER}`."
                    " Storage will not migrate automatically due to possible data loss. Delete the"
                    " files or normalize it with dlt 0.3.x",
                )
            from_version = semver.Version.parse("1.0.1")
            self._save_version(from_version)
