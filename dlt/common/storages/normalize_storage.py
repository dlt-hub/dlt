import os
import glob
import semver
from typing import ClassVar, Sequence

from semver import VersionInfo

from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.storages.exceptions import StorageMigrationError
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import PackageStorage
from dlt.common.storages.configuration import NormalizeStorageConfiguration
from dlt.common.utils import set_working_dir


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
            NormalizeStorage.STORAGE_VERSION,
            is_owner,
            FileStorage(config.normalize_volume_path, "t", makedirs=is_owner),
        )
        self.config = config
        if is_owner:
            self.initialize_storage()
        self.extracted_packages = PackageStorage(
            FileStorage(os.path.join(self.storage.storage_path, NormalizeStorage.EXTRACTED_FOLDER)),
            "extracted",
        )

    def initialize_storage(self) -> None:
        self.storage.create_folder(NormalizeStorage.EXTRACTED_FOLDER, exists_ok=True)

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

    def migrate_storage(self, from_version: VersionInfo, to_version: VersionInfo) -> None:
        if from_version == "1.0.0" and from_version < to_version:
            # get files in storage
            if len(self.list_files_to_normalize_sorted()) > 0:
                raise StorageMigrationError(
                    self.storage.storage_path,
                    from_version,
                    to_version,
                    f"There are extract files in {NormalizeStorage.EXTRACTED_FOLDER} folder."
                    " Storage will not migrate automatically duo to possible data loss. Delete the"
                    " files or normalize it with dlt 0.3.x",
                )
            from_version = semver.VersionInfo.parse("1.0.1")
            self._save_version(from_version)
