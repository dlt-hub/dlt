import semver
from dlt.common.exceptions import DltException

class StorageException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class NoMigrationPathException(StorageException):
    def __init__(self, storage_path: str, initial_version: semver.VersionInfo, migrated_version: semver.VersionInfo, target_version: semver.VersionInfo) -> None:
        self.storage_path = storage_path
        self.initial_version = initial_version
        self.migrated_version = migrated_version
        self.target_version = target_version
        super().__init__(f"Could not find migration path for {storage_path} from v {initial_version} to {target_version}, stopped at {migrated_version}")


class WrongStorageVersionException(StorageException):
    def __init__(self, storage_path: str, initial_version: semver.VersionInfo, target_version: semver.VersionInfo) -> None:
        self.storage_path = storage_path
        self.initial_version = initial_version
        self.target_version = target_version
        super().__init__(f"Expected storage {storage_path} with v {target_version} but found {initial_version}")
