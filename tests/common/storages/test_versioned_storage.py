import pytest
import semver

from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.exceptions import NoMigrationPathException, WrongStorageVersionException
from dlt.common.storages.versioned_storage import VersionedStorage

from tests.utils import write_version, test_storage


class MigratedStorage(VersionedStorage):
    def migrate_storage(self, from_version: semver.Version, to_version: semver.Version) -> None:
        # migration example:
        if from_version == "1.0.0" and from_version < to_version:
            from_version = semver.Version.parse("1.1.0")
            self._save_version(from_version)
        if from_version == "1.1.0" and from_version < to_version:
            from_version = semver.Version.parse("1.2.0")
            self._save_version(from_version)


def test_new_versioned_storage(test_storage: FileStorage) -> None:
    v = VersionedStorage("1.0.1", True, test_storage)
    assert v.version == "1.0.1"


def test_new_versioned_storage_non_owner(test_storage: FileStorage) -> None:
    with pytest.raises(WrongStorageVersionException) as wsve:
        VersionedStorage("1.0.1", False, test_storage)
    assert wsve.value.storage_path == test_storage.storage_path
    assert wsve.value.target_version == "1.0.1"
    assert wsve.value.initial_version == "0.0.0"


def test_migration(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.0.0")
    v = MigratedStorage("1.2.0", True, test_storage)
    assert v.version == "1.2.0"


def test_unknown_migration_path(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.0.0")
    with pytest.raises(NoMigrationPathException) as wmpe:
        MigratedStorage("1.3.0", True, test_storage)
    assert wmpe.value.migrated_version == "1.2.0"


def test_only_owner_migrates(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.0.0")
    with pytest.raises(WrongStorageVersionException) as wmpe:
        MigratedStorage("1.2.0", False, test_storage)
    assert wmpe.value.initial_version == "1.0.0"


def test_downgrade_not_possible(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.2.0")
    with pytest.raises(NoMigrationPathException) as wmpe:
        MigratedStorage("1.1.0", True, test_storage)
    assert wmpe.value.migrated_version == "1.2.0"
