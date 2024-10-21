from typing import Union

import semver

from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.exceptions import NoMigrationPathException, WrongStorageVersionException


class VersionedStorage:
    VERSION_FILE = ".version"

    def __init__(
        self, version: Union[semver.Version, str], is_owner: bool, storage: FileStorage
    ) -> None:
        if isinstance(version, str):
            version = semver.Version.parse(version)
        self.storage = storage
        # read current version
        if self.storage.has_file(VersionedStorage.VERSION_FILE):
            existing_version = self._load_version()
            if existing_version != version:
                if existing_version > version:
                    # version cannot be downgraded
                    raise NoMigrationPathException(
                        storage.storage_path, existing_version, existing_version, version
                    )
                if is_owner:
                    # only owner can migrate storage
                    self.migrate_storage(existing_version, version)
                    # storage should be migrated to desired version
                    migrated_version = self._load_version()
                    if version != migrated_version:
                        raise NoMigrationPathException(
                            storage.storage_path, existing_version, migrated_version, version
                        )
                else:
                    # we cannot use storage and we must wait for owner to upgrade it
                    raise WrongStorageVersionException(
                        storage.storage_path, existing_version, version
                    )
        else:
            if is_owner:
                self._save_version(version)
            else:
                raise WrongStorageVersionException(
                    storage.storage_path, semver.Version.parse("0.0.0"), version
                )

    def migrate_storage(self, from_version: semver.Version, to_version: semver.Version) -> None:
        # migration example:
        # # semver lib supports comparing both to string and other semvers
        # if from_version == "1.0.0" and from_version < to_version:
        #     # do migration
        #     # save migrated version
        #     from_version = semver.Version.parse("1.1.0")
        #     self._save_version(from_version)
        pass

    @property
    def version(self) -> semver.Version:
        return self._load_version()

    def _load_version(self) -> semver.Version:
        version_str = self.storage.load(VersionedStorage.VERSION_FILE)
        return semver.Version.parse(version_str)

    def _save_version(self, version: semver.Version) -> None:
        self.storage.save(VersionedStorage.VERSION_FILE, str(version))
