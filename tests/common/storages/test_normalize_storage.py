import pytest

from dlt.common.utils import uniq_id
from dlt.common.storages import NormalizeStorage, NormalizeStorageConfiguration
from dlt.common.storages.exceptions import NoMigrationPathException
from dlt.common.storages.normalize_storage import TParsedNormalizeFileName

from tests.utils import write_version, autouse_test_storage


@pytest.mark.skip()
def test_load_events_and_group_by_sender() -> None:
    # TODO: create fixture with two sender ids and 3 files and check the result
    pass


def test_build_extracted_file_name() -> None:
    load_id = uniq_id()
    name = NormalizeStorage.build_extracted_file_stem("event", "table_with_parts__many", load_id) + ".jsonl"
    assert NormalizeStorage.get_schema_name(name) == "event"
    assert NormalizeStorage.parse_normalize_file_name(name) == TParsedNormalizeFileName("event", "table_with_parts__many", load_id, "jsonl")

    # empty schema should be supported
    name = NormalizeStorage.build_extracted_file_stem("", "table", load_id) + ".jsonl"
    assert NormalizeStorage.parse_normalize_file_name(name) == TParsedNormalizeFileName("", "table", load_id, "jsonl")


def test_full_migration_path() -> None:
    # create directory structure
    s = NormalizeStorage(True)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = NormalizeStorage(True)
    assert s.version == NormalizeStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = NormalizeStorage(True)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        NormalizeStorage(False)
