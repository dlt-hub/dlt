import os

def pytest_configure(config):
    # patch the configurations to use test storage by default, we modify the types (classes) fields
    # the dataclass implementation will use those patched values when creating instances (the values present
    # in the declaration are not frozen allowing patching)

    from dlt.common.configuration import RunConfiguration, LoadVolumeConfiguration, NormalizeVolumeConfiguration, SchemaVolumeConfiguration

    test_storage_root = "_storage"
    RunConfiguration.config_files_storage_path = os.path.join(test_storage_root, "config/%s")
    LoadVolumeConfiguration.load_volume_path = os.path.join(test_storage_root, "load")
    NormalizeVolumeConfiguration.normalize_volume_path = os.path.join(test_storage_root, "normalize")
    SchemaVolumeConfiguration.schema_volume_path = os.path.join(test_storage_root, "schemas")

    assert RunConfiguration.config_files_storage_path == os.path.join(test_storage_root, "config/%s")
    assert RunConfiguration().config_files_storage_path == os.path.join(test_storage_root, "config/%s")
