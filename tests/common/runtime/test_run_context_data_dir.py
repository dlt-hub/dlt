import dlt

# import auto fixture that sets global and data dir to TEST_STORAGE
from tests.utils import TEST_STORAGE_ROOT, patch_home_dir


def test_data_dir_test_storage() -> None:
    run_context = dlt.current.run()
    assert run_context.global_dir.endswith(TEST_STORAGE_ROOT)
    assert run_context.global_dir == run_context.data_dir
