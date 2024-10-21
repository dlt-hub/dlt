import os

import dlt

# import auto fixture that sets global and data dir to TEST_STORAGE + random folder
from tests.utils import TEST_STORAGE_ROOT, patch_random_home_dir


def test_data_dir_test_storage() -> None:
    run_context = dlt.current.run()
    assert TEST_STORAGE_ROOT in run_context.global_dir
    assert "global_" in run_context.global_dir
    assert run_context.global_dir == run_context.data_dir
    assert os.path.isabs(run_context.global_dir)
