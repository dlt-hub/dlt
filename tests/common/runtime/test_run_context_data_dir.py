import os

import dlt

# import auto fixture that sets global and data dir to TEST_STORAGE
from dlt.common.runtime.run_context import DOT_DLT
from tests.utils import TEST_STORAGE_ROOT, patch_home_dir


def test_data_dir_test_storage() -> None:
    run_context = dlt.current.run()
    assert run_context.global_dir.endswith(os.path.join(TEST_STORAGE_ROOT, DOT_DLT))
    assert run_context.global_dir == run_context.data_dir
    assert os.path.isabs(run_context.global_dir)
