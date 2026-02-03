import os

import dlt

# import auto fixture that sets global and data dir to TEST_STORAGE
from dlt.common.runtime.run_context import DOT_DLT
from tests.utils import get_test_storage_root, auto_test_run_context


def test_data_dir_test_storage() -> None:
    run_context = dlt.current.run_context()
    assert run_context.global_dir.endswith(os.path.join(get_test_storage_root(), DOT_DLT))
    assert run_context.global_dir == run_context.data_dir
    assert os.path.isabs(run_context.global_dir)
