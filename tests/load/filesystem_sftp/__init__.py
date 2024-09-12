import pytest
from tests.utils import skip_if_not_active
from tests.load.utils import ALL_FILESYSTEM_DRIVERS

skip_if_not_active("filesystem")

if "sftp" not in ALL_FILESYSTEM_DRIVERS:
    pytest.skip("sftp filesystem driver not configured", allow_module_level=True)
