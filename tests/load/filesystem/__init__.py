import pytest

from tests.utils import ACTIVE_DESTINATIONS

if 'filesystem' not in ACTIVE_DESTINATIONS:
    pytest.skip("filesystem not configured", allow_module_level=True)