import pytest

from tests.utils import ACTIVE_DESTINATIONS

if 'postgres' not in ACTIVE_DESTINATIONS:
    pytest.skip("postgres not configured", allow_module_level=True)
