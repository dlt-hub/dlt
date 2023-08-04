import pytest

from tests.utils import ALL_DESTINATIONS

if 'postgres' not in ALL_DESTINATIONS:
    pytest.skip("postgres not configured", allow_module_level=True)
