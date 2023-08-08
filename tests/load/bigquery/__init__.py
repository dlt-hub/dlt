import pytest

from tests.utils import ALL_DESTINATIONS

if 'bigquery' not in ALL_DESTINATIONS:
    pytest.skip("bigquery not configured", allow_module_level=True)
