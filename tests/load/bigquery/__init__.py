import pytest

from tests.utils import ACTIVE_DESTINATIONS

if 'bigquery' not in ACTIVE_DESTINATIONS:
    pytest.skip("bigquery not configured", allow_module_level=True)
