import pytest

from tests.utils import ACTIVE_DESTINATIONS


if 'duckdb' not in ACTIVE_DESTINATIONS:
    pytest.skip('duckdb not configured', allow_module_level=True)
