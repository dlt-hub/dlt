import pytest

from tests.utils import ALL_DESTINATIONS


if 'duckdb' not in ALL_DESTINATIONS:
    pytest.skip('duckdb not configured', allow_module_level=True)
