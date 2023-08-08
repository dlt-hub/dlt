import pytest

from tests.utils import ALL_DESTINATIONS

if 'snowflake' not in ALL_DESTINATIONS:
    pytest.skip("snowflake not configured", allow_module_level=True)
