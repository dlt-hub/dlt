import pytest

from tests.utils import ACTIVE_DESTINATIONS

if 'snowflake' not in ACTIVE_DESTINATIONS:
    pytest.skip("snowflake not configured", allow_module_level=True)
