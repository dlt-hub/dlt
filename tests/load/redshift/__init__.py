import pytest
from tests.utils import ACTIVE_DESTINATIONS


if 'redshift' not in ACTIVE_DESTINATIONS:
    pytest.skip("redshift not configured", allow_module_level=True)

