import pytest
from tests.utils import ALL_DESTINATIONS


if 'redshift' not in ALL_DESTINATIONS:
    pytest.skip("redshift not configured", allow_module_level=True)

