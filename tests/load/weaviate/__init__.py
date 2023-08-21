import pytest

from tests.utils import ACTIVE_DESTINATIONS

if 'weaviate' not in ACTIVE_DESTINATIONS:
    pytest.skip("weaviate not configured", allow_module_level=True)
