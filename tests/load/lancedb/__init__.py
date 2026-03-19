import pytest
from tests.utils import skip_if_not_active

skip_if_not_active("lancedb", "lance")
pytest.importorskip("lancedb")
