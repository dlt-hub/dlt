import pytest
from tests.utils import skip_if_not_active

skip_if_not_active("lancedb")
pytest.importorskip("lancedb")
