# NOTE: tests in this package are run for both the `lancedb` and `lance` destinations — `lance` has
# additional tests in `tests/load/lance` that are not run for `lancedb`

import pytest
from tests.utils import skip_if_not_active

skip_if_not_active("lancedb", "lance")
pytest.importorskip("lancedb")
