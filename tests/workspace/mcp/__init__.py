import sys
import pytest

# skip the whole module on python < 3.10
if sys.version_info < (3, 10):
    pytest.skip("requires Python 3.10+", allow_module_level=True)
