
from tests.pipeline.utils import assert_load_info
from docs.snippets.utils import run_snippet

def test_snippet_1() -> None:
    variables = run_snippet("getting_started/index_snippet_start")
    assert_load_info(variables["load_info"])


def test_snippet_load_data() -> None:
    variables = run_snippet("getting_started/index_snippet_load_data")
    assert_load_info(variables["load_info"])
