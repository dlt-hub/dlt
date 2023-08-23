
from tests.pipeline.utils import assert_load_info
from docs.snippets.utils import run_snippet

def test_intro_snippet() -> None:
    variables = run_snippet("intro_snippet")
    assert_load_info(variables["info"])
