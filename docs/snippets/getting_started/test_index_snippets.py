import os
import pytest

from tests.pipeline.utils import assert_load_info
from docs.snippets.utils import run_snippet, list_snippets

# we do not want github events to run because it consumes too much free github credits
RUN_SNIPPETS = set(list_snippets("getting_started")) - {"github_events_dispatch.py"}
print(RUN_SNIPPETS)


@pytest.mark.parametrize("snippet_name", RUN_SNIPPETS)
def test_snippet(snippet_name: str) -> None:
    # all our getting started examples run simple pipelines and return load_info
    variables = run_snippet(os.path.join("getting_started", snippet_name))
    assert_load_info(variables["load_info"])
