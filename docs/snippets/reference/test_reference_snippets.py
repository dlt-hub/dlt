import os
import pytest

from tests.pipeline.utils import assert_load_info
from docs.snippets.utils import run_snippet, list_snippets

# we do not want github events to run because it consumes too much free github credits
RUN_SNIPPETS = list_snippets("reference") + ["parallel_load/parallel_load.py"]


# @pytest.mark.parametrize("snippet_name", RUN_SNIPPETS)
# def test_snippet(snippet_name: str) -> None:
#     run_snippet(os.path.join("reference", snippet_name))
