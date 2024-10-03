import os
from typing import ClassVar

from dlt.common.configuration import plugins
from dlt.common.configuration.specs.pluggable_run_context import SupportsRunContext
from dlt.common.runtime.run_context import RunContext, DOT_DLT

from tests.utils import TEST_STORAGE_ROOT


class RunContextTest(RunContext):
    CONTEXT_NAME: ClassVar[str] = "dlt-test"

    @property
    def run_dir(self) -> str:
        return os.path.abspath("tests")

    @property
    def settings_dir(self) -> str:
        return os.path.join(self.run_dir, DOT_DLT)

    @property
    def data_dir(self) -> str:
        return os.path.abspath(TEST_STORAGE_ROOT)


@plugins.hookimpl(specname="plug_run_context")
def plug_run_context_impl() -> SupportsRunContext:
    return RunContextTest()
