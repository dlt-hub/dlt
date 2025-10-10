"""A module that imports all pluggy plugins implementations to work with pyproject entrypoint"""
from typing import Any, Dict, Optional

from dlt.common.configuration import plugins as _plugins
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.runtime.run_context import RunContext

# import workspace plugins before core plugin
from dlt._workspace._plugins import *  # noqa
from dlt._workspace.cli._plugins import *  # noqa


# make sure OSS context is tried last
@_plugins.hookimpl(specname="plug_run_context", trylast=True)
def plug_run_context_impl(
    run_dir: Optional[str], runtime_kwargs: Optional[Dict[str, Any]]
) -> Optional[RunContextBase]:
    return RunContext(run_dir)
