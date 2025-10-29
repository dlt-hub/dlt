from typing import ClassVar, Optional, Sequence
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration


@configspec
class WorkspaceSettings(BaseConfiguration):
    name: Optional[str] = None
    # TODO: implement default profile switch. it requires reading the configuration, discovering
    # the profile and then recreating the workspace context. since this functionality is not
    # immediately needed it will be skipped for now
    # default_profile: Optional[str] = None
    working_dir: Optional[str] = None
    """Pipeline working dirs, other writable folders, local destination files (by default). Relative to workspace root"""
    local_dir: Optional[str] = None
    """Destination local files, by default it is within data_dir/local. Relative to workspace root"""


@configspec
class WorkspaceRuntimeConfiguration(RuntimeConfiguration):
    """Extends runtime configuration with dlthub runtime"""

    # TODO: connect workspace to runtime here
    # TODO: optionally define scripts and other runtime settings


@configspec
class WorkspaceConfiguration(BaseConfiguration):
    settings: WorkspaceSettings = None
    runtime: WorkspaceRuntimeConfiguration = None
    # NOTE: is resolved separately but in the same layout
    # dashboard: DashboardConfiguration
    # TODO: launch workspace mcp using mcp configuration
    # mcp_config: McpConfiguration

    __recommended_sections__: ClassVar[Sequence[str]] = (known_sections.WORKSPACE,)
