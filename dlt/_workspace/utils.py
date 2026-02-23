import os
from typing import List, Literal, NamedTuple, Optional

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext


TLocationScope = Literal["project", "global"]


class ProviderLocationInfo(NamedTuple):
    path: str
    present: bool
    scope: TLocationScope
    profile_name: Optional[str]


class ProviderInfo(NamedTuple):
    provider: ConfigProvider
    locations: List[ProviderLocationInfo]


def get_provider_locations() -> List[ProviderInfo]:
    """Return structured info about all config providers and their locations.

    Works with both RunContext (OSS, no profiles) and WorkspaceRunContext
    (profile-aware). Always reflects the currently active profile.
    """
    ctx_plug = Container()[PluggableRunContext]
    ctx = ctx_plug.context
    providers = ctx_plug.providers.providers

    settings_dir = os.path.abspath(ctx.settings_dir)
    global_dir = os.path.abspath(ctx.global_dir)

    result: List[ProviderInfo] = []
    for provider in providers:
        profile: Optional[str] = getattr(provider, "_profile", None)
        present_set = set(os.path.abspath(p) for p in provider.present_locations)

        loc_infos: List[ProviderLocationInfo] = []
        for path in provider.locations:
            abs_path = os.path.abspath(path)
            is_present = abs_path in present_set

            # determine scope: project if under settings_dir, global otherwise
            if abs_path.startswith(settings_dir + os.sep) or abs_path == settings_dir:
                scope: TLocationScope = "project"
            elif abs_path.startswith(global_dir + os.sep) or abs_path == global_dir:
                scope = "global"
            else:
                scope = "global"

            # determine if this specific location is profile-scoped
            profile_name: Optional[str] = None
            if profile and os.path.basename(path).startswith(f"{profile}."):
                profile_name = profile

            loc_infos.append(ProviderLocationInfo(path, is_present, scope, profile_name))

        result.append(ProviderInfo(provider, loc_infos))
    return result
