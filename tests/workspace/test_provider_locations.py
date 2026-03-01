import os
from pathlib import Path
from typing import Iterator, List

import pytest

from dlt._workspace.cli.ai import (
    ai_secrets_list_command,
    ai_secrets_view_redacted_command,
    ai_secrets_update_fragment_command,
)
from dlt._workspace.cli.utils import get_provider_locations
from dlt._workspace.typing import ProviderInfo, ProviderLocationInfo

from tests.workspace.utils import isolated_workspace


@pytest.fixture
def default_workspace(autouse_test_storage: None, preserve_run_context: None) -> Iterator[None]:
    with isolated_workspace("default", profile="dev"):
        yield


@pytest.fixture
def default_workspace_no_profile(
    autouse_test_storage: None, preserve_run_context: None
) -> Iterator[None]:
    with isolated_workspace("default"):
        yield


def _secrets_with_locations(infos: List[ProviderInfo]) -> ProviderInfo:
    """Find the first secrets provider that has file locations."""
    for i in infos:
        if i.provider.supports_secrets and len(i.locations) > 0:
            return i
    raise AssertionError("No secrets provider with locations found")


def _config_with_locations(infos: List[ProviderInfo]) -> ProviderInfo:
    """Find the first non-secrets provider that has file locations."""
    for i in infos:
        if not i.provider.supports_secrets and len(i.locations) > 0:
            return i
    raise AssertionError("No config provider with locations found")


def test_get_provider_locations_workspace(default_workspace: None) -> None:
    """Workspace context with dev profile returns profile-scoped locations."""
    infos = get_provider_locations()
    assert len(infos) > 0

    secrets_info = _secrets_with_locations(infos)

    # check for profile-scoped location
    profile_locs = [loc for loc in secrets_info.locations if loc.profile_name == "dev"]
    assert len(profile_locs) >= 1
    # profile-scoped file in project dir should be present (dev.secrets.toml exists)
    project_profile = [loc for loc in profile_locs if loc.scope == "project"]
    assert len(project_profile) >= 1
    assert project_profile[0].present is True

    # check for base (non-profile) project location
    base_project = [
        loc for loc in secrets_info.locations if loc.scope == "project" and loc.profile_name is None
    ]
    assert len(base_project) >= 1
    # secrets.toml exists in the default workspace
    assert base_project[0].present is True

    # global locations should exist (scope="global")
    global_locs = [loc for loc in secrets_info.locations if loc.scope == "global"]
    assert len(global_locs) >= 1
    # global locations are not present (mocked global_dir is empty)
    for loc in global_locs:
        assert loc.present is False


def test_get_provider_locations_non_secrets(default_workspace: None) -> None:
    """Config providers are returned with supports_secrets=False."""
    infos = get_provider_locations()

    config_info = _config_with_locations(infos)
    # config.toml should exist in the default workspace
    project_locs = [loc for loc in config_info.locations if loc.scope == "project"]
    assert len(project_locs) >= 1


def test_get_provider_locations_profile_switch(
    autouse_test_storage: None, preserve_run_context: None
) -> None:
    """After profile switch, locations reflect the new profile."""
    with isolated_workspace("default", profile="dev") as ctx:
        infos_dev = get_provider_locations()
        secrets_dev = _secrets_with_locations(infos_dev)
        dev_profiles = [loc for loc in secrets_dev.locations if loc.profile_name == "dev"]
        assert len(dev_profiles) >= 1

        # switch to prod (no prod.secrets.toml in default workspace)
        ctx.switch_profile("prod")
        infos_prod = get_provider_locations()
        secrets_prod = _secrets_with_locations(infos_prod)

        # no dev-scoped locations after switch
        dev_after = [loc for loc in secrets_prod.locations if loc.profile_name == "dev"]
        assert len(dev_after) == 0

        # prod-scoped locations appear
        prod_locs = [loc for loc in secrets_prod.locations if loc.profile_name == "prod"]
        assert len(prod_locs) >= 1
        # prod.secrets.toml doesn't exist in default workspace
        for loc in prod_locs:
            assert loc.present is False


def test_get_provider_locations_oss(autouse_test_storage: None, preserve_run_context: None) -> None:
    """OSS context (RunContext) has no profile info on providers."""
    # use "legacy" workspace which doesn't have .workspace marker
    with isolated_workspace("legacy", required="RunContext"):
        infos = get_provider_locations()
        assert len(infos) > 0

        for info in infos:
            for loc in info.locations:
                # no profile info in OSS context
                assert loc.profile_name is None

        # secrets and config providers should exist with file locations
        file_providers = [i for i in infos if len(i.locations) > 0]
        assert len(file_providers) >= 1


def test_get_provider_locations_environ_provider(default_workspace: None) -> None:
    """EnvironProvider has empty locations list."""
    infos = get_provider_locations()

    # find providers with no locations (EnvironProvider)
    no_loc_infos = [i for i in infos if len(i.locations) == 0]
    assert len(no_loc_infos) >= 1


def test_ai_secrets_list_oss(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """OSS context lists secrets.toml without profiles."""
    # note: test conftest patches RunContext.initial_providers to use tests/.dlt
    # so providers point to tests/.dlt, not the workspace's .dlt — that's expected
    with isolated_workspace("legacy", required="RunContext"):
        ai_secrets_list_command()
        output = capsys.readouterr().out

        assert "Secret file locations:" in output
        # no profile tags in OSS context
        assert "profile:" not in output
        # no "not found"
        assert "not found" not in output.lower()


def test_ai_secrets_list_workspace_with_profile(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Workspace context lists profile-scoped path first, no global."""
    with isolated_workspace("default", profile="dev"):
        ai_secrets_list_command()
        output = capsys.readouterr().out

        assert "Secret file locations:" in output
        # profile path listed
        assert "dev.secrets.toml" in output
        assert "profile: dev" in output
        # base path listed
        assert "secrets.toml" in output
        # no global
        assert "global" not in output.lower()
        # no "not found"
        assert "not found" not in output.lower()
        # profile-scoped appears before base
        lines = [line.strip() for line in output.splitlines() if "secrets.toml" in line]
        assert len(lines) >= 2
        assert "dev.secrets.toml" in lines[0]


def test_ai_secrets_view_redacted_missing_creates_nothing(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """view-redacted on a missing file warns but does not create the file."""
    with isolated_workspace("empty", profile="dev"):
        secrets_path = os.path.join(".dlt", "secrets.toml")
        assert not os.path.exists(secrets_path)

        ai_secrets_view_redacted_command(path=secrets_path)
        output = capsys.readouterr().out

        assert "not found" in output.lower()
        # file should NOT be created
        assert not os.path.exists(secrets_path)


def test_ai_secrets_update_fragment_creates_file(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """update-fragment creates secrets file if it doesn't exist."""
    with isolated_workspace("empty", profile="dev"):
        secrets_path = os.path.join(".dlt", "secrets.toml")
        assert not os.path.exists(secrets_path)

        ai_secrets_update_fragment_command(
            fragment='[sources.my_api]\napi_key = "sk-test-xxx"\n',
            path=secrets_path,
        )
        output = capsys.readouterr().out

        # file was created
        assert os.path.isfile(secrets_path)
        # output shows redacted content
        assert "api_key" in output
        assert "sk-test-xxx" not in output
        assert "**" in output
        # actual file has the real value
        content = Path(secrets_path).read_text(encoding="utf-8")
        assert "sk-test-xxx" in content
