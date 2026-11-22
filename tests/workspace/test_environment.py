import os
import shutil
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
import tomlkit

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    CONFIG_TOML,
    SECRETS_TOML,
    CustomLoaderDocProvider,
)
from dlt.common.configuration.specs import config_providers_context
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.utils import clone_dict_nested, custom_environ

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext, switch_context
from dlt._workspace.environment import (
    activate_environment,
    activate_environment_profile,
    add_environments,
    connect_remote_workspace,
    create_environment_definition,
    declare_environments,
    disconnect_remote_workspace,
    get_deployment_module_for_env,
    get_profiles_for_env,
    select_environment,
    select_environment_profile,
    set_global_secret,
    use_environment,
    use_environment_profile,
)
from dlt._workspace.exceptions import (
    EnvironmentAlreadyExists,
    EnvironmentNotDeclared,
    InvalidProfileName,
    NoEnvironment,
    OrganizationMismatch,
    ProfileAlreadyExists,
    ProfileNotDeclared,
    ProfileNotInEnvironment,
    WorkspaceIdConflict,
)
from dlt._workspace.profile import activate_profile, use_profile
from dlt._workspace.settings_files import load_yaml_file
from dlt._workspace.typing import TEnvironmentDefinition
from dlt._workspace.workspace_definition import (
    create_built_in_workspace_definition,
    get_default_profile_for_env,
    get_environment_definition,
    get_profile_definition,
)
from dlt._workspace.workspace_file import (
    WORKSPACE_FILE_NAME,
    build_stored_workspace_definition,
    load_workspace_definition,
)

from tests.workspace.utils import (
    append_settings,
    declare_environment,
    full_environment,
    isolated_workspace,
    save_definition,
)

SHARED_LEGACY = '\n[runtime]\nworkspace_id = "shared-id"\n[workspace.settings]\nname = "ingest"\n'
"""The id and the name of the remote workspace as `config.toml` held them before environments."""
OWN_LEGACY = '[runtime]\nworkspace_id = "own-id"\n[workspace.settings]\nname = "ingest-own"\n'
"""The same values in the settings file of one profile."""
PROD_SECTION = '[workspace.environment.prod]\nworkspace_id = "section-id"\nname = "ingest-prd"\n'


def _as_declared(environment: TEnvironmentDefinition) -> TEnvironmentDefinition:
    """`environment` of a loaded workspace without `configured`, which the load computes."""
    declared: TEnvironmentDefinition = clone_dict_nested(environment)  # type: ignore[type-var]
    for profile in declared["profiles"].values():
        del profile["configured"]
    return declared


def _settings(ctx: WorkspaceRunContext, file_name: str) -> Dict[str, Any]:
    with open(os.path.join(ctx.settings_dir, file_name), "r", encoding="utf-8") as f:
        return tomlkit.parse(f.read()).unwrap()


def _settings_bytes(ctx: WorkspaceRunContext) -> Dict[str, bytes]:
    """Content of every file in the settings dir, to tell if anything was written."""
    files: Dict[str, bytes] = {}
    for name in os.listdir(ctx.settings_dir):
        path = os.path.join(ctx.settings_dir, name)
        if os.path.isfile(path):
            with open(path, "rb") as f:
                files[name] = f.read()
    return files


def _connections(ctx: WorkspaceRunContext, file_name: str) -> Dict[str, Any]:
    return _settings(ctx, file_name).get("workspace", {}).get("environment", {})


def test_get_default_profile_and_profiles_for_env() -> None:
    # the default write profile is declared last, after another profile with write data access
    environment: TEnvironmentDefinition = {
        "name": "x",
        "profiles": {
            "x-read": {"location": "remote", "data_access": "read", "env": "x", "default": True},
            "x-old": {"location": "remote", "data_access": "write", "env": "x"},
            "x-write": {"location": "remote", "data_access": "write", "env": "x", "default": True},
        },
    }
    assert get_default_profile_for_env(environment, "write") == "x-write"
    assert get_default_profile_for_env(environment, "read") == "x-read"
    assert get_default_profile_for_env(environment, "none") is None
    # deployed together: the default write profile first, the others in declaration order
    assert get_profiles_for_env(environment) == ["x-write", "x-read", "x-old"]


@pytest.mark.parametrize(
    "lookup,name",
    [(get_profile_definition, "ci"), (get_environment_definition, "ci-write")],
    ids=["profile-is-environment", "environment-is-profile"],
)
def test_lookup_of_undeclared_name(lookup: Callable[..., Any], name: str) -> None:
    """A lookup finds declared names only: no profile by the name of its environment, no
    environment by the name of its default write profile."""
    doc = create_built_in_workspace_definition()
    doc["environments"]["ci"] = full_environment("ci")
    with pytest.raises(KeyError):
        lookup(doc, name)


def test_add_environments() -> None:
    with isolated_workspace("empty") as ctx:
        path = ctx.get_setting(WORKSPACE_FILE_NAME)
        # declared into a loaded definition: neither the file nor the context changed
        environment = create_environment_definition("ci")
        doc = load_workspace_definition(ctx)
        declare_environments(doc, {"ci": environment}, path)
        assert doc["environments"]["ci"] == environment
        assert "ci" not in load_yaml_file(path)["environments"]
        assert "ci" not in ctx.environment_definitions
        # added with its profiles, the built-in `prod` stays the default
        declare_environment(ctx, "ci")
        assert _as_declared(ctx.environment_definitions["ci"]) == full_environment("ci")
        assert list(ctx.environment_definitions) == ["prod", "ci"]
        assert ctx.default_environment == "prod"
        # the file holds the new declarations
        assert load_yaml_file(path) == build_stored_workspace_definition(
            ctx.workspace_definition, path
        )

        # refused: a declared environment, a declared profile, an invalid name
        with pytest.raises(EnvironmentAlreadyExists) as exc:
            declare_environment(ctx, "ci")
        assert os.path.join(".dlt", WORKSPACE_FILE_NAME) in str(exc.value)
        with pytest.raises(ProfileAlreadyExists):
            add_environments(
                ctx,
                {
                    "qa": {
                        **create_environment_definition("qa"),
                        "profiles": {
                            "dev": {
                                "location": "remote",
                                "data_access": "write",
                                "env": "qa",
                                "default": True,
                            }
                        },
                    }
                },
            )
        with pytest.raises(InvalidProfileName):
            add_environments(ctx, {"a b": create_environment_definition("a b")})

        # the first environment becomes the default
        doc = create_built_in_workspace_definition()
        doc["environments"] = {}
        save_definition(ctx, doc)
        declare_environment(ctx, "ci")
        assert ctx.default_environment == "ci"


@pytest.mark.parametrize(
    "name,profile,expected",
    [
        (None, "dev", "prod"),
        (None, "tests", "prod"),
        (None, "access", "prod"),
        (None, "ci-read", "ci"),
        ("prod", "dev", "prod"),
        ("ci", "dev", "ci"),
        ("prod", "ci-read", "prod"),
    ],
    ids=[
        "default-profile",
        "local-profile",
        "profile-of-default-environment",
        "profile-of-environment",
        "named-default-environment",
        "named-environment",
        "named-over-active-profile",
    ],
)
def test_select_environment(name: Optional[str], profile: str, expected: str) -> None:
    """The named environment, else the environment of the active profile, else the default one."""
    environments = {
        "prod": create_built_in_workspace_definition()["environments"]["prod"],
        "ci": full_environment("ci"),
    }
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        ctx = ctx.switch_profile(profile)
        selected = select_environment(name)
        # with the deployment module from the configuration, the declared environment has none
        assert _as_declared(selected) == {
            **environments[expected],
            "deployment_module": "__deployment__",
        }
        assert "deployment_module" not in ctx.get_environment_definition(expected)


@pytest.mark.parametrize(
    "name,profile,remedy",
    [
        ("ci-write", "ci-write", "`ci-write` is a profile of environment `ci`. Name `ci`, or"),
        ("access", "dev", "`access` is a profile of environment `prod`. Name `prod`, or"),
        ("dev", "dev", "`dev` is a local profile, not an environment."),
        ("nope", "dev", "Add it with `dlthub profile add-environment nope`."),
    ],
    ids=["default-write-profile-name", "profile-name", "local-profile-name", "undeclared-name"],
)
def test_select_environment_not_declared(name: str, profile: str, remedy: str) -> None:
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        ctx = ctx.switch_profile(profile)
        with pytest.raises(EnvironmentNotDeclared) as exc:
            select_environment(name)
        assert (exc.value.env, exc.value.environments) == (name, ["prod", "ci"])
        path = os.path.join(".dlt", WORKSPACE_FILE_NAME)
        message = str(exc.value)
        assert message.startswith(
            f"`{name}` is not an environment declared in `{path}` (declared: prod, ci)."
        )
        # a profile name gets no remedy that declares it as an environment
        assert remedy in message


def test_select_environment_without_default() -> None:
    with isolated_workspace("default") as ctx:
        # `prod` is declared and is not the default: name one
        doc = create_built_in_workspace_definition()
        del doc["environments"]["prod"]["default"]
        save_definition(ctx, doc)
        with pytest.raises(NoEnvironment) as exc:
            select_environment()
        message = str(exc.value)
        assert "The active profile `dev` is a local profile" in message
        assert "--environment`: prod" in message
        assert "make one of them the default environment" in message
        # no environment is declared
        doc["environments"] = {}
        save_definition(ctx, doc)
        with pytest.raises(NoEnvironment, match="No environment is declared"):
            select_environment()


@pytest.mark.parametrize("environment", ["b", None], ids=["remote-profile", "local-profile"])
def test_profile_named_like_environment(environment: Optional[str]) -> None:
    """A profile can take the name of an environment without being its default write profile."""
    doc = create_built_in_workspace_definition()
    # deployed with `b-write`
    doc["environments"]["b"] = create_environment_definition("b")
    if environment:
        doc["environments"]["b"]["profiles"]["b"] = {
            "location": "remote",
            "data_access": "read",
            "env": "b",
            "default": True,
        }
    else:
        doc["local_profiles"]["b"] = {"location": "local", "data_access": "write"}
    with isolated_workspace("empty") as ctx:
        save_definition(ctx, doc)
        assert ctx.get_profile_definition("b").get("env") == environment
        # the name is a profile where a profile is named and an environment where one is
        ctx = ctx.switch_profile("b")
        assert (ctx.profile, ctx.environment) == ("b", environment)
        assert get_default_profile_for_env(select_environment("b"), "write") == "b-write"


def test_activate_environment() -> None:
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        # the environment is returned and its default write profile is the active profile
        assert _as_declared(activate_environment("ci")) == {
            **full_environment("ci"),
            "deployment_module": "__deployment__",
        }
        ctx = dlt.current.workspace()
        assert (ctx.profile, ctx.environment) == ("ci-write", "ci")
        assert os.environ[WORKSPACE__PROFILE] == "ci-write"
        # another profile of the environment is active: its default write profile takes over
        activate_profile("ci-read")
        assert activate_environment()["name"] == "ci"
        assert dlt.current.workspace().profile == "ci-write"
        # a refused name leaves the active profile
        with pytest.raises(EnvironmentNotDeclared):
            activate_environment("ci-read")
        assert dlt.current.workspace().profile == "ci-write"
        assert os.environ[WORKSPACE__PROFILE] == "ci-write"


@pytest.mark.parametrize(
    "environment,profile,active,expected",
    [
        (None, None, "dev", ("prod", "prod")),
        ("ci", None, "dev", ("ci", "ci-write")),
        (None, None, "ci-read", ("ci", "ci-read")),
        ("prod", None, "ci-read", ("prod", "prod")),
        (None, "ci-read", "dev", ("ci", "ci-read")),
        ("ci", "ci-read", "access", ("ci", "ci-read")),
    ],
    ids=[
        "local-active-default-environment",
        "local-active-named-environment",
        "active-profile-of-environment-kept",
        "active-profile-of-other-environment",
        "profile-names-its-environment",
        "profile-of-named-environment",
    ],
)
def test_select_environment_profile(
    environment: Optional[str], profile: Optional[str], active: str, expected: Tuple[str, str]
) -> None:
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        ctx.switch_profile(active)
        definition, selected = select_environment_profile(environment, profile)
        assert (definition["name"], selected) == expected
        # nothing is activated
        assert dlt.current.workspace().profile == active


@pytest.mark.parametrize(
    "environment,profile,message",
    [
        (
            "ci",
            "access",
            (
                "`access` is a profile of environment `prod`, not of `ci`. Name a profile of `ci`:"
                " ci-write, ci-read, ci-none."
            ),
        ),
        (
            None,
            "dev",
            (
                "`dev` is a local profile and belongs to no environment. Name a profile of an"
                " environment. `dlthub profile list` shows them."
            ),
        ),
        (
            "prod",
            "dev",
            (
                "`dev` is a local profile and belongs to no environment. Name a profile of"
                " `prod`: prod, access, no-access."
            ),
        ),
    ],
    ids=["profile-of-other-environment", "local-profile", "local-profile-named-environment"],
)
def test_select_environment_profile_not_in_environment(
    environment: Optional[str], profile: str, message: str
) -> None:
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        with pytest.raises(ProfileNotInEnvironment) as exc:
            select_environment_profile(environment, profile)
        assert str(exc.value) == message
        # undeclared names are refused as everywhere else
        with pytest.raises(ProfileNotDeclared):
            select_environment_profile(environment, "nope")
        with pytest.raises(EnvironmentNotDeclared):
            select_environment_profile("nope", profile)


@pytest.mark.parametrize("raises", [False, True], ids=["block-returns", "block-raises"])
def test_use_environment_restores_the_context(raises: bool) -> None:
    """`use_profile`, `use_environment` and `use_environment_profile` run the block with the
    profile and give the active profile and `WORKSPACE__PROFILE` back afterwards."""
    with isolated_workspace("default") as ctx, custom_environ({}):
        os.environ.pop(WORKSPACE__PROFILE, None)
        declare_environment(ctx, "ci")
        for use, expected in (
            (lambda: use_profile("access"), "access"),
            (lambda: use_environment("ci"), "ci-write"),
            (lambda: use_environment_profile(profile="ci-read"), "ci-read"),
        ):
            try:
                with use():
                    assert dlt.current.workspace().profile == expected
                    assert os.environ[WORKSPACE__PROFILE] == expected
                    if raises:
                        raise RuntimeError("block failed")
            except RuntimeError:
                assert raises
            assert dlt.current.workspace().profile == "dev"
            assert WORKSPACE__PROFILE not in os.environ
        # a refused name leaves the context untouched
        with pytest.raises(ProfileNotDeclared):
            with use_profile("nope"):
                pass
        assert dlt.current.workspace().profile == "dev"


def test_activate_environment_profile() -> None:
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        assert activate_environment_profile(profile="ci-read")["name"] == "ci"
        ctx = dlt.current.workspace()
        assert (ctx.profile, ctx.environment) == ("ci-read", "ci")
        assert os.environ[WORKSPACE__PROFILE] == "ci-read"


def test_get_deployment_module_for_env() -> None:
    """An environment names its deployment module, and whether it deploys the workspace dashboard,
    under `[workspace.environment.<name>]`. Without an environment, dlt takes the one that
    `select_environment` selects."""
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        append_settings(
            ctx,
            CONFIG_TOML,
            '[workspace.environment.prod]\ndeployment_module = "__prod__"\n'
            "deploy_dashboard = false\n",
        )
        with custom_environ({"WORKSPACE__ENVIRONMENT__CI__DEPLOYMENT_MODULE": "jobs/ci.py"}):
            ctx = ctx.switch_profile("dev")
            assert select_environment("ci")["deployment_module"] == "jobs/ci.py"
            # unset, the manifest decides from the strictness of the environment
            assert "deploy_dashboard" not in select_environment("ci")
            assert select_environment("prod")["deployment_module"] == "__prod__"
            assert select_environment("prod")["deploy_dashboard"] is False
            # a declared environment resolves it
            assert (
                get_deployment_module_for_env(ctx.get_environment_definition("ci")) == "jobs/ci.py"
            )
            # a local profile takes the default environment, a remote profile its own
            assert get_deployment_module_for_env() == "__prod__"
            ctx = ctx.switch_profile("ci-read")
            assert get_deployment_module_for_env() == "jobs/ci.py"
            assert ctx.config.deployment_module == "jobs/ci.py"
        # without an environment or its setting, the module is `__deployment__`
        ctx = ctx.switch_profile("dev")
        undeclared = create_environment_definition("qa")
        assert get_deployment_module_for_env(undeclared) == "__deployment__"
        assert ctx.config.deployment_module is None
        doc = create_built_in_workspace_definition()
        doc["environments"] = {}
        save_definition(ctx, doc)
        assert get_deployment_module_for_env() == "__deployment__"


@pytest.mark.parametrize(
    "files,environ,profile,workspace_id,name",
    [
        pytest.param(
            {CONFIG_TOML: '\n[workspace.environment.staging]\nworkspace_id = "section-id"\n'},
            {"RUNTIME__WORKSPACE_ID": "env-id"},
            "staging-write",
            "env-id",
            "default-staging",
            id="env-var-beats-section",
        ),
        pytest.param(
            {CONFIG_TOML: '\n[workspace.settings]\nname = "ingest"\n' + PROD_SECTION},
            {},
            "prod",
            "section-id",
            "ingest-prd",
            id="section-of-environment",
        ),
        pytest.param(
            {CONFIG_TOML: SHARED_LEGACY, "staging-write.config.toml": OWN_LEGACY},
            {},
            "staging-write",
            "own-id",
            "ingest-own",
            id="own-file-legacy-in-other-environment",
        ),
        pytest.param(
            {CONFIG_TOML: SHARED_LEGACY},
            {},
            "prod",
            "shared-id",
            "ingest",
            id="shared-legacy-in-default-environment",
        ),
        pytest.param(
            {CONFIG_TOML: SHARED_LEGACY},
            {},
            "access",
            "shared-id",
            "ingest",
            id="read-profile-of-default-environment",
        ),
        # the shared legacy id belongs to the default environment
        pytest.param(
            {CONFIG_TOML: SHARED_LEGACY},
            {},
            "staging-write",
            None,
            "ingest-staging",
            id="shared-legacy-not-in-other-environment",
        ),
        pytest.param(
            {CONFIG_TOML: SHARED_LEGACY},
            {
                "WORKSPACE__ENVIRONMENT__STAGING__WORKSPACE_ID": "stg-id",
                "WORKSPACE__ENVIRONMENT__STAGING__NAME": "ingest-stg",
            },
            "staging-write",
            "stg-id",
            "ingest-stg",
            id="section-from-env-vars",
        ),
        pytest.param(
            {
                CONFIG_TOML: (
                    SHARED_LEGACY + '[workspace.environment.staging]\nworkspace_id = "stg-id"\n'
                ),
                "staging-write.config.toml": OWN_LEGACY,
            },
            {},
            "staging-write",
            "stg-id",
            "ingest-own",
            id="section-beats-own-file-legacy-in-other-environment",
        ),
        pytest.param(
            {
                CONFIG_TOML: SHARED_LEGACY,
                "staging-write.config.toml": '[workspace.settings]\nname = "ingest-own"\n',
            },
            {},
            "staging-write",
            None,
            "ingest-own",
            id="own-file-name-kept",
        ),
        pytest.param(
            {CONFIG_TOML: SHARED_LEGACY},
            {"WORKSPACE__SETTINGS__NAME": "ingest-ci"},
            "staging-write",
            None,
            "ingest-ci",
            id="env-var-name-kept",
        ),
        pytest.param(
            {CONFIG_TOML: SHARED_LEGACY + PROD_SECTION},
            {},
            "dev",
            "shared-id",
            "ingest",
            id="local-profile-untouched",
        ),
        pytest.param({}, {}, "staging-write", None, "default-staging", id="not-connected"),
    ],
)
def test_workspace_id_and_name_precedence(
    files: Dict[str, str],
    environ: Dict[str, str],
    profile: str,
    workspace_id: Optional[str],
    name: str,
) -> None:
    """Where the active profile takes the id and the name of the remote workspace of its
    environment from. `prod` is the default environment, `staging` is another one."""
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "staging")
        for file_name, content in files.items():
            append_settings(ctx, file_name, content)
        with custom_environ(environ):
            ctx = switch_context(ctx.run_dir, profile=profile)
            assert ctx.runtime_config.workspace_id == workspace_id
            assert ctx.name == name
            assert ctx.config.settings.name == name


@pytest.mark.parametrize(
    "files,environ,profile,location",
    [
        ({CONFIG_TOML: SHARED_LEGACY + PROD_SECTION}, {}, "prod", CONFIG_TOML),
        (
            {CONFIG_TOML: "\n" + PROD_SECTION, "prod.config.toml": OWN_LEGACY},
            {},
            "prod",
            "prod.config.toml",
        ),
        (
            {CONFIG_TOML: SHARED_LEGACY},
            {"WORKSPACE__ENVIRONMENT__PROD__WORKSPACE_ID": "prod-id"},
            "access",
            CONFIG_TOML,
        ),
        (
            {
                CONFIG_TOML: PROD_SECTION,
                SECRETS_TOML: '\n[runtime]\nworkspace_id = "section-id"\n',
            },
            {},
            "prod",
            SECRETS_TOML,
        ),
    ],
    ids=[
        "shared-legacy-and-section",
        "own-file-legacy-and-section",
        "section-from-env-vars",
        "equal-ids-in-secrets",
    ],
)
def test_runtime_workspace_id_conflicts_with_default_environment(
    files: Dict[str, str], environ: Dict[str, str], profile: str, location: str
) -> None:
    """`workspace_id` under `[runtime]` in a settings file is the remote workspace of the default
    environment, set before environments existed: its section cannot set another one, not even
    an equal one. The message names the file to fix. The variable that the platform and CI set
    always wins."""
    with isolated_workspace("default") as ctx:
        for file_name, content in files.items():
            append_settings(ctx, file_name, content)
        with custom_environ(environ), pytest.raises(WorkspaceIdConflict) as exc:
            switch_context(ctx.run_dir, profile=profile)
        assert (exc.value.environment, exc.value.location) == ("prod", location)
        message = str(exc.value)
        assert "`[workspace.environment.prod]`" in message
        assert message.endswith(
            f"Remove `workspace_id` under `[runtime]` in `{os.path.join('.dlt', location)}`."
        )
        with custom_environ({**environ, "RUNTIME__WORKSPACE_ID": "run-id"}):
            ctx = switch_context(ctx.run_dir, profile=profile)
            assert ctx.runtime_config.workspace_id == "run-id"


def test_get_environment_config() -> None:
    """The remote workspace of any environment resolves without a change of the active profile."""
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "staging")
        append_settings(
            ctx,
            "prod.config.toml",
            '[workspace.settings]\nname = "ingest-prd"\n[runtime]\nworkspace_id = "prod-id"\n',
        )
        append_settings(
            ctx,
            "staging-write.config.toml",
            '[workspace.environment.staging]\nworkspace_id = "stg-id"\n',
        )
        ctx = switch_context(ctx.run_dir, profile="dev")
        # dev does not see the profile files of the default write profiles
        assert ctx.name == "default"
        assert ctx.runtime_config.workspace_id is None
        prod = ctx.get_environment_config("prod")
        assert prod.settings.name == "ingest-prd"
        assert prod.runtime.workspace_id == "prod-id"
        staging = ctx.get_environment_config("staging")
        assert staging.settings.name == "default-staging"
        assert staging.runtime.workspace_id == "stg-id"
        # the active context is untouched
        assert dlt.current.workspace().profile == "dev"
        assert ctx.name == "default"
        assert ctx.runtime_config.workspace_id is None
        # the environment of the active profile resolves as the context does
        ctx = switch_context(ctx.run_dir, profile="prod")
        prod = ctx.get_environment_config("prod")
        assert prod.settings.name == ctx.name == "ingest-prd"
        assert prod.runtime.workspace_id == ctx.runtime_config.workspace_id == "prod-id"


def test_get_environment_config_with_extra_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Another environment resolves with the extra providers too, such as a vault. A value that such
    a provider serves to every profile belongs to the default environment. Meanwhile the other
    threads keep the providers of the active profile."""
    seen: List[Tuple[ConfigProvidersContainer, ConfigProvidersContainer]] = []

    def _read_in_thread() -> ConfigProvidersContainer:
        read: List[ConfigProvidersContainer] = []
        thread = threading.Thread(
            target=lambda: read.append(Container()[PluggableRunContext].providers)
        )
        thread.start()
        thread.join()
        return read[0]

    def _load_vault() -> Dict[str, Any]:
        seen.append((Container()[PluggableRunContext].providers, _read_in_thread()))
        return {
            "workspace": {"environment": {"staging": {"workspace_id": "vault-stg"}}},
            "runtime": {"workspace_id": "vault-shared"},
        }

    monkeypatch.setattr(
        config_providers_context,
        "_extra_providers",
        lambda: [CustomLoaderDocProvider("vault", _load_vault)],
    )
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "staging")
        declare_environment(ctx, "qa")
        ctx = switch_context(ctx.run_dir, profile="dev")
        active_providers = Container()[PluggableRunContext].providers
        seen.clear()

        assert ctx.get_environment_config("staging").runtime.workspace_id == "vault-stg"
        assert ctx.get_environment_config("prod").runtime.workspace_id == "vault-shared"
        assert ctx.get_environment_config("qa").runtime.workspace_id is None
        # the providers of the other profile served the current thread only
        assert len(seen) == 3
        for current_thread, other_thread in seen:
            assert current_thread is not active_providers
            assert other_thread is active_providers
        assert Container()[PluggableRunContext].providers is active_providers


def test_connect_remote_workspace() -> None:
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "staging")
        connect_remote_workspace(
            ctx,
            "staging",
            workspace_id="stg-id",
            workspace_name="ingest-stg",
            organization_id="org-id",
        )
        assert _connections(ctx, CONFIG_TOML) == {
            "staging": {"workspace_id": "stg-id", "name": "ingest-stg"}
        }
        assert _settings(ctx, CONFIG_TOML)["runtime"]["organization_id"] == "org-id"
        # the active context resolves the connection right away
        assert ctx.get_environment_config("staging").runtime.workspace_id == "stg-id"
        ctx = switch_context(ctx.run_dir, profile="staging-write")
        assert ctx.name == "ingest-stg"
        assert ctx.runtime_config.workspace_id == "stg-id"
        assert ctx.runtime_config.organization_id == "org-id"
        # the other environment is not affected
        assert ctx.get_environment_config("prod").runtime.workspace_id is None
        # values not given are kept, the same organization is accepted
        connect_remote_workspace(ctx, "staging", workspace_id="stg-id-2", organization_id="org-id")
        ctx = switch_context(ctx.run_dir, profile="staging-write")
        assert ctx.name == "ingest-stg"
        assert ctx.runtime_config.workspace_id == "stg-id-2"
        # one organization per workspace
        with pytest.raises(OrganizationMismatch) as exc:
            connect_remote_workspace(ctx, "prod", workspace_id="prod-id", organization_id="org-2")
        assert exc.value.connected == "org-id"
        assert "[runtime]" in str(exc.value)
        assert ctx.get_environment_config("prod").runtime.workspace_id is None
        # disconnect removes the id, the other settings of the environment stay
        disconnect_remote_workspace(ctx, "staging")
        assert _connections(ctx, CONFIG_TOML) == {"staging": {"name": "ingest-stg"}}
        assert _settings(ctx, CONFIG_TOML)["runtime"]["organization_id"] == "org-id"
        assert ctx.runtime_config.workspace_id is None
        assert ctx.name == "ingest-stg"
        # disconnecting again is a no-op
        before = _settings_bytes(ctx)
        disconnect_remote_workspace(ctx, "staging")
        assert _settings_bytes(ctx) == before
        # a section left without settings goes away
        connect_remote_workspace(ctx, "prod", workspace_id="prod-id")
        disconnect_remote_workspace(ctx, "prod")
        assert _connections(ctx, CONFIG_TOML) == {"staging": {"name": "ingest-stg"}}


def test_set_global_secret() -> None:
    """The user's secrets go to the global `secrets.toml`, which is created when missing, and the
    active context resolves them right away. The workspace settings stay as they are."""
    with isolated_workspace("default") as ctx:
        shutil.rmtree(ctx.global_dir)
        before = _settings_bytes(ctx)
        set_global_secret(ctx, "auth_token", "token-1", "runtime")
        set_global_secret(ctx, "refresh_token", "refresh-1", "runtime")
        assert ctx.runtime_config.auth_token == "token-1"
        # a new value replaces the old one and keeps the other keys
        set_global_secret(ctx, "auth_token", "token-2", "runtime")
        with open(os.path.join(ctx.global_dir, SECRETS_TOML), "r", encoding="utf-8") as f:
            assert tomlkit.parse(f.read()).unwrap() == {
                "runtime": {"auth_token": "token-2", "refresh_token": "refresh-1"}
            }
        assert ctx.runtime_config.auth_token == "token-2"
        assert _settings_bytes(ctx) == before

        # without a reload the file changes and the resolved config stays until reloaded
        set_global_secret(ctx, "auth_token", "token-3", "runtime", reload_context=False)
        with open(os.path.join(ctx.global_dir, SECRETS_TOML), "r", encoding="utf-8") as f:
            assert tomlkit.parse(f.read()).unwrap()["runtime"]["auth_token"] == "token-3"
        assert ctx.runtime_config.auth_token == "token-2"
        Container()[PluggableRunContext].reload_providers()
        assert ctx.runtime_config.auth_token == "token-3"

        # a dict merges into the section: the keys it does not name stay
        set_global_secret(ctx, "runtime", {"auth_token": "token-4", "api_key": "key-4"})
        with open(os.path.join(ctx.global_dir, SECRETS_TOML), "r", encoding="utf-8") as f:
            assert tomlkit.parse(f.read()).unwrap() == {
                "runtime": {
                    "auth_token": "token-4",
                    "refresh_token": "refresh-1",
                    "api_key": "key-4",
                }
            }
        assert ctx.runtime_config.auth_token == "token-4"


def test_connect_with_legacy_settings() -> None:
    """Connect moves the legacy `[runtime] workspace_id` of any environment to its section."""
    # `prod.config.toml` and `stg.config.toml` each hold the remote workspace of one environment
    with isolated_workspace("cicd_staging") as ctx:
        stg_settings = {
            "runtime": {"organization_id": "org-id"},
            "workspace": {"settings": {"name": "dlt-de-stage"}},
        }
        connect_remote_workspace(
            ctx,
            "stg",
            workspace_id="stg-ws-id",
            workspace_name="dlt-de-stage",
            organization_id="org-id",
        )
        assert _settings(ctx, CONFIG_TOML) == {
            "workspace": {"environment": {"stg": {"workspace_id": "stg-ws-id"}}}
        }
        assert _settings(ctx, "stg.config.toml") == stg_settings
        # the default environment keeps its legacy id until it connects
        assert _settings(ctx, "prod.config.toml")["runtime"]["workspace_id"] == "prod-ws-id"
        assert ctx.get_environment_config("stg").runtime.workspace_id == "stg-ws-id"
        # the name and the organization resolve from `stg.config.toml`: nothing is written again
        before = _settings_bytes(ctx)
        connect_remote_workspace(
            ctx,
            "stg",
            workspace_id="stg-ws-id",
            workspace_name="dlt-de-stage",
            organization_id="org-id",
        )
        assert _settings_bytes(ctx) == before
        # the organization lives in `stg.config.toml`
        with pytest.raises(OrganizationMismatch) as exc:
            connect_remote_workspace(ctx, "stg", workspace_id="stg-ws-id-2", organization_id="o-2")
        assert exc.value.connected == "org-id"
        assert exc.value.path == ".dlt"
        assert "[runtime]" in str(exc.value)
        # an undeclared environment is refused
        with pytest.raises(EnvironmentNotDeclared) as not_declared:
            connect_remote_workspace(ctx, "qa", workspace_id="qa-ws-id", organization_id="org-id")
        assert not_declared.value.environments == ["prod", "stg"]
        with pytest.raises(EnvironmentNotDeclared):
            disconnect_remote_workspace(ctx, "qa")
        assert _settings_bytes(ctx) == before

        # the default environment moves its id to its section, `stg` stays as it is
        connect_remote_workspace(
            ctx,
            "prod",
            workspace_id="prod-ws-id",
            workspace_name="dlt-de-prd",
            organization_id="org-id",
        )
        assert _connections(ctx, CONFIG_TOML) == {
            "stg": {"workspace_id": "stg-ws-id"},
            "prod": {"workspace_id": "prod-ws-id"},
        }
        assert "workspace_id" not in _settings(ctx, "prod.config.toml")["runtime"]
        assert _settings(ctx, "stg.config.toml") == stg_settings
        assert ctx.get_environment_config("prod").runtime.workspace_id == "prod-ws-id"
        assert ctx.get_environment_config("prod").settings.name == "dlt-de-prd"

        # a legacy id in `config.toml` belongs to the default environment: `stg` leaves it
        append_settings(ctx, CONFIG_TOML, '[runtime]\nworkspace_id = "old-id"\n')
        connect_remote_workspace(ctx, "stg", workspace_id="stg-ws-id-2", organization_id="org-id")
        assert _connections(ctx, CONFIG_TOML)["stg"] == {"workspace_id": "stg-ws-id-2"}
        assert _settings(ctx, CONFIG_TOML)["runtime"]["workspace_id"] == "old-id"
        ctx = switch_context(ctx.run_dir, profile="stg")
        assert ctx.runtime_config.workspace_id == "stg-ws-id-2"
        assert ctx.name == "dlt-de-stage"

        # next to the section of the default environment the legacy id is a conflict that
        # connect resolves
        with pytest.raises(WorkspaceIdConflict):
            ctx.get_environment_config("prod")
        connect_remote_workspace(ctx, "prod", workspace_id="new-id")
        assert "workspace_id" not in _settings(ctx, CONFIG_TOML)["runtime"]
        assert ctx.get_environment_config("prod").runtime.workspace_id == "new-id"


@pytest.mark.parametrize(
    "file_name", [CONFIG_TOML, SECRETS_TOML, "prod.secrets.toml"], ids=lambda name: name
)
def test_connect_and_disconnect_never_leave_two_workspace_ids(file_name: str) -> None:
    """A legacy `[runtime] workspace_id` in any settings file of the default environment moves to
    its section on connect, also to another remote workspace, and goes away on disconnect. The
    other keys of the file stay."""
    with isolated_workspace("default") as ctx:
        legacy = '[runtime]\nworkspace_id = "%s"\norganization_id = "org-id"\n'
        append_settings(ctx, file_name, "\n" + legacy % "old-id")
        ctx = switch_context(ctx.run_dir, profile="dev")
        assert ctx.get_environment_config("prod").runtime.workspace_id == "old-id"

        connect_remote_workspace(ctx, "prod", workspace_id="new-id")
        assert _settings(ctx, file_name)["runtime"] == {"organization_id": "org-id"}
        assert _connections(ctx, CONFIG_TOML)["prod"] == {"workspace_id": "new-id"}
        assert ctx.get_environment_config("prod").runtime.workspace_id == "new-id"

        # an older dlthub client writes the legacy id again
        with open(os.path.join(ctx.settings_dir, file_name), "w", encoding="utf-8") as f:
            f.write(legacy % "new-id")
        disconnect_remote_workspace(ctx, "prod")
        assert _settings(ctx, file_name)["runtime"] == {"organization_id": "org-id"}
        assert "prod" not in _connections(ctx, CONFIG_TOML)
        assert ctx.get_environment_config("prod").runtime.workspace_id is None


@pytest.mark.parametrize(
    "environment,name,shared_id,other_environment,other_id",
    [
        ("prod", "dlt-de-prd", None, "stg", "stg-ws-id"),
        # the legacy id in `config.toml` belongs to the default environment and stays
        ("stg", "dlt-de-stage", "shared-id", "prod", "prod-ws-id"),
    ],
    ids=["default-environment", "other-environment"],
)
def test_disconnect_removes_legacy_workspace_id(
    environment: str,
    name: str,
    shared_id: Optional[str],
    other_environment: str,
    other_id: str,
) -> None:
    """Disconnect removes the legacy `[runtime] workspace_id` from the settings file of the
    default write profile, and from `config.toml` for the default environment only."""
    with isolated_workspace("cicd_staging") as ctx:
        append_settings(ctx, CONFIG_TOML, '[runtime]\nworkspace_id = "shared-id"\n')
        disconnect_remote_workspace(ctx, environment)
        assert _settings(ctx, CONFIG_TOML)["runtime"].get("workspace_id") == shared_id
        # the organization and the name stay
        assert _settings(ctx, f"{environment}.config.toml") == {
            "runtime": {"organization_id": "org-id"},
            "workspace": {"settings": {"name": name}},
        }
        ctx = switch_context(ctx.run_dir, profile=environment)
        assert ctx.runtime_config.workspace_id is None
        assert ctx.runtime_config.organization_id == "org-id"
        assert ctx.name == name
        # in the other environment the local workspace stays connected
        assert ctx.get_environment_config(other_environment).runtime.workspace_id == other_id
        # disconnecting again is a no-op
        after = _settings_bytes(ctx)
        disconnect_remote_workspace(ctx, environment)
        assert _settings_bytes(ctx) == after
