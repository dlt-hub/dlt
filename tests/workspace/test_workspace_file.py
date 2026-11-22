import os
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

import pytest
from pytest_mock import MockerFixture

from dlt.common.typing import DictStrAny
from dlt.common.utils import custom_environ

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext, switch_context
from dlt._workspace.exceptions import InvalidWorkspaceFile, WorkspaceFileEngineNoUpgradePath
from dlt._workspace.profile import add_profiles
from dlt._workspace.settings_files import dump_yaml, load_yaml_file, save_profile_pin
from dlt._workspace.typing import TWorkspaceDefinition
from dlt._workspace.workspace_definition import (
    create_built_in_workspace_definition,
    is_profile_usable,
)
from dlt._workspace.workspace_file import (
    WORKSPACE_FILE_NAME,
    expand_workspace_definition,
    get_workspace_file,
    migrate_workspace_definition,
    save_workspace_file,
    build_stored_workspace_definition,
    validate_workspace_definition,
)

from tests.workspace.utils import (
    empty_workspace_file,
    full_environment,
    isolated_workspace,
    touch_settings,
)

DEFINITION_PATH = os.path.join(".dlt", WORKSPACE_FILE_NAME)


def _remote(env: str, data_access: str = "write", default: bool = True, **fields: Any) -> Any:
    profile = {"location": "remote", "data_access": data_access, "env": env, **fields}
    if default:
        profile["default"] = True
    return profile


def _environment(name: str, profiles: DictStrAny, **fields: Any) -> Any:
    return {"name": name, "profiles": profiles, **fields}


def _make_stored_profile(data_access: str = "write", default: bool = False, **fields: Any) -> Any:
    """A profile as `.dlt/.workspace` stores it."""
    profile = {"data_access": data_access, **fields}
    if default:
        profile["default"] = True
    return profile


def _make_stored_environment(profiles: DictStrAny, **fields: Any) -> Any:
    return {"profiles": profiles, **fields}


def _make_single_environment(**fields: Any) -> Any:
    """Environment `b` with its default write profile `b`, and `fields`."""
    return {"b": _make_stored_environment({"b": _make_stored_profile(default=True)}, **fields)}


def _definition(
    environments: Optional[DictStrAny] = None,
    local_profiles: Optional[DictStrAny] = None,
    **fields: Any,
) -> Any:
    return {
        "engine_version": 1,
        "environments": environments or {},
        "local_profiles": local_profiles or {},
        **fields,
    }


def _doc() -> TWorkspaceDefinition:
    # `solo` is a custom profile as the migration declares it
    doc = expand_workspace_definition(
        migrate_workspace_definition({}, 0, 1, implicit_profiles=["solo"])
    )
    doc["environments"]["prod"]["profiles"]["analytics"] = _remote("prod", default=False)
    doc["environments"]["ci"] = full_environment("ci")
    return doc


def _write_workspace_file(ctx: WorkspaceRunContext, content: str) -> None:
    with open(ctx.get_setting(WORKSPACE_FILE_NAME), "w", encoding="utf-8") as f:
        f.write(content)
    ctx.reload_workspace_definition()


def _assert_invalid(ex: InvalidWorkspaceFile, path: str, errors: List[str]) -> None:
    assert len(ex.errors) == len(errors)
    for expected in errors:
        assert any(expected in error for error in ex.errors), expected
    # the message tells where the profiles are declared and how to fix the file, nothing more
    assert path in str(ex)
    assert str(ex).endswith(
        "Fix the entries above, or empty the file: dlt then declares the built-in profiles again."
    )


INVALID_DEFINITIONS: Dict[str, Tuple[DictStrAny, List[str]]] = {
    "unknown-node": (_definition(extra=1), ["unexpected fields"]),
    "unknown-environment-node": (
        _definition(_make_single_environment(head="b")),
        ["unexpected fields"],
    ),
    # keys of a layout that was never released get no hint
    "deployment-profile-key": (
        _definition(_make_single_environment(deployment_profile="b")),
        ["unexpected fields"],
    ),
    "strict-data-access-key": (
        _definition(_make_single_environment(strict_data_access=True)),
        ["unexpected fields"],
    ),
    # what follows from where an entry is declared, or comes from the configuration, is not stored
    "environment-deployment-module": (
        _definition(_make_single_environment(deployment_module="x")),
        [
            "unexpected fields",
            "set `deployment_module` under `[workspace.environment.b]` in `config.toml`",
        ],
    ),
    "profile-location": (
        _definition(local_profiles={"dev": _make_stored_profile(location="local")}),
        ["unexpected fields"],
    ),
    "bad-access": (
        _definition(local_profiles={"dev": _make_stored_profile("full")}),
        ["is not one of"],
    ),
    "no-access": (_definition(local_profiles={"dev": {}}), ["missing required fields"]),
    "bad-name": (
        _definition(local_profiles={"my.prof": _make_stored_profile()}),
        ["profile `my.prof`: a name has only letters, digits"],
    ),
    "bad-environment-name": (
        _definition(
            {"my.env": _make_stored_environment({"x": _make_stored_profile(default=True)})}
        ),
        ["environment `my.env`: a name has only letters, digits"],
    ),
    "local-read": (
        _definition(local_profiles={"dev": _make_stored_profile("read")}),
        ["a local profile has write data access"],
    ),
    "declared-twice": (
        _definition(
            {
                "b": _make_stored_environment(
                    {"b": _make_stored_profile(default=True), "x": _make_stored_profile()}
                ),
                "c": _make_stored_environment(
                    {"c": _make_stored_profile(default=True), "x": _make_stored_profile()}
                ),
            }
        ),
        ["profile `x`: declared in environment `c` and in environment `b`"],
    ),
    "no-profiles": (
        _definition(
            {
                "b": _make_stored_environment({}),
                "c": _make_stored_environment({"c": _make_stored_profile(default=True)}),
            }
        ),
        ["environment `b`: no profile with write data access"],
    ),
    "no-write-profile": (
        _definition(
            {"b": _make_stored_environment({"b": _make_stored_profile("read", default=True)})}
        ),
        ["environment `b`: no profile with write data access"],
    ),
    "no-default-of-data-access": (
        _definition(
            {
                "b": _make_stored_environment(
                    {"b": _make_stored_profile(default=True), "r": _make_stored_profile("read")}
                )
            }
        ),
        [
            "environment `b`: none of r is the default with read data access. Set `default: true`"
            " on one."
        ],
    ),
    "two-defaults-of-data-access": (
        _definition(
            {
                "b": _make_stored_environment(
                    {
                        "b": _make_stored_profile(default=True),
                        "x": _make_stored_profile(default=True),
                    }
                )
            }
        ),
        [
            "environment `b`: profiles b, x are all the default with write data access. Keep"
            " `default: true` on one."
        ],
    ),
    "two-default-profiles": (
        _definition(
            local_profiles={
                "a": _make_stored_profile(default=True),
                "b": _make_stored_profile(default=True),
            }
        ),
        ["profiles a, b are all the default local profile. Keep `default: true` on one."],
    ),
    "two-default-environments": (
        _definition(
            {
                "a": _make_stored_environment(
                    {"a": _make_stored_profile(default=True)}, default=True
                ),
                "b": _make_stored_environment(
                    {"b": _make_stored_profile(default=True)}, default=True
                ),
            }
        ),
        ["environments a, b are all the default. Keep `default: true` on one."],
    ),
    "every-rule-error": (
        _definition(
            {
                "b": _make_stored_environment(
                    {"b": _make_stored_profile(default=True)}, default=True
                ),
                "c": _make_stored_environment({"c": _make_stored_profile()}, default=True),
            },
            {"a": _make_stored_profile("read")},
        ),
        [
            "has write data access",
            "environment `c`: none of c is the default with write data access",
            "environments b, c are all the default",
        ],
    ),
}


@pytest.mark.parametrize("doc,errors", INVALID_DEFINITIONS.values(), ids=list(INVALID_DEFINITIONS))
def test_validate_workspace_definition(doc: DictStrAny, errors: List[str]) -> None:
    with pytest.raises(InvalidWorkspaceFile) as exc:
        validate_workspace_definition(doc, DEFINITION_PATH)
    _assert_invalid(exc.value, DEFINITION_PATH, errors)


@pytest.mark.parametrize(
    "content,errors",
    [
        ("engine_version: '1'\nenvironments: {}\nlocal_profiles: {}\n", ["must be an integer"]),
        ("- a\n- b\n", ["must contain a yaml mapping"]),
        ("engine_version: 1\nenvironments: {}\n", ["missing required fields"]),
        # only an empty file is migrated
        (
            "local_profiles:\n  lab: {location: local, data_access: write}\n",
            ["`engine_version` is missing, set `engine_version: 1`"],
        ),
    ],
    ids=[
        "engine-not-int",
        "not-a-mapping",
        "no-local-profiles",
        "content-without-engine-version",
    ],
)
def test_invalid_workspace_file(content: str, errors: List[str]) -> None:
    with isolated_workspace("empty") as ctx:
        path = ctx.get_setting(WORKSPACE_FILE_NAME)
        _write_workspace_file(ctx, content)
        with pytest.raises(InvalidWorkspaceFile) as exc:
            _ = ctx.profile_definitions
        # the message shows the file relative to the workspace, the working dir is in it
        _assert_invalid(exc.value, DEFINITION_PATH, errors)
        # an invalid file is left as it is
        with open(path, encoding="utf-8") as f:
            assert f.read() == content


def test_migrate_workspace_definition() -> None:
    # engine 0 gets the built-in declarations and each profile in use as an environment of its
    # own: once, and not for a built-in or an invalid name
    in_use = ["staging", "ci", "staging", "dev", "access", "bad.name", "my profile"]
    migrated = migrate_workspace_definition({}, 0, 1, implicit_profiles=in_use)
    expected = create_built_in_workspace_definition()
    for name in ("staging", "ci"):
        # deployed with the profile alone and, without `strict_access`, not strict
        expected["environments"][name] = _environment(name, {name: _remote(name)})
    # the migration gives the stored form
    assert migrated == build_stored_workspace_definition(expected, DEFINITION_PATH)
    assert list(migrated) == ["engine_version", "environments", "local_profiles"]
    assert list(migrated["environments"]) == ["prod", "staging", "ci"]
    # the same version changes nothing
    assert migrate_workspace_definition(dict(migrated), 1, 1) == migrated
    # the definition of a newer dlt is refused, also when a workspace loads it
    with pytest.raises(WorkspaceFileEngineNoUpgradePath, match="upgrade dlt"):
        migrate_workspace_definition(dict(migrated), 2, 1)
    with isolated_workspace("empty") as ctx:
        _write_workspace_file(ctx, "engine_version: 2\nenvironments: {}\nlocal_profiles: {}\n")
        with pytest.raises(WorkspaceFileEngineNoUpgradePath):
            _ = ctx.profile_definitions


class MigrationCase(NamedTuple):
    """A workspace with an empty `.dlt/.workspace` as dlt finds it on load, and what the load
    gives."""

    files: Sequence[str] = ()
    """Empty files in `.dlt`"""
    recorded: Optional[str] = None
    """Content of `.dlt/profile-name`"""
    environ: Optional[str] = None
    """`WORKSPACE__PROFILE`"""
    profile: Optional[str] = None
    """Profile the context is created with, as `--profile` and `switch_profile` do"""
    workspace: str = "empty"
    read_only: bool = False
    environments: Sequence[str] = ()
    """Environments expected next to `prod`, one for each profile the migration declares"""
    active: str = "dev"
    environment: Optional[str] = None


MIGRATION_CASES: Dict[str, MigrationCase] = {
    "no-settings-files": MigrationCase(),
    "built-in-profile-files": MigrationCase(
        workspace="default",
        files=("prod.config.toml", "access.secrets.toml", "tests.config.toml"),
    ),
    "two-custom-profiles": MigrationCase(
        files=("stg.secrets.toml", "analytics.config.toml", "analytics.secrets.toml"),
        environments=("analytics", "stg"),
    ),
    "recorded-custom-no-files": MigrationCase(
        recorded="staging", environments=("staging",), active="staging", environment="staging"
    ),
    "recorded-custom-with-files": MigrationCase(
        recorded="staging",
        files=("staging.config.toml", "staging.secrets.toml"),
        environments=("staging",),
        active="staging",
        environment="staging",
    ),
    "recorded-built-in": MigrationCase(recorded="prod", active="prod", environment="prod"),
    "not-profile-files": MigrationCase(
        workspace="default",
        files=(
            "my.profile.config.toml",
            "config.toml.bak",
            "analytics.config.toml.bak",
            "analytics.toml",
        ),
    ),
    "loaded-with-dev": MigrationCase(
        files=("analytics.config.toml",),
        recorded="staging",
        profile="dev",
        environments=("analytics", "staging"),
    ),
    "loaded-with-prod": MigrationCase(
        files=("analytics.config.toml",),
        recorded="staging",
        environ="prod",
        environments=("analytics", "staging"),
        active="prod",
        environment="prod",
    ),
    "environ-custom-with-empty-file": MigrationCase(
        files=("staging-write.config.toml",),
        environ="staging-write",
        environments=("staging-write",),
        active="staging-write",
        environment="staging-write",
    ),
    "read-only-settings-dir": MigrationCase(
        recorded="staging",
        read_only=True,
        environments=("staging",),
        active="staging",
        environment="staging",
    ),
}


@pytest.mark.parametrize("case", MIGRATION_CASES.values(), ids=list(MIGRATION_CASES))
def test_workspace_file_migration(case: MigrationCase, mocker: MockerFixture) -> None:
    """The migration of an empty `.dlt/.workspace` declares the profiles with settings files and
    the recorded one, each as an environment of its own."""
    with isolated_workspace(case.workspace) as ctx:
        path = ctx.get_setting(WORKSPACE_FILE_NAME)
        run_dir, name = ctx.run_dir, ctx.name
        touch_settings(ctx, *case.files)
        if case.recorded:
            save_profile_pin(ctx, case.recorded)
        # the context above migrated the workspace already
        empty_workspace_file(ctx)
        if case.read_only:
            # stands in for a settings dir that refuses the write
            mocker.patch("dlt._workspace.workspace_file.save_yaml_file", side_effect=OSError)
        expected = build_stored_workspace_definition(
            create_built_in_workspace_definition(), DEFINITION_PATH
        )
        for env in case.environments:
            # deployed with the profile alone and, without `strict_access`, not strict
            expected["environments"][env] = {
                "profiles": {env: {"data_access": "write", "default": True}},
            }
        written: DictStrAny = {} if case.read_only else dict(expected)

        with custom_environ({WORKSPACE__PROFILE: case.environ} if case.environ else {}):
            # the second load reads what the first wrote, in a read-only dir it migrates again
            for _ in range(2):
                ctx = switch_context(run_dir, profile=case.profile)
                assert (ctx.profile, ctx.environment) == (case.active, case.environment)
                assert (
                    build_stored_workspace_definition(ctx.workspace_definition, DEFINITION_PATH)
                    == expected
                )
                assert list(ctx.environment_definitions) == ["prod", *case.environments]
                assert load_yaml_file(path) == written
            # the remote workspace of a migrated profile is named after its environment
            suffix = "" if case.environment in (None, "prod") else f"-{case.environment}"
            assert ctx.name == name + suffix


def test_build_stored_workspace_definition() -> None:
    doc = _doc()
    # `configured` is computed on load and `deployment_module` comes from the configuration
    doc["environments"]["ci"]["profiles"]["ci-write"]["configured"] = True
    doc["environments"]["ci"]["deployment_module"] = "__ci__"
    doc["local_profiles"]["dev"]["configured"] = False
    stored = build_stored_workspace_definition(doc, DEFINITION_PATH)
    # the name of an environment is its key, the environment and the location of a profile
    # follow from where it is declared
    assert stored["environments"]["ci"] == {
        "strict_access": True,
        "profiles": {
            "ci-write": {"data_access": "write", "default": True},
            "ci-read": {"data_access": "read", "default": True},
            "ci-none": {"data_access": "none", "default": True},
        },
    }
    assert stored["local_profiles"] == {
        "dev": {"data_access": "write", "default": True},
        "tests": {"data_access": "write"},
    }
    # expanded, the definition states again what follows from where an entry is declared
    assert expand_workspace_definition(stored) == _doc()
    # `all` is a data verb of job access: a profile that declares it is valid and loads as write
    stored["local_profiles"]["tests"]["data_access"] = "all"
    stored["environments"]["ci"]["profiles"]["ci-write"]["data_access"] = "all"
    validate_workspace_definition(stored, DEFINITION_PATH)
    assert expand_workspace_definition(stored) == _doc()

    # an invalid definition is validated before it is saved, the file stays as it is
    doc["local_profiles"]["tests"]["default"] = True
    with pytest.raises(InvalidWorkspaceFile, match="all the default local profile"):
        build_stored_workspace_definition(doc, DEFINITION_PATH)
    with isolated_workspace("empty") as ctx:
        content = load_yaml_file(get_workspace_file(ctx))
        with pytest.raises(InvalidWorkspaceFile):
            save_workspace_file(ctx, doc)
        assert load_yaml_file(get_workspace_file(ctx)) == content


def test_configured_follows_settings_files() -> None:
    with isolated_workspace("empty") as ctx:
        # a declared profile without settings files is not configured, one without data access is
        # usable all the same
        assert ctx.get_profile_definition("dev")["configured"] is False
        profiles = ctx.get_environment_definition("prod")["profiles"]
        assert profiles == {
            "prod": {**_remote("prod"), "configured": False},
            "access": {**_remote("prod", "read"), "configured": False},
            "no-access": {**_remote("prod", "none"), "configured": False},
        }
        assert [name for name, p in profiles.items() if is_profile_usable(p)] == ["no-access"]
        # the flag follows the settings files once the definition is loaded again
        touch_settings(ctx, "access.config.toml")
        assert ctx.get_profile_definition("access")["configured"] is False
        ctx.reload_workspace_definition()
        assert ctx.get_profile_definition("access")["configured"] is True
        assert ctx.get_profile_definition("prod")["configured"] is False
        # a new context computes it as well
        ctx = ctx.switch_profile("tests")
        assert ctx.get_profile_definition("access")["configured"] is True
        # a profile without data access needs no settings files
        add_profiles(ctx, {"audit": {"data_access": "none"}}, "prod")
        assert ctx.get_profile_definition("audit")["configured"] is False
        assert is_profile_usable(ctx.get_profile_definition("audit"))
        assert ctx.configured_profiles() == ["access"]

        # the saved file has no flag, and a flag written to the file is refused
        path = get_workspace_file(ctx)
        stored = load_yaml_file(path)
        assert "configured" not in dump_yaml(stored)
        stored["environments"]["prod"]["profiles"]["prod"]["configured"] = True
        _write_workspace_file(ctx, dump_yaml(stored))
        with pytest.raises(InvalidWorkspaceFile, match="unexpected fields"):
            ctx.get_profile_definition("prod")
