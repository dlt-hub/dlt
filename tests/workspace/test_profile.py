import os

import pytest

import dlt
from dlt.common.utils import custom_environ

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.environment import add_environments, new_environment
from dlt._workspace.exceptions import (
    EnvironmentNotDeclared,
    InvalidProfileName,
    InvalidWorkspaceFile,
    ProfileAlreadyExists,
    ProfileNotDeclared,
)
from dlt._workspace.profile import (
    activate_profile,
    add_profiles,
    check_profile_declared,
    declare_profiles,
)
from dlt._workspace.settings_files import load_yaml_file
from dlt._workspace.workspace_definition import (
    WORKSPACE_FILE_NAME,
    environment_profiles,
    load_workspace_definition,
)

from tests.workspace.utils import declare_environment, isolated_workspace, save_definition


def test_add_profiles() -> None:
    with isolated_workspace("empty") as ctx:
        # declared and not saved: neither the file nor the context changed
        doc = declare_profiles(ctx, {"lab": {"location": "local", "data_access": "write"}})
        assert "lab" in doc["local_profiles"]
        stored = load_yaml_file(ctx.get_setting(WORKSPACE_FILE_NAME))
        assert "lab" not in stored["local_profiles"]
        assert "lab" not in ctx.profile_definitions

        declare_environment(ctx, "ci")
        # refused: a declared profile, an invalid name, an undeclared environment
        with pytest.raises(ProfileAlreadyExists):
            add_profiles(ctx, {"ci-read": {"location": "remote", "data_access": "read"}}, "ci")
        with pytest.raises(InvalidProfileName):
            add_profiles(ctx, {"a b": {"location": "local", "data_access": "write"}})
        with pytest.raises(EnvironmentNotDeclared):
            add_profiles(
                ctx, {"x": {"location": "remote", "data_access": "read", "env": "qa"}}, "qa"
            )
        # invalid together with the declared profiles
        with pytest.raises(InvalidWorkspaceFile, match="under `local_profiles` is local"):
            add_profiles(ctx, {"x": {"location": "remote", "data_access": "read", "env": "ci"}})
        with pytest.raises(InvalidWorkspaceFile, match="in environment `ci`, set `env: ci`"):
            add_profiles(
                ctx, {"x": {"location": "remote", "data_access": "read", "env": "prod"}}, "ci"
            )
        assert "x" not in ctx.profile_definitions
        # a profile that states no location and no environment gets them from where it is declared
        add_profiles(ctx, {"x": {"data_access": "read"}}, "ci")  # type: ignore[typeddict-item]
        assert ctx.get_profile_definition("x") == {
            "location": "remote",
            "data_access": "read",
            "env": "ci",
            "configured": False,
        }

        # the first profile of a data access in an environment becomes its default, also among
        # the profiles added in one call
        add_environments(ctx, {"qa": new_environment("qa")})
        add_profiles(
            ctx,
            {
                "qa-read": {"location": "remote", "data_access": "read"},
                "qa-read2": {"location": "remote", "data_access": "read"},
                "qa-write2": {"location": "remote", "data_access": "write"},
            },
            "qa",
        )
        qa_profiles = ctx.environment_definitions["qa"]["profiles"]
        assert {name: profile.get("default") for name, profile in qa_profiles.items()} == {
            "qa-write": True,
            "qa-read": True,
            "qa-read2": None,
            "qa-write2": None,
        }

        # no local profile is the default: a profile of an environment never takes it, the
        # first local profile added does
        doc = load_workspace_definition(ctx)
        del doc["local_profiles"]["dev"]["default"]
        save_definition(ctx, doc)
        add_profiles(
            ctx, {"ci-audit": {"location": "remote", "data_access": "none", "env": "ci"}}, "ci"
        )
        assert ctx.get_profile_definition("ci-audit").get("default") is None
        assert environment_profiles(ctx.environment_definitions["ci"])[-1] == "ci-audit"
        add_profiles(ctx, {"local2": {"location": "local", "data_access": "write"}})
        assert ctx.default_profile == "local2"


def test_check_profile_declared() -> None:
    with isolated_workspace("default") as ctx:
        declare_environment(ctx, "ci")
        for name in ("dev", "access", "ci-read"):
            check_profile_declared(name)
        # an undeclared name and the name of an environment are no profiles
        for name in ("staging", "ci"):
            with pytest.raises(ProfileNotDeclared) as exc:
                check_profile_declared(name)
            assert exc.value.source == "argument"
        # for an environment the message lists the profiles to name instead
        assert (
            "`ci` is an environment. Name one of its profiles. Declared profiles: dev, tests, prod,"
            " access, no-access, ci-write, ci-read, ci-none."
            in str(exc.value)
        )
        # WORKSPACE__PROFILE does not exempt a name
        with custom_environ({WORKSPACE__PROFILE: "staging"}):
            with pytest.raises(ProfileNotDeclared):
                check_profile_declared("staging")


def test_activate_profile() -> None:
    with isolated_workspace("default") as ctx:
        activate_profile("prod")
        assert os.environ[WORKSPACE__PROFILE] == "prod"
        assert dlt.current.workspace().profile == "prod"
        assert dlt.current.workspace() is not ctx
        declare_environment(dlt.current.workspace(), "ci")
        activate_profile("ci-read")
        assert os.environ[WORKSPACE__PROFILE] == "ci-read"
        assert dlt.current.workspace().profile == "ci-read"
        assert dlt.current.workspace().environment == "ci"
        # an environment and an undeclared name are refused before the variable is set
        for name in ("ci", "qa"):
            with pytest.raises(ProfileNotDeclared) as exc:
                activate_profile(name)
            assert (exc.value.name, exc.value.source) == (name, "argument")
            assert os.environ[WORKSPACE__PROFILE] == "ci-read"
            assert dlt.current.workspace().profile == "ci-read"
    with isolated_workspace("legacy", required=None):
        # outside of a workspace only the environment variable is set
        activate_profile("prod")
        assert os.environ[WORKSPACE__PROFILE] == "prod"
