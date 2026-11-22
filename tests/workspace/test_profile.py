import os

import pytest

import dlt
from dlt.common.utils import custom_environ

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.environment import (
    add_environments,
    create_environment_definition,
    get_profiles_for_env,
)
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
from dlt._workspace.workspace_file import WORKSPACE_FILE_NAME, load_workspace_definition

from tests.workspace.utils import declare_environment, isolated_workspace, save_definition


def test_add_profiles() -> None:
    with isolated_workspace("empty") as ctx:
        # declared into a loaded definition: neither the file nor the context changed
        doc = load_workspace_definition(ctx)
        declare_profiles(doc, {"lab": {"data_access": "write"}}, None, "")
        assert "lab" in doc["local_profiles"]
        stored = load_yaml_file(ctx.get_setting(WORKSPACE_FILE_NAME))
        assert "lab" not in stored["local_profiles"]
        assert "lab" not in ctx.profile_definitions

        declare_environment(ctx, "ci")
        # refused: a declared profile, an invalid name, an undeclared environment
        with pytest.raises(ProfileAlreadyExists):
            add_profiles(ctx, {"ci-read": {"data_access": "read"}}, "ci")
        with pytest.raises(InvalidProfileName):
            add_profiles(ctx, {"a b": {"data_access": "write"}})
        with pytest.raises(EnvironmentNotDeclared):
            add_profiles(ctx, {"x": {"data_access": "read"}}, "qa")
        # invalid as stored, so not saved
        with pytest.raises(InvalidWorkspaceFile, match="a local profile has write data access"):
            add_profiles(ctx, {"x": {"data_access": "read"}})
        assert "x" not in ctx.profile_definitions
        # where a profile is declared gives its location and environment, whatever it states
        stated = {"location": "local", "env": "prod", "data_access": "read"}
        add_profiles(ctx, {"x": stated}, "ci")  # type: ignore[dict-item]
        assert ctx.get_profile_definition("x") == {
            "location": "remote",
            "data_access": "read",
            "env": "ci",
            "configured": False,
        }
        stated = {"location": "remote", "env": "ci", "data_access": "write"}
        add_profiles(ctx, {"lab": stated})  # type: ignore[dict-item]
        assert ctx.get_profile_definition("lab")["location"] == "local"
        assert "env" not in ctx.get_profile_definition("lab")

        # the first profile of a data access in an environment becomes its default, also among
        # the profiles added in one call
        add_environments(ctx, {"qa": create_environment_definition("qa")})
        add_profiles(
            ctx,
            {
                "qa-read": {"data_access": "read"},
                "qa-read2": {"data_access": "read"},
                "qa-write2": {"data_access": "write"},
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

        # `dev` without the flag stays the default: no added profile takes it
        doc = load_workspace_definition(ctx)
        del doc["local_profiles"]["dev"]["default"]
        save_definition(ctx, doc)
        add_profiles(ctx, {"ci-audit": {"data_access": "none"}}, "ci")
        assert ctx.get_profile_definition("ci-audit").get("default") is None
        assert get_profiles_for_env(ctx.environment_definitions["ci"])[-1] == "ci-audit"
        add_profiles(ctx, {"local2": {"data_access": "write"}})
        assert ctx.default_profile == "dev"
        # without `dev` the first local profile declared becomes the default
        doc = load_workspace_definition(ctx)
        del doc["local_profiles"]["dev"]
        declare_profiles(doc, {"local3": {"data_access": "write"}}, None, "")
        assert doc["local_profiles"]["local3"]["default"] is True


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
