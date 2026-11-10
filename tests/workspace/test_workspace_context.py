import os
import pickle
import tempfile
from typing import List, Optional, Sequence

import pytest
import yaml

import dlt
from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext, switch_context
from dlt._workspace.cli.dlthub.utils import check_delete_local_data, delete_local_data
from dlt._workspace.exceptions import (
    ProfileNotDeclared,
    TProfileSource,
    WorkspaceRunContextNotAvailable,
)
from dlt._workspace.helpers.runtime.runtime_artifacts import sync_from_runtime
from dlt._workspace.profile import DEFAULT_PROFILE, add_profiles
from dlt._workspace.settings_files import load_yaml_file, read_profile_pin, save_profile_pin
from dlt._workspace.workspace_definition import (
    WORKSPACE_FILE_NAME,
    built_in_workspace_definition,
    load_workspace_definition,
    stored_workspace_definition,
)
from dlt._workspace.run_context import (
    DEFAULT_LOCAL_FOLDER,
    DEFAULT_WORKSPACE_WORKING_FOLDER,
    switch_profile,
)
from dlt._workspace.cli.echo import always_choose
from dlt.common.configuration.providers import CONFIG_TOML, SECRETS_TOML
from dlt.common.runtime.exceptions import RunContextNotAvailable
from dlt.common.runtime.exec_info import get_execution_context
from dlt.common.runtime.run_context import DOT_DLT, RunContext
from dlt.common.schema import Schema
from dlt.common.utils import custom_environ

from dlt.common.storages.file_storage import FileStorage
from tests.pipeline.utils import PIPELINE_TEST_CASES_PATH, assert_table_counts
from tests.utils import clean_test_storage
from tests.workspace.utils import (
    empty_workspace_file,
    isolated_workspace,
    save_definition,
    touch_settings,
)


def test_legacy_workspace() -> None:
    # do not create workspace context without feature flag
    with isolated_workspace("legacy", required=None) as ctx:
        assert isinstance(ctx, RunContext)
        # fail when getting active workspace
        with pytest.raises(WorkspaceRunContextNotAvailable):
            dlt.current.workspace()


def test_require_workspace_context() -> None:
    with pytest.raises(RunContextNotAvailable):
        with isolated_workspace("legacy", required="WorkspaceRunContext"):
            pass


def test_workspace_settings() -> None:
    with isolated_workspace("default") as ctx:
        assert_workspace_context(ctx, "default", DEFAULT_PROFILE)
        assert_dev_config()


def test_workspace_profile() -> None:
    with isolated_workspace("default", profile="prod") as ctx:
        assert_workspace_context(ctx, "default", "prod")
        # mocked global dir
        assert ctx.global_dir.endswith(".global_dir")

        # files for dev profile will be ignores
        assert dlt.config["config_val"] == "config.toml"
        assert dlt.config["config_val_ovr"] == "config.toml"
        assert dlt.config.get("config_val_dev") is None

        assert dlt.secrets["secrets_val"] == "secrets.toml"
        assert dlt.secrets["secrets_val_ovr"] == "secrets.toml"
        assert dlt.secrets.get("secrets_val_dev") is None

        # switch profile
        ctx = ctx.switch_profile("dev")
        assert ctx.profile == "dev"
        ctx = dlt.current.workspace()
        assert ctx.profile == "dev"
        assert_workspace_context(ctx, "default", "dev")
        # the context of another profile keeps the mocked global dir
        assert ctx.global_dir.endswith(".global_dir")
        assert_dev_config()


def test_workspace_trace() -> None:
    """Pipeline trace run in a workspace carries run context name and profile and resolved config values"""
    # the workspace is entered with a changed cwd so resolve the relative cases path first
    with open(
        os.path.abspath(f"{PIPELINE_TEST_CASES_PATH}/contracts/trace.schema.yaml"), encoding="utf-8"
    ) as f:
        trace_contract = Schema.from_dict(yaml.safe_load(f), remove_processing_hints=True)

    with isolated_workspace("default", profile="dev") as ctx:
        assert get_execution_context()["run_context"] == {"name": ctx.name, "profile": "dev"}

        @dlt.resource
        def data(
            config_val: str = dlt.config.value,
            config_val_ovr: str = dlt.config.value,
            secrets_val_dev: str = dlt.secrets.value,
        ):
            yield {"config_val": config_val, "config_val_ovr": config_val_ovr}

        pipeline = dlt.pipeline(pipeline_name="workspace_trace", destination="duckdb")
        pipeline.run(data())

        trace = pipeline.last_trace
        assert trace.execution_context["run_context"] == {"name": ctx.name, "profile": "dev"}

        resolved = {v.key: v for v in trace.resolved_config_values}
        assert resolved["config_val"].value == "config.toml"
        # value comes from the profile file which takes precedence
        assert resolved["config_val_ovr"].value == "dev.config.toml"
        # secret values are not stored in the trace
        assert resolved["secrets_val_dev"].is_secret_hint is True
        assert resolved["secrets_val_dev"].value is None
        # profile and base toml files are merged into a single provider so the exact file that
        # held the value is in the location, not in the provider name
        assert resolved["config_val_ovr"].provider_name == CONFIG_TOML
        assert resolved["secrets_val_dev"].provider_name == SECRETS_TOML
        assert resolved["config_val_ovr"].provider_location == "dev." + CONFIG_TOML
        assert resolved["secrets_val_dev"].provider_location == "dev." + SECRETS_TOML
        # key not present in the profile file comes from the base file
        assert resolved["config_val"].provider_location == CONFIG_TOML

        # only a workspace trace carries a profile, so the contract pinned by
        # tests/pipeline/test_pipeline_trace.py can only be enforced for that shape here
        trace_pipeline = dlt.pipeline(
            pipeline_name="workspace_trace_contract", destination="duckdb", dev_mode=True
        )
        trace_pipeline.run(
            [trace], table_name="trace", schema=trace_contract, schema_contract="freeze"
        )
        # freeze rejects unknown columns so the profile must land in the contracted name
        with trace_pipeline.sql_client() as client:
            with client.execute_query(
                "SELECT execution_context__run_context__name,"
                " execution_context__run_context__profile FROM trace"
            ) as cur:
                assert cur.fetchall() == [(ctx.name, "dev")]

        # profile switch is reflected in the next trace
        ctx = ctx.switch_profile("prod")
        pipeline = dlt.pipeline(pipeline_name="workspace_trace", destination="duckdb")
        pipeline.run([{"foo": 1}], table_name="table_foo")
        assert pipeline.last_trace.execution_context["run_context"] == {
            "name": ctx.name,
            "profile": "prod",
        }


def test_profile_switch_no_workspace():
    with isolated_workspace("legacy", required=None):
        with pytest.raises(RunContextNotAvailable):
            switch_profile("dev")


def test_workspace_configuration():
    with isolated_workspace("configured_workspace", profile="tests") as ctx:
        # should be used as component for logging
        assert ctx.runtime_config.pipeline_name == "component"
        assert ctx.name == "name_override"
        # check dirs for tests profile
        assert ctx.data_dir == os.path.join(ctx.run_dir, "_data")
        assert ctx.local_dir.endswith(os.path.join(".dlt", DEFAULT_LOCAL_FOLDER, "tests"))

        ctx = ctx.switch_profile("dev")
        assert ctx.name == "name_override"
        assert ctx.data_dir == os.path.join(ctx.run_dir, "_data")
        # this OSS compat mode where local dir is same as run dir
        assert ctx.local_dir == os.path.join(ctx.run_dir, ".")


@pytest.mark.parametrize(
    "recorded,environ,argument,active",
    [
        (None, None, None, "dev"),
        ("prod", None, None, "prod"),
        (None, "prod", None, "prod"),
        ("prod", "tests", None, "tests"),
        ("prod", "tests", "access", "access"),
    ],
    ids=[
        "default",
        "recorded",
        "environ-over-default",
        "environ-over-recorded",
        "argument-over-environ",
    ],
)
def test_active_profile_precedence(
    recorded: Optional[str], environ: Optional[str], argument: Optional[str], active: str
) -> None:
    """An argument overrides `WORKSPACE__PROFILE`, which overrides the profile recorded in
    `.dlt/profile-name`, which overrides the default profile."""
    with isolated_workspace("default") as ctx:
        if recorded:
            save_profile_pin(ctx, recorded)
            assert read_profile_pin(ctx) == recorded
            # recording a profile creates no settings files
            assert ctx.configured_profiles() == ["dev"]
        with custom_environ({WORKSPACE__PROFILE: environ} if environ else {}):
            ctx = switch_context(ctx.run_dir, profile=argument)
            assert dlt.current.workspace().profile == active
            assert_workspace_context(ctx, "default", active)
        # an override lasts as long as it is given
        assert switch_context(ctx.run_dir).profile == (recorded or "dev")


@pytest.mark.parametrize(
    "recorded,environ,argument,files,migrated,source",
    [
        ("my profile", None, None, (), False, "recorded"),
        (None, "custom", None, (), False, "environ"),
        (None, None, "custom", (), False, "argument"),
        (None, None, "qa", ("qa.config.toml",), True, "argument"),
    ],
    ids=["recorded-invalid-name", "environ-typo", "argument-typo", "file-added-after-migration"],
)
def test_migration_declares_no_undeclared_profile(
    recorded: Optional[str],
    environ: Optional[str],
    argument: Optional[str],
    files: Sequence[str],
    migrated: bool,
    source: TProfileSource,
) -> None:
    """The migration of an empty `.dlt/.workspace` declares no profile that only an argument or
    `WORKSPACE__PROFILE` names and none with an invalid name. After it, settings files declare
    nothing."""
    with isolated_workspace("empty") as ctx:
        path = ctx.get_setting(WORKSPACE_FILE_NAME)
        touch_settings(ctx, *files)
        if recorded:
            save_profile_pin(ctx, recorded)
        if not migrated:
            empty_workspace_file(ctx)
        undeclared = argument or environ or recorded
        with custom_environ({WORKSPACE__PROFILE: environ} if environ else {}):
            with pytest.raises(ProfileNotDeclared) as exc:
                switch_context(ctx.run_dir, profile=argument)
        assert (exc.value.name, exc.value.source) == (undeclared, source)
        assert load_yaml_file(path) == stored_workspace_definition(built_in_workspace_definition())
        if argument:
            # declared as a local profile, it loads with the files it had
            add_profiles(ctx, {argument: {"location": "local", "data_access": "write"}})
            ctx = switch_context(ctx.run_dir, profile=argument)
            assert ctx.profile == argument
            assert ctx.configured_profiles() == ([argument] if files else [])


DECLARED_PROFILES = (
    "Declared profiles: dev, tests, prod, access, analytics, ci-write, ci-read, ci-none."
)


@pytest.mark.parametrize(
    "source,name,fragments",
    [
        (
            "environ",
            "nope",
            ["Set `WORKSPACE__PROFILE` to a declared profile, or unset it.", DECLARED_PROFILES],
        ),
        (
            "environ",
            "ci",
            ["Set `WORKSPACE__PROFILE` to a declared profile, or unset it.", DECLARED_PROFILES],
        ),
        (
            "recorded",
            "nope",
            [
                "Remove `.dlt/profile-name` to run with the default profile.",
                DECLARED_PROFILES,
                (
                    "Then, to use `nope`, run `dlthub profile add nope` and `dlthub local profile"
                    " use nope`."
                ),
            ],
        ),
        (
            "recorded",
            "ci",
            ["Remove `.dlt/profile-name` to run with the default profile.", DECLARED_PROFILES],
        ),
        (
            "argument",
            "nope",
            [
                "run `dlthub profile add nope --local`",
                "run `dlthub profile add nope --env <environment>`. Environments: prod, ci.",
            ],
        ),
        ("argument", "ci", [DECLARED_PROFILES]),
        (
            "default",
            "dev",
            [
                (
                    "It is the default profile, and nothing names another one. Name a declared"
                    " profile with `WORKSPACE__PROFILE`."
                ),
                "Declared profiles: tests, prod, access,",
                "Then declare a local profile with `dlthub profile add dev --local`.",
            ],
        ),
    ],
    ids=[
        "environ-undeclared",
        "environ-environment-name",
        "recorded-undeclared",
        "recorded-environment-name",
        "argument-undeclared",
        "argument-environment-name",
        "default-undeclared",
    ],
)
def test_undeclared_profile_fails_on_load(
    source: TProfileSource, name: str, fragments: List[str]
) -> None:
    with isolated_workspace("job_profiles") as ctx:
        path = ctx.get_setting(WORKSPACE_FILE_NAME)
        if source == "recorded":
            save_profile_pin(ctx, name)
        if source == "default":
            # no local profile is the default, so dlt assumes `dev`
            doc = load_workspace_definition(ctx)
            del doc["local_profiles"]["dev"]
            save_definition(ctx, doc)
        with custom_environ({WORKSPACE__PROFILE: name} if source == "environ" else {}):
            with pytest.raises(ProfileNotDeclared) as exc:
                if source == "argument":
                    ctx.switch_profile(name)
                else:
                    switch_context(ctx.run_dir)
        assert (exc.value.name, exc.value.source) == (name, source)
        message = str(exc.value)
        assert message.startswith(f"`{name}` is not a profile declared in `{path}`.")
        for fragment in fragments:
            assert fragment in message
        # every source says that an environment name is not a profile name
        is_environment = f"`{name}` is an environment. Name one of its profiles."
        assert (is_environment in message) is (name == "ci")
        # an environment name is never declared as a profile, a typo in the variable neither
        adds_profile = any("dlthub profile add" in fragment for fragment in fragments)
        assert ("dlthub profile add" in message) is adds_profile
        # the failed switch keeps the previous context
        assert dlt.current.workspace().profile == "dev"


def test_available_and_configured_profiles() -> None:
    with isolated_workspace("default") as ctx:
        assert set(ctx.available_profiles()) == {"dev", "prod", "tests", "access", "no-access"}
        assert ctx.configured_profiles() == ["dev"]
        # settings files alone make no profile available
        touch_settings(ctx, "staging.config.toml")
        assert "staging" not in ctx.available_profiles()
        assert ctx.configured_profiles() == ["dev"]
        # once declared, the profile is available and configured
        add_profiles(ctx, {"staging": {"location": "local", "data_access": "write"}})
        assert set(ctx.available_profiles()) == {
            "dev",
            "prod",
            "tests",
            "access",
            "no-access",
            "staging",
        }
        assert ctx.configured_profiles() == ["dev", "staging"]


def test_workspace_pipeline() -> None:
    with isolated_workspace("pipelines", profile="tests") as ctx:
        # prod and test have explicit config for profiles
        assert set(ctx.configured_profiles()) == {"tests", "prod"}

        # `ducklake_pipeline` configured in config.toml
        pipeline = dlt.pipeline(pipeline_name="ducklake_pipeline")
        assert pipeline.run_context is ctx
        assert pipeline.dataset_name == "lake_data"
        assert pipeline.destination.destination_name == "ducklake"

        load_info = pipeline.run([{"foo": 1}, {"foo": 2}], table_name="table_foo")
        print(load_info)
        assert_table_counts(pipeline, {"table_foo": 2})
        # make sure that local and working files got created
        assert os.path.isfile(os.path.join(ctx.local_dir, "test_ducklake.sqlite"))
        assert os.path.isdir(os.path.join(ctx.local_dir, "test_ducklake.files"))
        # make sure that working folder got created
        assert os.path.isdir(os.path.join(ctx.get_data_entity("pipelines"), "ducklake_pipeline"))

        # test wipe function
        with always_choose(always_choose_default=False, always_choose_value=True):
            delete_local_data(ctx, check_delete_local_data(ctx, skip_local_data_dir=False))
        # must recreate pipeline
        pipeline = pipeline.drop()
        load_info = pipeline.run([{"foo": 1}, {"foo": 2}], table_name="table_foo")
        print(load_info)
        assert_table_counts(pipeline, {"table_foo": 2})

        # switch to prod
        ctx = ctx.switch_profile("prod")
        # must re-create pipeline if context changed
        pipeline = dlt.pipeline(pipeline_name="ducklake_pipeline")
        load_info = pipeline.run([{"foo": 1}, {"foo": 2}], table_name="table_foo")
        print(load_info)
        assert_table_counts(pipeline, {"table_foo": 2})
        # local files point to prod
        assert os.path.isfile(os.path.join(ctx.local_dir, "prod_ducklake.sqlite"))
        assert os.path.isdir(os.path.join(ctx.local_dir, "prod_ducklake.files"))
        assert set(ctx.configured_profiles()) == {"prod", "tests"}

        # switch to dev: it has no settings files, pipelines do not configure a profile
        ctx = ctx.switch_profile("dev")
        assert set(ctx.configured_profiles()) == {"prod", "tests"}


def test_workspace_send_artifacts() -> None:
    # create a random temp directory for the test bucket
    with tempfile.TemporaryDirectory() as temp_bucket_dir:
        bucket_base = os.path.join(temp_bucket_dir, "local_bucket", "workspace_id")
        send_bucket_url = os.path.join(bucket_base, "tests", "pipelines")

        # mock run id to enable artifact storage
        os.environ["RUNTIME__RUN_ID"] = "uniq_run_id"
        # emit runtime filesystem info
        os.environ["SEND__ARTIFACTS__BUCKET_URL"] = send_bucket_url
        # auto create dirs
        os.environ["ARTIFACTS__KWARGS"] = '{"auto_mkdir": true}'

        with isolated_workspace("pipelines", profile="tests") as ctx:
            # `ducklake_pipeline` configured in config.toml
            pipeline = dlt.pipeline(pipeline_name="ducklake_pipeline")
            pipeline.run([{"foo": 1}, {"foo": 2}], table_name="table_foo")

            print(ctx.run_dir)

        # delete the whole workspace
        clean_test_storage()

        with isolated_workspace("pipelines", profile="tests") as ctx:
            # now restore pipeline from bucket
            os.environ["SYNC__ARTIFACTS__BUCKET_URL"] = bucket_base
            sync_from_runtime()
            # now pipeline sees restored state
            pipeline = dlt.pipeline(pipeline_name="ducklake_pipeline")
            assert pipeline.first_run is False
            assert pipeline.default_schema_name == "ducklake"
            assert pipeline.default_schema.tables["table_foo"] is not None
            assert pipeline.last_trace is not None


def assert_dev_config() -> None:
    # check profile toml providers
    assert dlt.config["config_val"] == "config.toml"
    assert dlt.config["config_val_ovr"] == "dev.config.toml"
    assert dlt.config["config_val_dev"] == "dev.config.toml"

    assert dlt.secrets["secrets_val"] == "secrets.toml"
    assert dlt.secrets["secrets_val_ovr"] == "dev.secrets.toml"
    assert dlt.secrets["secrets_val_dev"] == "dev.secrets.toml"


def assert_workspace_context(context: WorkspaceRunContext, name_prefix: str, profile: str) -> None:
    # basic properties must be set
    assert context.name.startswith(name_prefix)
    assert context.profile == profile
    assert context.default_profile == "dev"
    assert context.profile in context.available_profiles()

    expected_settings = os.path.join(context.run_dir, DOT_DLT)
    assert context.settings_dir == expected_settings

    # .dlt / state / profile
    expected_data_dir = os.path.join(
        context.settings_dir, DEFAULT_WORKSPACE_WORKING_FOLDER, profile
    )
    assert context.data_dir == expected_data_dir
    # got created
    assert os.path.isdir(context.data_dir)

    # local files: .dlt / data / profile
    expected_local_dir = os.path.join(context.settings_dir, DEFAULT_LOCAL_FOLDER, profile)
    assert context.local_dir == expected_local_dir
    # got created
    assert os.path.isdir(context.local_dir)

    # test entity paths
    assert context.get_data_entity("pipelines") == os.path.join(expected_data_dir, "pipelines")
    # no special folders for entities
    assert context.get_run_entity("sources") == context.run_dir
    # settings in settings
    assert context.get_setting("config.toml") == os.path.join(expected_settings, "config.toml")

    # check if can be pickled
    pickled_ = pickle.dumps(context)
    run_context_unpickled = pickle.loads(pickled_)
    assert dict(context.runtime_config) == dict(run_context_unpickled.runtime_config)
    assert dict(context.config) == dict(run_context_unpickled.config)


def test_workspace_run_context_repr() -> None:
    # Test repr with default profile
    with isolated_workspace("default") as ctx:
        result = repr(ctx)
        # must match the exact format specified in the issue
        assert result == f"<WorkspaceRunContext(run_dir={ctx.run_dir!r}, profile={ctx.profile!r})>"
        # profile must appear as plain string (no escaping needed)
        assert ctx.profile in result
        # must follow Python convention: < > wrapping means object can't be re-instantiated
        assert result.startswith("<WorkspaceRunContext(")
        assert result.endswith(")>")

    # Test repr reflects the correct profile when non-default profile is used
    with isolated_workspace("default", profile="dev") as ctx:
        result = repr(ctx)
        assert "dev" in result
        assert result == f"<WorkspaceRunContext(run_dir={ctx.run_dir!r}, profile={ctx.profile!r})>"
