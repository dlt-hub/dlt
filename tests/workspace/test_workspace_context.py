import os
import pytest
import pickle

import dlt
from dlt._workspace._workspace_context import WorkspaceRunContext, switch_context
from dlt._workspace.cli.utils import check_delete_local_data, delete_local_data
from dlt._workspace.exceptions import WorkspaceRunContextNotAvailable
from dlt._workspace.profile import DEFAULT_PROFILE, read_profile_pin, save_profile_pin
from dlt._workspace.run_context import (
    DEFAULT_LOCAL_FOLDER,
    DEFAULT_WORKSPACE_WORKING_FOLDER,
    switch_profile,
)
from dlt._workspace.cli.echo import always_choose
from dlt.common.runtime.exceptions import RunContextNotAvailable
from dlt.common.runtime.run_context import DOT_DLT, RunContext, global_dir

from tests.pipeline.utils import assert_table_counts
from tests.workspace.utils import isolated_workspace


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
        # standard global dir
        assert ctx.global_dir == global_dir()
        assert_dev_config()


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
        assert ctx.local_dir.endswith(os.path.join("_local", "tests"))

        ctx = ctx.switch_profile("dev")
        assert ctx.name == "name_override"
        assert ctx.data_dir == os.path.join(ctx.run_dir, "_data")
        # this OSS compat mode where local dir is same as run dir
        assert ctx.local_dir == os.path.join(ctx.run_dir, ".")


def test_pinned_profile() -> None:
    with isolated_workspace("default") as ctx:
        save_profile_pin(ctx, "prod")
        assert read_profile_pin(ctx) == "prod"

        # this is new default profile
        ctx = switch_context(ctx.run_dir)
        assert ctx.profile == "prod"
        ctx = dlt.current.workspace()
        assert ctx.profile == "prod"
        assert_workspace_context(ctx, "default", "prod")


def test_dev_env_overwrite() -> None:
    pass


def test_workspace_pipeline() -> None:
    pytest.importorskip("duckdb", minversion="1.3.2")

    with isolated_workspace("pipelines", profile="tests") as ctx:
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
            delete_local_data(ctx, check_delete_local_data(ctx, skip_data_dir=False))
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

    expected_settings = os.path.join(context.run_dir, DOT_DLT)
    assert context.settings_dir == expected_settings

    # path / .var / profile
    expected_data_dir = os.path.join(
        context.settings_dir, DEFAULT_WORKSPACE_WORKING_FOLDER, profile
    )
    assert context.data_dir == expected_data_dir
    # got created
    assert os.path.isdir(context.data_dir)

    # local files
    expected_local_dir = os.path.join(context.run_dir, DEFAULT_LOCAL_FOLDER, profile)
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
