import os
import pytest
import dlt

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import (
    get_dashboard_config_sections,
    resolve_dashboard_config,
)
from tests.workspace.utils import isolated_workspace


def test_get_dashboard_config_sections(success_pipeline_duckdb) -> None:
    # NOTE: "dashboard" obligatory section comes from configuration __section__
    assert get_dashboard_config_sections(success_pipeline_duckdb) == (
        "pipelines",
        "success_pipeline_duckdb",
    )
    assert get_dashboard_config_sections(None) == ()

    # create workspace context
    with isolated_workspace("configured_workspace"):
        assert get_dashboard_config_sections(None) == ("workspace",)


def test_resolve_dashboard_config(success_pipeline_duckdb) -> None:
    """Test resolving dashboard config with a real pipeline"""

    os.environ["PIPELINES__SUCCESS_PIPELINE_DUCKDB__DASHBOARD__DATETIME_FORMAT"] = "some format"
    os.environ["DASHBOARD__DATETIME_FORMAT"] = "other format"

    config = resolve_dashboard_config(success_pipeline_duckdb)

    assert isinstance(config, DashboardConfiguration)
    assert isinstance(config.datetime_format, str)
    assert config.datetime_format == "some format"

    other_pipeline = dlt.pipeline(pipeline_name="other_pipeline", destination="duckdb")
    config = resolve_dashboard_config(other_pipeline)
    assert config.datetime_format == "other format"

    # create workspace context
    with isolated_workspace("configured_workspace"):
        os.environ["WORKSPACE__DASHBOARD__DATETIME_FORMAT"] = "workspace format"
        config = resolve_dashboard_config(None)
        assert config.datetime_format == "workspace format"
