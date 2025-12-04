import pytest
from pytest_console_scripts import ScriptRunner

from tests.workspace.utils import isolated_workspace


def test_import_props() -> None:
    import dlt.hub

    # hub plugin found
    assert dlt.hub.__found__
    assert len(dlt.hub.__all__) > 0

    # no exception
    assert dlt.hub.__exception__ is None

    # regular attribute error raised

    with pytest.raises(AttributeError) as attr_err:
        dlt.hub._unknown_feature

    assert "_unknown_feature" in str(attr_err.value)


def test_runtime_client_imports(script_runner: ScriptRunner) -> None:
    pytest.importorskip("dlt_runtime")

    import dlt_runtime  # type: ignore[import-untyped,import-not-found,unused-ignore]

    print(dlt_runtime.__version__)

    # check command activation

    with isolated_workspace("pipelines"):
        result = script_runner.run(["dlt", "runtime", "-h"])
        assert result.returncode == 0
