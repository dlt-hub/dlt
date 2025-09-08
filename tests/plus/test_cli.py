from pytest_console_scripts import ScriptRunner


def test_project_command(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "project", "-h"])
    assert result.returncode == 0

    assert "Usage: dlt project" in result.stdout
