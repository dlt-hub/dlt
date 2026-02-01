from pytest_console_scripts import ScriptRunner


def test_license_command(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "license", "-h"])
    assert result.returncode == 0

    assert "Usage: dlt license" in result.stdout
