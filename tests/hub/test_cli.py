from pytest_console_scripts import ScriptRunner


def test_dlthub_command(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlthub", "init", "--dry-run"])
    assert result.returncode == 0

    assert "Creating dlthub workspace at" in result.stdout
