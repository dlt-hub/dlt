from IPython.terminal.interactiveshell import TerminalInteractiveShell


def test_init_command():
    # Run the init magic command
    shell = TerminalInteractiveShell()
    shell.run_cell("import dlt")
    shell.run_cell("from dlt.cli.magics import register_notebook_magics")
    shell.run_cell("register_notebook_magics()")
    result = shell.run_line_magic('init', "--source_name=chess --destination_name=duckdb")
    # Check if the init command returns the expected result
    assert result == 0