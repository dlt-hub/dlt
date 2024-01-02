
import pytest
from IPython import get_ipython
import dlt

# @pytest.fixture
# def magic_shell():
#     # Create a mock IPython environment
#     shell = get_ipython()
#     return shell





def test_init_command():
    # Run the init magic command
    from IPython.terminal.interactiveshell import TerminalInteractiveShell
    # import dlt
    from dlt.cli.magics import register_notebook_magics

    shell = TerminalInteractiveShell()
    shell.run_cell("import dlt")
    shell.run_cell("from dlt.cli.magics import register_notebook_magics")
    shell.run_cell("register_notebook_magics()")
    result = shell.run_line_magic('init', "--source_name=chess --destination_name=duckdb --use_generic_template --repo_location=")
    print("result is", result)
    # Check if the init command returns the expected result
    assert result == -1


test_init_command()

