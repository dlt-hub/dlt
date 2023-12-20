
import pytest
import dlt

@pytest.fixture
def magic_shell():
    # Create a mock IPython environment
    shell = get_ipython()
    return shell

def test_init_command(magic_shell):
    # Run the init magic command
    shell= magic_shell()
    result = shell.run_line_magic('init', "--source_name=chess --destination_name=duckdb --use_generic_template --repo_location=https://github.com/dlt-hub/verified-sources")

    # Check if the init command returns the expected result
    assert result == 0