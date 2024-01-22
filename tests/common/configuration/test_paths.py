import os
import shutil

import pytest

from dlt.common.configuration.paths import create_symlink_to_dlt

DOT_DLT = ".dlt"


@pytest.fixture
def setup_and_teardown_symlink_env(tmp_path):
    # Set up: Change working directory to a temporary path
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    yield tmp_path

    # Teardown: Change back to the original working directory and cleanup temporary files
    os.chdir(original_cwd)
    if (tmp_path / DOT_DLT).is_dir():
        shutil.rmtree(tmp_path / DOT_DLT)
    if (tmp_path / "symlink_to_dlt").exists():
        os.remove(tmp_path / "symlink_to_dlt")
    shutil.rmtree(tmp_path)


def test_create_symlink_to_dlt(setup_and_teardown_symlink_env):
    tmp_path = setup_and_teardown_symlink_env
    symlink_dir = tmp_path / "symlink_to_dlt"

    # Run the function
    create_symlink_to_dlt(symlink_dir)

    # Assert that .dlt directory is created
    dlt_dir = tmp_path / DOT_DLT
    assert dlt_dir.is_dir()

    # Assert that the symbolic link is created and points to the .dlt directory
    assert symlink_dir.is_symlink()
    assert symlink_dir.resolve().samefile(dlt_dir)
