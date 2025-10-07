import pytest
import subprocess
import time
import os
from pathlib import Path


@pytest.fixture(scope="session", autouse=True)
def dashboard_server():
    env = os.environ.copy()
    env["DLT_PROJECT_DIR"] = str(Path(__file__).parent.parent.parent)
    
    proc = subprocess.Popen(
        ["dlt", "pipeline", "--list-pipelines"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    proc.wait()
    
    proc = subprocess.Popen(
        ["dlt", "pipeline", "info", "--start-server", "2718"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    time.sleep(3)
    
    yield
    
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {
        **browser_context_args,
        "ignore_https_errors": True,
        "viewport": {"width": 1920, "height": 1080},
    }
