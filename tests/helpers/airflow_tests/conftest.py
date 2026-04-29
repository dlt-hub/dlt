import os
import tempfile

# point AIRFLOW_HOME at a temp directory before any airflow import so that
# a stale ~/airflow/airflow.cfg (e.g. with AF3 xcom_backend) doesn't poison AF2
os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="dlt_airflow_test_")

from tests.helpers.airflow_tests.utils import initialize_airflow_db
from tests.utils import (
    preserve_environ,
    autouse_test_storage,
    get_test_storage_root,
    auto_test_run_context,
)
