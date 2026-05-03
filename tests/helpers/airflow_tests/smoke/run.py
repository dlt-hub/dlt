import os
import json
import subprocess
import tempfile
import time
from pathlib import Path


DAG_ID = "dlt_smoke"
LOGICAL_DATE = "2024-01-01"
DATASET_NAME = "smoke_data"
POLL_SECONDS = 2
TIMEOUT_SECONDS = 180

EXPECTED_TABLES = {("users", 9), ("events", 12)}


def run_airflow(args, env, check=True, capture_output=True):
    cmd = ["uv", "run", "airflow", *args]
    return subprocess.run(
        cmd,
        env=env,
        text=True,
        check=check,
        capture_output=capture_output,
    )


def verify(db_path: str) -> None:
    """Query the sqlite destination directly and verify loaded data."""
    import dlt

    if not os.path.exists(db_path):
        raise RuntimeError(f"sqlite file not found at {db_path}")

    pipeline = dlt.attach(
        pipeline_name="dlt_smoke",
        dataset_name=DATASET_NAME,
        destination=dlt.destinations.sqlalchemy(f"sqlite:///{db_path}"),
    )
    # 3 dags were run: so original item counts * 3
    assert set(pipeline.dataset().row_counts().fetchall()) == EXPECTED_TABLES
    print("\nSmoke test PASSED")


def main():
    script_dir = Path(__file__).resolve().parent
    work_dir = Path(tempfile.mkdtemp())
    scheduler = None
    api_server = None

    try:
        airflow_home = work_dir / "airflow"
        airflow_home.mkdir(parents=True, exist_ok=True)
        db_path = str(work_dir / "smoke.db")

        print("=== START ===")
        print(f"work_dir: {work_dir}")

        env = os.environ.copy()
        env["AIRFLOW_HOME"] = str(airflow_home)
        env["AIRFLOW__CORE__DAGS_FOLDER"] = str(script_dir / "dags")
        env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        env["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
        env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{airflow_home / 'airflow.db'}"
        env["AIRFLOW__API__BASE_URL"] = "http://127.0.0.1:8080"
        # Airflow >=3.2 routes log messages and warnings to stdout, polluting
        # CLI output we parse (e.g. `dags state`). Use ERROR to suppress them.
        env["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "ERROR"
        env["DLT_SMOKE_DB_PATH"] = db_path
        # use 2 normalize workers to exercise spawn pool inside Airflow (#3586)
        env["NORMALIZE__WORKERS"] = "2"

        print("=== Initializing Airflow DB ===")
        try:
            run_airflow(["db", "migrate"], env)
        except subprocess.CalledProcessError:
            run_airflow(["db", "init"], env)

        print("=== Starting API Server ===")
        api_server = subprocess.Popen(
            ["uv", "run", "airflow", "api-server", "--port", "8080"],
            env=env,
            stdout=open(work_dir / "api-server.log", "w", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            text=True,
        )

        print("=== Starting scheduler ===")
        scheduler = subprocess.Popen(
            ["uv", "run", "airflow", "scheduler"],
            env=env,
            stdout=open(work_dir / "scheduler.log", "w", encoding="utf8"),
            stderr=subprocess.STDOUT,
            text=True,
        )

        print("=== Waiting for DAG to be discoverable ===")
        for _ in range(60):
            run_airflow(["dags", "reserialize"], env, check=False)
            result = run_airflow(["dags", "list"], env)
            if DAG_ID in result.stdout:
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"DAG {DAG_ID!r} was not discovered")

        print("=== Unpausing DAG ===")
        run_airflow(["dags", "unpause", DAG_ID], env, check=False)

        run_id = f"manual_smoke_{int(time.time())}"

        print("=== Triggering DAG run ===")
        try:
            run_airflow(
                ["dags", "trigger", DAG_ID, "--run-id", run_id, "--logical-date", LOGICAL_DATE],
                env,
            )
        except subprocess.CalledProcessError:
            run_airflow(
                ["dags", "trigger", DAG_ID, "--run-id", run_id, "--exec-date", LOGICAL_DATE],
                env,
            )

        print("=== Waiting for terminal state ===")
        deadline = time.time() + TIMEOUT_SECONDS
        while time.time() < deadline:
            result = run_airflow(["dags", "state", DAG_ID, run_id], env, check=False)
            state = result.stdout.strip().lower()
            if state == "success":
                print("DAG run succeeded")
                break
            if state == "failed":
                res = run_airflow(
                    ["tasks", "states-for-dag-run", DAG_ID, run_id, "--output", "json"],
                    env,
                    check=False,
                )
                print("=== Task states ===")
                print(res.stdout)

                failed_tasks = []
                try:
                    rows = json.loads(res.stdout)
                    for row in rows:
                        if str(row.get("state", "")).lower() == "failed":
                            failed_tasks.append((row["task_id"], int(row.get("try_number") or 1)))
                except Exception:
                    pass

                for task_id, try_number in failed_tasks:
                    log_path = (
                        airflow_home
                        / "logs"
                        / f"dag_id={DAG_ID}"
                        / f"run_id={run_id}"
                        / f"task_id={task_id}"
                        / f"attempt={try_number}.log"
                    )
                    print(f"=== Log for failed task {task_id} ===")
                    if log_path.exists():
                        print(log_path.read_text(errors="replace")[-20000:])
                    else:
                        print(f"Log not found: {log_path}")
                raise RuntimeError("DAG run failed")
            time.sleep(POLL_SECONDS)
        else:
            raise TimeoutError("Timed out waiting for DAG run to finish")

        print("=== Verifying results ===")
        verify(db_path)

    finally:
        if api_server is not None:
            api_server.terminate()
            try:
                api_server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                api_server.kill()

        if scheduler is not None:
            scheduler.terminate()
            try:
                scheduler.wait(timeout=10)
            except subprocess.TimeoutExpired:
                scheduler.kill()

        # uncomment to inspect on failure:
        # print(f"Work dir preserved: {work_dir}")
        import shutil

        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
