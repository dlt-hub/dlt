"""Working concept tests for the @job and @interactive decorators.

Run with: uv run python local_workspace/job_tests.py
"""

import asyncio
import inspect

import dlt
from dlt._workspace.deployment.decorators import JobFactory, job, interactive
from dlt._workspace.deployment.typing import TTrigger


# -- 1. @job without parens --
@job
def simple_job():
    """A simple job."""
    return "done"


assert isinstance(simple_job, JobFactory), "decorator without parens returns JobFactory"
assert simple_job.name == "simple_job"
assert simple_job.job_type == "batch"
assert simple_job() == "done"
print("1. @job without parens: OK")


# -- 2. @job with parens and args --
@job(trigger="0 8 * * *", timeout="4h")
def scheduled_job():
    return "scheduled"


assert isinstance(scheduled_job, JobFactory)
assert scheduled_job.job_type == "batch"
assert scheduled_job.timeout == "4h"
assert len(scheduled_job.trigger) == 1
assert scheduled_job.trigger[0] == TTrigger("0 8 * * *")
assert scheduled_job() == "scheduled"
print("2. @job with parens: OK")

# -- 3. trigger properties --
assert simple_job.success == TTrigger("job.success:jobs.job_tests.simple_job")
assert simple_job.fail == TTrigger("job.fail:jobs.job_tests.simple_job")
assert simple_job.completed == (simple_job.success, simple_job.fail)
print("3. .success/.fail/.completed triggers: OK")


# -- 4. trigger referencing another job --
@job(trigger=[simple_job.success, "tag:backfill"])
def chained_job():
    pass


assert len(chained_job.trigger) == 2
assert chained_job.trigger[0] == TTrigger("job.success:jobs.job_tests.simple_job")
assert chained_job.trigger[1] == TTrigger("tag:backfill")
print("4. Trigger referencing another job: OK")


# -- 5. config injection --
@job
def config_job(job_name=dlt.config.value):
    """This magic job will run code for any other job"""
    return job_name


job_def = config_job.to_job_definition()
assert "job_name" in job_def.get("config_keys", []), f"config_keys: {job_def.get('config_keys')}"
print("5. Config injection (config keys discovered): OK")


# -- 6. async job --
@job
async def async_job():
    return "async_done"


assert isinstance(async_job, JobFactory)
result = asyncio.run(async_job())
assert result == "async_done"
assert inspect.iscoroutinefunction(async_job._deco_f)
print("6. Async job: OK")

# -- 7. to_job_definition (batch) --
job_def = scheduled_job.to_job_definition()
assert job_def["job_ref"] == "jobs.job_tests.scheduled_job"
assert job_def["entry_point"]["job_type"] == "batch"
assert job_def["entry_point"]["function"] == "scheduled_job"
assert "launcher" not in job_def["entry_point"]
assert job_def["triggers"] == [TTrigger("0 8 * * *")]
assert job_def["execution"]["timeout"] == {"timeout": 14400.0}
assert job_def["execution"]["concurrency"] == 1
print("7. to_job_definition (batch): OK")

# -- 8. isinstance check (detector pattern) --
assert isinstance(simple_job, JobFactory)
assert isinstance(scheduled_job, JobFactory)
assert not isinstance(lambda: None, JobFactory)
print("8. isinstance(decorated, JobFactory): OK")


# -- 9. starred job and tags --
@job(starred=True, tags=["ingestion", "backfill"])
def starred_job():
    pass


assert starred_job.starred is True
assert starred_job.tags == ["ingestion", "backfill"]
job_def = starred_job.to_job_definition()
assert job_def["starred"] is True
assert job_def["tags"] == ["ingestion", "backfill"]
print("9. Starred job and tags: OK")

# -- 10. description from docstring --
job_def = config_job.to_job_definition()
assert job_def["description"] == "This magic job will run code for any other job"
print("10. Description from docstring: OK")


# -- 11. signature preservation --
@job
def parameterized_job(x: int, y: str = "hello"):
    return f"{x}-{y}"


sig = inspect.signature(parameterized_job)
params = list(sig.parameters.keys())
assert "x" in params
assert "y" in params
print("11. Signature preservation: OK")


# -- 12. @interactive without parens --
@interactive
def my_webapp():
    """A web app."""
    pass


assert isinstance(my_webapp, JobFactory)
assert my_webapp.job_type == "interactive"
assert my_webapp.expose == {"interface": "gui", "port": 5000}
assert my_webapp.trigger == [TTrigger("http:5000")]
print("12. @interactive without parens: OK")


# -- 13. @interactive with port and interface --
@interactive(port=9090, interface="rest_api")
def my_api():
    """REST API server."""
    pass


assert isinstance(my_api, JobFactory)
assert my_api.job_type == "interactive"
assert my_api.expose == {"interface": "rest_api", "port": 9090}
assert my_api.trigger == [TTrigger("http:9090")]
print("13. @interactive with port and interface: OK")

# -- 14. @interactive to_job_definition --
job_def = my_api.to_job_definition()
assert job_def["entry_point"]["job_type"] == "interactive"
assert job_def["entry_point"]["expose"] == {"interface": "rest_api", "port": 9090}
assert "launcher" not in job_def["entry_point"]
assert job_def["triggers"] == [TTrigger("http:9090")]
print("14. @interactive to_job_definition: OK")


# -- 15. @interactive with mcp interface --
@interactive(interface="mcp")
def my_mcp_server():
    """MCP tool server."""
    pass


assert my_mcp_server.expose["interface"] == "mcp"
assert my_mcp_server.expose["port"] == 5000
job_def = my_mcp_server.to_job_definition()
assert job_def["entry_point"]["expose"]["interface"] == "mcp"
assert job_def["description"] == "MCP tool server."
print("15. @interactive with mcp interface: OK")


# -- 16. @interactive with run_params --
@interactive(port=8080, run_params={"ws_port": 8081})
def my_app_with_ws():
    pass


assert my_app_with_ws.expose["run_params"] == {"ws_port": 8081}
job_def = my_app_with_ws.to_job_definition()
assert job_def["entry_point"]["expose"]["run_params"] == {"ws_port": 8081}
print("16. @interactive with run_params: OK")

# -- 17. trigger properties work on interactive jobs --
assert my_webapp.success == TTrigger("job.success:jobs.job_tests.my_webapp")
assert my_webapp.fail == TTrigger("job.fail:jobs.job_tests.my_webapp")
print("17. Trigger properties on interactive jobs: OK")


# -- 18. async interactive job --
@interactive(port=3000)
async def async_interactive():
    return "async_interactive"


assert isinstance(async_interactive, JobFactory)
result = asyncio.run(async_interactive())
assert result == "async_interactive"
assert async_interactive.job_type == "interactive"
print("18. Async interactive job: OK")

print("\nAll tests passed!")


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Run a job defined in this module via the job launcher."
    )
    parser.add_argument("job_name", help="name of a job defined in this module")
    parser.add_argument("--run-id", default="test-run-1")
    parser.add_argument("--trigger", default="manual:")
    cli_args = parser.parse_args()

    target = globals().get(cli_args.job_name)
    if not isinstance(target, JobFactory):
        print(f"unknown job: {cli_args.job_name}")
        sys.exit(1)

    job_def = target.to_job_definition()
    # to_job_definition stores __main__ as module when run directly,
    # replace with the real module path for the launcher
    job_def["entry_point"]["module"] = "local_workspace.job_tests"

    from dlt._workspace.deployment.launchers.job import run

    result = run(
        entry_point=job_def["entry_point"],
        run_id=cli_args.run_id,
        trigger=cli_args.trigger,
        config={},
    )
    print(f"result: {result}")
