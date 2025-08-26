import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")

with app.setup:
    import os
    from typing import Iterator

    import marimo as mo
    import dlt
    import requests
    from dlt.sources.rest_api import rest_api_source
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth


    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Define""")
    return


@app.function
@dlt.resource(primary_key="id")
def gha_workflows(client: RESTClient, owner="dlt-hub", repo="dlt"):
    response = client.get(
        path="repos/{owner}/{repo}/actions/workflows",
        params={"owner": owner, "repo": repo}
    )
    data = response.content
    for workflow in data["workflows"]:
        yield workflow


@app.function
@dlt.resource(primary_key="id")
def gha_runs(client: RESTClient, owner="dlt-hub", repo="dlt"):
    FIELDS_TO_KEEP = [
        "id",
        "workflow_id",
        "name",
        "display_title",
        "node_id",
        "head_branch",
        "head_sha",
        "path",
        "run_number",
        "run_attempt",
        "event",
        "status",
        "conclusion",
        "url",
        "html_url",
        "created_at",
        "updated_at",
        "run_started_at",
        # actor: id, type, html_url
        # triggering_actor: id, type, html_url
        "head_commit",
        # head_repository: html_url
        "repository",
        "jobs_url",
        "logs_url",
        "check_suite_url",
        "artifacts_url",
        "workflow_url",
    ]
    
    response = client.get(
        path="repos/{owner}/{repo}/actions/runs",
        params={"owner": owner, "repo": repo},
    )
    data = response.content
    for run in data["workflow_runs"]:
        yield run


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Organize""")
    return


@app.function
@dlt.source
def github():
    client = RESTClient(
        base_url="https://api.github.com/",
        headers={"Accept": "application/vnd.github+json"},
        auth=BearerTokenAuth(),  # TODO
    )

    return [gha_workflows(client)]


@app.cell
def _():
    pipeline = dlt.pipeline("github_actions", destination="duckdb")
    return (pipeline,)


@app.cell
def _(pipeline, source):
    pipeline.run(source.add_limit(20))
    return


@app.cell
def _(pipeline):
    dataset = pipeline.dataset()
    return (dataset,)


@app.cell
def _(dataset):
    dataset["gha_runs"].df()
    return


@app.cell
def _(BASE_URL, HEADERS):





    def fetch_paginated(url: str) -> Iterator[dict]:
        """Generator to handle GitHub paginated results"""
        while url:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            for item in data.get("workflow_runs", []) + data.get("jobs", []):
                yield item
            # Get next page URL from headers
            url = None
            if 'Link' in response.headers:
                links = response.headers['Link'].split(',')
                for link in links:
                    if 'rel="next"' in link:
                        url = link[link.find('<')+1:link.find('>')]
                        break

    @dlt.source
    def github_workflow_runs():
        url = f"{BASE_URL}/runs?per_page=100"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        runs = response.json().get("workflow_runs", [])
        for run in runs:
            yield run

    @dlt.resource(name="workflow_runs")
    def workflow_runs_resource():
        return github_workflow_runs()

    @dlt.source
    def github_jobs(run_id: int):
        url = f"{BASE_URL}/runs/{run_id}/jobs?per_page=100"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        jobs = response.json().get("jobs", [])
        for job in jobs:
            yield job

    @dlt.resource(name="jobs")
    def jobs_resource():
        # We'll fetch jobs for all runs
        runs = list(github_workflow_runs())
        for run in runs:
            run_id = run["id"]
            for job in github_jobs(run_id):
                # Add run_id to link job with run
                job["workflow_run_id"] = run_id
                yield job

    if __name__ == "__main__":
        pipeline = dlt.pipeline(pipeline_name="github_actions", destination="duckdb")
        # Run both resources in the same pipeline
        load_info = pipeline.run(workflow_runs_resource(), jobs_resource())
        print(load_info)

    return (pipeline,)


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
