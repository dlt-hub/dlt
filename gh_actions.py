import marimo

__generated_with = "0.15.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import dlt
    import dlt_plus
    import ibis
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth


@app.cell
def _():
    mo.md(r"""# Etract & Load (EL)""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Define""")
    return


@app.function
def get_dev_client():
    client = RESTClient(
        base_url="https://api.github.com/",
        headers={"Accept": "application/vnd.github+json"},
        auth=BearerTokenAuth(dlt.secrets.get("GH_TOKEN")),
    )
    return client


@app.function
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    columns={"updated_at": {"dedup_sort": "desc"}}
)
def gha_workflows(client: RESTClient, owner="dlt-hub", repo="dlt"):
    for page in client.paginate(
        path=f"repos/{owner}/{repo}/actions/workflows",
        data_selector="workflows",
    ):
        for workflow in page:
            yield workflow


@app.function
def process_workflow_run(item):
    if not item:
        return item

    item["actor"] = {k:v for k,v in item["actor"].items() if k in ["id", "type", "html_url"]}
    item["triggering_actor"] = {k:v for k,v in item["triggering_actor"].items() if k in ["id", "type", "html_url"]}
    item["head_commit"] = {k:v for k,v in item["head_commit"].items() if k in ["id", "message", "timestamp"]}

    if item.get("repository"):
        item["repository"] = item.get("repository", {}).get("html_url")
    if item.get("head_repository"):
        item["head_repository"] = item.get("head_repository", {}).get("html_url")

    for key_to_delete in ("previous_attempt_url", "check_suite_url", "cancel_url", "rerun_url", "check_suite_id", "check_suite_node_id"):
        del item[key_to_delete]

    return item


@app.function
@dlt.resource(
    primary_key="id"
)
def gha_runs(
    client: RESTClient,
    owner="dlt-hub",
    repo="dlt",
    updated_at=dlt.sources.incremental("created_at", lag=3600, last_value_func=min)
):
    for page in client.paginate(
        path=f"repos/{owner}/{repo}/actions/runs",
        data_selector="workflow_runs",
    ):
        for run in page:
            yield process_workflow_run(run)


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Organize""")
    return


@app.function
@dlt.source
def github(GH_TOKEN = dlt.secrets.value):
    client = RESTClient(
        base_url="https://api.github.com/",
        headers={"Accept": "application/vnd.github+json"},
        auth=BearerTokenAuth(GH_TOKEN),
    )

    return [
        gha_workflows(client),
        gha_runs(client),
    ]


@app.cell
def _():
    pipeline = dlt.pipeline("github_actions", destination="duckdb")
    return (pipeline,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Execute""")
    return


@app.cell
def _(pipeline):
    mo.stop()
    pipeline.run(github())
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Review""")
    return


@app.cell
def _(pipeline):
    dataset = pipeline.dataset()
    con = dataset.ibis()
    return (dataset,)


@app.cell
def _(dataset):
    dataset.schema
    return


@app.cell
def _(dataset):
    dataset.row_counts().df()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Transform (T)""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Define""")
    return


@app.cell
def _(dataset):
    prs = dataset.table("gha_runs__pull_requests")
    prs.df()
    return


@app.cell
def _(dataset):
    runs = dataset.table("gha_runs").filter("created_at", "gt", "2025-01-01")
    runs.df()
    return


@app.function
@dlt_plus.transformation
def n_runs_per_pr(dataset: dlt.Dataset):
    runs_rel = dataset.table("gha_runs", table_type="ibis")
    query = (
        runs_rel
        .mutate(duration=(runs_rel.updated_at.epoch_seconds() - runs_rel.created_at.epoch_seconds()))
        # .aggregate(
        #     by="head_branch",
        #     total_duration=ibis._.duration.sum(),
        # )
    )
    
    yield query


@app.cell
def _(dataset):
    df = list(n_runs_per_pr(dataset))[0].df()
    df
    return (df,)


@app.cell
def _(df):
    df.loc[df.duration < 90000].duration.plot(kind="hist", bins=20, logy=True)
    return


@app.function
@dlt_plus.transformation
def n_runs_per_day(dataset: dlt.Dataset):
    yield


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
