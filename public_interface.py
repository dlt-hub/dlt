import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")

with app.setup:
    import semver
    import dlt
    import marimo as mo
    import griffe
    import great_tables as gt
    import pyarrow
    import polars as pl
    import requests
    import duckdb


@app.function
def get_release_versions(repo: str, org: str) -> list[str]:
    """Returns versions from old to recent"""
    url = f"https://api.github.com/repos/{org}/{repo}/tags"
    response = requests.get(url)

    release_versions = []
    for tag in response.json():
        try:
            semver.parse(tag["name"])
            release_versions.append(tag["name"])
        except ValueError:
            continue

    return release_versions[::-1]


@app.function
@dlt.resource(file_format="parquet")
def breaking_changes_resource(repo: str = "dlt", org: str = "dlt-hub"):
    state_key = "processed_versions"
    processed_versions = dlt.current.resource_state().setdefault(state_key, [])

    # versions are sorted from old to recent
    release_versions = get_release_versions(repo, org)

    for idx, version in enumerate(release_versions[1:]):
        if version in processed_versions:
            continue

        previous_version = release_versions[idx-1]

        package = griffe.load_git(repo, ref=version)
        previous_package = griffe.load_git(repo, ref=previous_version)

        yield from griffe.find_breaking_changes(package, previous_package)

        processed_versions.append(version)

    dlt.current.resource_state()[state_key] = processed_versions


@app.cell
def _():
    response = requests.get("https://api.github.com/repos/dlt-hub/dlt/tags")
    return (response,)


@app.cell
def _(response):
    dlt_release_versions = []
    for tag in response.json():
        try:
            semver.parse(tag["name"])
            dlt_release_versions.append(tag["name"])
        except ValueError:
            continue
    return (dlt_release_versions,)


@app.function
@mo.cache
def get_breaking_changes(version_tag: str, version_tag_previous: str):
    pkg = griffe.load_git("dlt", ref=version_tag)
    pkg_previous = griffe.load_git("dlt", ref=version_tag_previous)
    return list(griffe.find_breaking_changes(pkg_previous, pkg))


@app.cell
def _(dlt_release_versions):
    version_selector = mo.ui.dropdown(dlt_release_versions, value=dlt_release_versions[0], label="Version")
    return (version_selector,)


@app.cell
def _(dlt_release_versions, version_selector):
    _earlier_versions = dlt_release_versions[dlt_release_versions.index(version_selector.value)+1:]
    previous_version_selector = mo.ui.dropdown(_earlier_versions, value=_earlier_versions[0], label="Previous version")
    return (previous_version_selector,)


@app.cell
def _(previous_version_selector, version_selector):
    versions_selector = mo.hstack([version_selector, previous_version_selector], justify="start")
    versions_selector
    return


@app.cell
def _(previous_version_selector, version_selector):
    breaking_changes = get_breaking_changes(version_selector.value, previous_version_selector.value)
    return (breaking_changes,)


@app.cell
def _(breaking_changes):
    for bc in breaking_changes:
        print(bc._explain_oneline())

    bc = breaking_changes[3]
    return (bc,)


@app.function
def normalize_breaking_change(bc) -> dict:
    qualified_name = bc.obj.name if bc.obj.parent.is_module else bc.obj.parent.name  + "." + bc.obj.name
    return dict(
        module=bc.obj.module.path,
        lineno=bc.obj.lineno,
        parent=bc.obj.parent.name ,
        name=bc.obj.name,
        qualified_name=qualified_name,
        kind=bc.kind.name,
        old_value=str(bc.old_value),
        new_value=str(bc.new_value),
    )


@app.cell
def _(bc):
    bc._format_new_value()
    return


@app.cell
def _(bc):
    normalize_breaking_change(bc)
    return


@app.cell
def _(breaking_changes):
    df = pl.DataFrame([normalize_breaking_change(bc) for bc in breaking_changes])
    df
    return (df,)


@app.cell
def _(df, previous_version_selector, version_selector):
    table = (
        gt.GT(df)
        .tab_header("Breaking Changes", subtitle=f"{previous_version_selector.value} -> {version_selector.value}")
        .tab_stub("qualified_name", groupname_col="module")
        .tab_spanner("source", columns=["parent", "name", "lineno"])
        .fmt_markdown(["old_value", "new_value"])
    )
    table
    return


if __name__ == "__main__":
    app.run()
