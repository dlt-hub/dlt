import sys

import dlt
from dlt.common import json


@dlt.source
def github():
    @dlt.resource(
        table_name="issues",
        write_disposition={"disposition": "merge", "strategy": "scd2"},
        primary_key="id",
    )
    def load_issues():
        # we should be in TEST_STORAGE folder
        with open(
            "../tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
        ) as f:
            yield json.load(f)

    return load_issues


if __name__ == "__main__":
    # get issue numbers to delete
    delete_issues = []
    if len(sys.argv) == 2:
        delete_issues = [int(p) for p in sys.argv[1].split(",")]

    def filter_issues(issue):
        return issue["number"] not in delete_issues

    p = dlt.pipeline("dlt_github_pipeline", destination="duckdb", dataset_name="github_3")
    github_source = github()
    info = p.run(github_source.load_issues.add_filter(filter_issues))
    print(info)
