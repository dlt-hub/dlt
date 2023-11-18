import sys

import dlt

from dlt.common import json

@dlt.source(root_key=True)
def github():

    @dlt.resource(table_name="issues", write_disposition="merge", primary_key="id", merge_key=("node_id", "url"))
    def load_issues():
        # we should be in TEST_STORAGE folder
        with open("../tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8") as f:
            yield from json.load(f)

    return load_issues


if __name__ == "__main__":
    p = dlt.pipeline("dlt_github_pipeline", destination="duckdb", dataset_name="github_3", full_refresh=False)
    github_source = github()
    if len(sys.argv) > 1:
        # load only N issues
        limit = int(sys.argv[1])
        github_source.add_limit(limit)
    info = p.run(github_source)
    print(info)
