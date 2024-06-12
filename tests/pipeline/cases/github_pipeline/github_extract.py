import sys

import dlt

from github_pipeline import github  # type: ignore[import-not-found]

if __name__ == "__main__":
    p = dlt.pipeline("dlt_github_pipeline", destination="duckdb", dataset_name="github_3")
    github_source = github()
    if len(sys.argv) > 1:
        # load only N issues
        limit = int(sys.argv[1])
        github_source.add_limit(limit)
    info = p.extract(github_source)
    print(info)
