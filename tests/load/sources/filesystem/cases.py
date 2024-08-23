import os

from tests.load.utils import WITH_GDRIVE_BUCKETS

TESTS_BUCKET_URLS = [
    os.path.join(bucket_url, "standard_source/samples")
    for bucket_url in WITH_GDRIVE_BUCKETS
    if not bucket_url.startswith("memory")
]

GLOB_RESULTS = [
    {
        "glob": None,
        "relative_paths": ["sample.txt"],
    },
    {
        "glob": "*/*",
        "relative_paths": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
            "gzip/taxi.csv.gz",
            "jsonl/mlb_players.jsonl",
            "parquet/mlb_players.parquet",
        ],
    },
    {
        "glob": "**/*.csv",
        "relative_paths": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
            "met_csv/A801/A881_20230920.csv",
            "met_csv/A803/A803_20230919.csv",
            "met_csv/A803/A803_20230920.csv",
        ],
    },
    {
        "glob": "*/*.csv",
        "relative_paths": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "csv/*",
        "relative_paths": [
            "csv/freshman_kgs.csv",
            "csv/freshman_lbs.csv",
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "csv/mlb*",
        "relative_paths": [
            "csv/mlb_players.csv",
            "csv/mlb_teams_2012.csv",
        ],
    },
    {
        "glob": "*",
        "relative_paths": ["sample.txt"],
    },
]
