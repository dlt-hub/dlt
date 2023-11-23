import os
from tempfile import mkdtemp

import dlt
from dlt.common.runners import Venv

from docs.examples.sources.singer_tap import tap

# create Venv with desired dependencies, in this case csv tap
# venv creation costs time so it should be created only once and reused

# here we use context manager to automatically delete venv after example was run
# the dependency is meltano version of csv tap
print(
    "Spawning virtual environment to run singer and installing csv tap from"
    " git+https://github.com/MeltanoLabs/tap-csv.git"
)
# WARNING: on MACOS you need to have working gcc to use tap-csv, otherwise dependency will not be installed
with Venv.create(mkdtemp(), ["git+https://github.com/MeltanoLabs/tap-csv.git"]) as venv:
    # prep singer config for tap-csv
    csv_tap_config = {
        "files": [
            {
                "entity": "annotations_202205",
                "path": os.path.abspath("examples/data/singer_taps/model_annotations.csv"),
                "keys": ["message id"],
            }
        ]
    }
    print("running tap-csv")
    tap_source = tap(venv, "tap-csv", csv_tap_config, "examples/data/singer_taps/csv_catalog.json")
    info = dlt.pipeline("meltano_csv", destination="postgres").run(
        tap_source, credentials="postgres://loader@localhost:5432/dlt_data"
    )
    print(info)
