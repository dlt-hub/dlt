from __future__ import annotations

import pathlib
import textwrap
from typing import TYPE_CHECKING, Any

from dlt.common.storages import FileStorage
from dlt.common.pipeline import get_dlt_pipelines_dir

if TYPE_CHECKING:
    import pyarrow


def list_pipelines(pipelines_dir: str = None) -> list[str]:
    pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)

    try:
        pipelines = storage.list_folder_dirs(".", to_root=False)
    except Exception:
        pipelines = []

    return pipelines


def _load_pickle(file_path: pathlib.Path) -> Any:
    import pickle

    return pickle.loads(file_path.read_bytes())


def _load_json(file_path: pathlib.Path) -> Any:
    from dlt.common import json

    return json.loads(file_path.read_text())


def _load_jsonl(file_path: pathlib.Path) -> list[Any]:
    from dlt.common import json

    with file_path.open("r") as f:
        records = [json.loads(line.strip("\n")) for line in f.readlines()]
    return records


def _load_gzip(file_path: pathlib.Path) -> Any:
    return FileStorage.open_zipsafe_ro(str(file_path)).read()


def _load_parquet(file_path: pathlib.Path) -> pyarrow.Table:
    import pyarrow.parquet

    return pyarrow.parquet.read_table(file_path)


def _load_csv(file_path: pathlib.Path) -> pyarrow.Table:
    import pyarrow.csv

    return pyarrow.csv.read_csv(file_path)


def _load_insert_values_gzip(file_path: pathlib.Path) -> pyarrow.Table:
    import sqlglot
    import duckdb

    table_name = file_path.name.partition(".")[0]

    insert_statement = _load_gzip(file_path)
    # Remove the `E` prefix used by postgres-style quote escapes
    insert_statement = insert_statement.replace("E'", r"'")
    # For some reason, the INSERT statement is missing the table name
    # we can get this info from the file name
    insert_statement = insert_statement.replace("{}", f"{table_name} ")
    insert_expr = sqlglot.parse(insert_statement)[0]

    query = textwrap.dedent(f"""\
        CREATE TABLE {table_name} AS
        FROM ({insert_expr.expression.sql()})
        AS {insert_expr.this.sql()}
        """)

    con = duckdb.connect(":memory:")
    con.execute(query)
    arrow_table = con.execute(f"FROM {table_name}").arrow()
    con.close()  # release the memory
    return arrow_table


def _load_raw_text(file_path: pathlib.Path) -> str:
    return file_path.read_text()


def _load_raw_bytes(file_path: pathlib.Path) -> bytes:
    return file_path.read_bytes()


def _load_file(file_path: pathlib.Path) -> Any:
    try:
        if file_path.suffix == ".pickle":
            return _load_pickle(file_path)
        elif file_path.suffix == ".json":
            return _load_json(file_path)
        elif file_path.suffix == ".jsonl":
            return _load_jsonl(file_path)
        elif ".insert_values" in file_path.suffixes and file_path.suffix in (".gzip", ".gz"):
            return _load_insert_values_gzip(file_path)
        elif file_path.suffix in (".gzip", ".gz"):
            return _load_gzip(file_path)
        elif file_path.suffix == ".parquet":
            return _load_parquet(file_path)
        elif file_path.suffix == ".csv":
            return _load_csv(file_path)
        else:
            return _load_raw_text(file_path)
    except Exception:
        return _load_raw_bytes(file_path)
