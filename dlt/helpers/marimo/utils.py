import pathlib
import textwrap
from typing import Any

from dlt.common.storages import FileStorage
from dlt.common.pipeline import get_dlt_pipelines_dir


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


def _load_json(file_path: pathlib.Path) -> dict:
    import json
    return json.loads(file_path.read_bytes())


def _load_jsonl(file_path: pathlib.Path) -> list[dict]:
    records = []
    with file_path.open() as f:
        f.readlines()
    return records


def _load_gzip(file_path: pathlib.Path) -> str:
    return FileStorage.open_zipsafe_ro(str(file_path)).read()


def _load_parquet(file_path: pathlib.Path) -> Any:
    import pyarrow.parquet as pq
    return pq.read_table(file_path)


def _load_insert_values_gzip(file_path: pathlib.Path):
    import sqlglot
    import duckdb

    table_name = file_path.name.partition(".")[0]
    
    insert_statement = _load_gzip(file_path)
    # Remove the `E` prefix used by postgres-style quote escapes
    insert_statement = insert_statement.replace("E\'", r"'")
    # For some reason, the INSERT statement is missing the table name
    # we can get this info from the file name
    insert_statement = insert_statement.replace("{}", f"{table_name} ")
    insert_expr = sqlglot.parse(insert_statement)[0]

    query = textwrap.dedent(
        f"""\
        CREATE TABLE {table_name} AS
        FROM ({insert_expr.expression.sql()})
        AS {insert_expr.this.sql()}
        """
    )

    con = duckdb.connect(":memory:")
    con.execute(query)
    arrow_table = con.execute(f"FROM {table_name}").arrow()
    con.close()  # release the memory
    return arrow_table


def _file_loader(file_path: pathlib.Path) -> Any:
    try:
        if file_path.suffix == ".pickle":
            return _load_pickle(file_path)
        elif file_path.suffix == ".json":
            return _load_json(file_path)
        elif file_path.suffix == ".jsonl":
            return _load_jsonl(file_path)
        elif all(suffix in file_path.suffixes for suffix in [".insert_values", ".gz"]):
            return _load_insert_values_gzip(file_path)
        elif file_path.suffix in (".gzip", ".gz"):
            return _load_gzip(file_path)
        else:
            return file_path.read_text()
    except Exception:
        return file_path.read_text()