import uuid
from pathlib import Path
from typing import List

import pytest
import sqlalchemy as sa
from sqlalchemy import event

from dlt.common.libs.sql_alchemy import MetaData
from dlt.sources.sql_database import sql_table

from tests.utils import get_test_storage_root


def _create_sqlite_db() -> str:
    """Creates an on-disk sqlite db with a single table and returns its connection url."""
    test_dir = Path(get_test_storage_root()) / f"sqlite_{uuid.uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    db_path = test_dir / "test.db"
    setup_engine = sa.create_engine(f"sqlite:///{db_path}")
    try:
        with setup_engine.begin() as conn:
            conn.execute(sa.text("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"))
            conn.execute(sa.text("INSERT INTO items VALUES (1, 'a'), (2, 'b')"))
    finally:
        setup_engine.dispose()
    return f"sqlite:///{db_path}"


@pytest.mark.parametrize(
    "defer_table_reflect",
    (False, True),
    ids=lambda x: "defer_table_reflect" + ("_true" if x else "_false"),
)
def test_reused_metadata_is_used_as_reflection_cache(defer_table_reflect: bool) -> None:
    """A reused MetaData must be honored as a reflection cache on both the eager and deferred
    paths: once a table is reflected, a subsequent run with the same MetaData reflects nothing.

    A schema is set so the table is stored under a schema-qualified key, the case where the
    deferred path used to miss the cache and re-reflect on every run.
    """
    credentials = _create_sqlite_db()
    metadata = MetaData(schema="main")

    reflected: List[str] = []
    event.listen(
        metadata,
        "column_reflect",
        lambda inspector, table, column_info: reflected.append(table.name),
    )

    def run() -> None:
        list(
            sql_table(
                credentials=credentials,
                table="items",
                schema="main",
                metadata=metadata,
                defer_table_reflect=defer_table_reflect,
            )
        )

    # cold run populates the cache
    run()
    assert reflected, "cold run should reflect the table"

    # warm run reuses the same metadata and must not reflect again
    reflected.clear()
    run()
    assert (
        reflected == []
    ), "warm run re-reflected the table; the reused MetaData was not used as a cache"
