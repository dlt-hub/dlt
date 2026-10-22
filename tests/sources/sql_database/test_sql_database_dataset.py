import uuid
from pathlib import Path
from typing import Any, Dict, Iterator

import pytest
import sqlalchemy as sa

import dlt
from dlt.common.typing import TDataItem
from dlt.sources.sql_database import (
    BaseTableLoader,
    TableBackend,
    TableLoader,
    sql_database,
)
from dlt.sources.sql_database.dataset import sql_database_dataset

from tests.utils import get_test_storage_root


def _create_sqlite_db() -> str:
    test_dir = Path(get_test_storage_root()) / f"sqlite_{uuid.uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    db_path = test_dir / "test.db"
    setup_engine = sa.create_engine(f"sqlite:///{db_path}")
    try:
        with setup_engine.begin() as conn:
            conn.execute(
                sa.text('CREATE TABLE "Items" ("ItemId" INTEGER PRIMARY KEY, "Value" TEXT)')
            )
            conn.execute(sa.text("INSERT INTO \"Items\" VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
    finally:
        setup_engine.dispose()
    return f"sqlite:///{db_path}"


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
def test_table_loader_load_query(backend: TableBackend) -> None:
    engine = sa.create_engine(_create_sqlite_db())
    loader = TableLoader(engine, backend, None, {}, chunk_size=2)
    # passed columns do not modify the loader columns
    list(loader.load_query(sa.text('SELECT "ItemId" FROM "Items"'), columns={}))
    assert loader.columns == {}

    items = list(
        loader.load_query(sa.text('SELECT "ItemId", "Value" FROM "Items" ORDER BY "ItemId"'))
    )
    assert len(items) == 2
    assert sum(len(item) for item in items) == 3
    engine.dispose()


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
def test_sql_database_dataset_sqlite(backend: TableBackend) -> None:
    source = sql_database(credentials=_create_sqlite_db(), backend=backend)
    dataset = sql_database_dataset(source)
    # sqlite tables live in the default `main` schema
    assert dataset.dataset_name == "main"
    assert "Items" in dataset.tables
    assert dataset.table("Items").select("Value").fetchall() == [("a",), ("b",), ("c",)]
    assert dataset('SELECT max("ItemId") AS m FROM "Items"').fetchone() == (3,)


def test_sql_database_dataset_errors() -> None:
    credentials = _create_sqlite_db()

    # deferred reflection gives no schema
    source = sql_database(credentials=credentials, table_names=["Items"], defer_table_reflect=True)
    with pytest.raises(ValueError, match="defer_table_reflect"):
        sql_database_dataset(source)

    # resources that are not sql tables
    @dlt.source
    def other_source():
        return dlt.resource([1, 2], name="numbers")

    with pytest.raises(ValueError, match="numbers"):
        sql_database_dataset(other_source())


def test_sql_database_dataset_custom_loader_without_load_query() -> None:
    class LoadRowsOnlyLoader(BaseTableLoader):
        def load_rows(self, backend_kwargs: Dict[str, Any] = None) -> Iterator[TDataItem]:
            with self.engine.connect() as conn:
                yield [dict(row._mapping) for row in conn.execute(self.make_query())]

    source = sql_database(credentials=_create_sqlite_db(), table_loader_class=LoadRowsOnlyLoader)
    # extraction still works
    assert len(list(source)) == 3

    dataset = sql_database_dataset(source)
    with pytest.raises(NotImplementedError):
        dataset.table("Items").fetchall()
