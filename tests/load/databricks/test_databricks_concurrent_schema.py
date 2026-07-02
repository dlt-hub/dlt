from tests.utils import skip_if_not_active

skip_if_not_active("databricks")

import threading
from typing import Any, Callable, Dict, Generator, Iterable, List, Tuple, cast
from unittest import mock

import pytest

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.schema.utils import new_table
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.destinations.impl.databricks.databricks import DatabricksClient

from tests.load.databricks.test_databricks_table_builder import create_client
from tests.load.utils import empty_schema, yield_client

pytestmark = pytest.mark.essential

TABLE_NAME = "event_test_table"
COL_A: TTableSchemaColumns = {"col_a": {"name": "col_a", "data_type": "text"}}
ALL_COLS: TTableSchemaColumns = {
    **COL_A,
    "col_b": {"name": "col_b", "data_type": "bigint"},
}


def _mocked_client(empty_schema: Schema) -> DatabricksClient:
    client = create_client(empty_schema, create_indexes=False)
    client.schema.update_table(
        new_table(
            TABLE_NAME,
            columns=[
                {"name": "col_a", "data_type": "text"},
                {"name": "col_b", "data_type": "bigint"},
            ],
        )
    )
    return client


def test_schema_update_converges_under_concurrent_modification(empty_schema: Schema) -> None:
    client = _mocked_client(empty_schema)
    # a concurrent load created the table with col_a only, then another one conflicts on ALTER
    client.get_storage_tables = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=[
            [(TABLE_NAME, {})],
            [(TABLE_NAME, dict(COL_A))],
            [(TABLE_NAME, dict(COL_A))],
            [(TABLE_NAME, dict(ALL_COLS))],
        ]
    )
    calls: List[str] = []

    def execute_many(statements: List[str], *args: str) -> None:
        calls.append("execute:" + ";".join(statements))
        if len(calls) == 2:
            raise DatabaseTerminalException(Exception("[DELTA_METADATA_CHANGED] concurrent update"))

    client.sql_client.execute_many = mock.MagicMock(side_effect=execute_many)  # type: ignore[method-assign]
    client.get_stored_schema_by_hash = mock.MagicMock(return_value=None)  # type: ignore[method-assign]
    client._update_schema_in_storage = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=lambda schema: calls.append("store")
    )

    client._execute_schema_update_sql([TABLE_NAME])

    executed = [c for c in calls if c.startswith("execute")]
    assert len(executed) == 3
    assert executed[0].startswith("execute:CREATE TABLE IF NOT EXISTS")
    assert "ADD COLUMN" in executed[1] and "`col_b`" in executed[1]
    assert "ADD COLUMN" in executed[2] and "`col_b`" in executed[2]
    assert calls[-1] == "store"


def test_schema_update_raises_on_other_errors(empty_schema: Schema) -> None:
    client = _mocked_client(empty_schema)
    client.get_storage_tables = mock.MagicMock(return_value=[(TABLE_NAME, {})])  # type: ignore[method-assign]
    client.sql_client.execute_many = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=DatabaseTerminalException(Exception("[PARSE_SYNTAX_ERROR] boom"))
    )
    client._update_schema_in_storage = mock.MagicMock()  # type: ignore[method-assign]

    with pytest.raises(DatabaseTerminalException):
        client._execute_schema_update_sql([TABLE_NAME])

    assert client.sql_client.execute_many.call_count == 1
    client._update_schema_in_storage.assert_not_called()


def test_schema_update_exhausts_attempts(
    empty_schema: Schema, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(DatabricksClient, "SCHEMA_UPDATE_MAX_ATTEMPTS", 3)
    client = _mocked_client(empty_schema)
    client.get_storage_tables = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=lambda table_names: [(TABLE_NAME, {})]
    )
    client.sql_client.execute_many = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=DatabaseTerminalException(Exception("[TABLE_OR_VIEW_ALREADY_EXISTS] raced"))
    )
    client._update_schema_in_storage = mock.MagicMock()  # type: ignore[method-assign]

    with pytest.raises(DatabaseTerminalException):
        client._execute_schema_update_sql([TABLE_NAME])

    assert client.sql_client.execute_many.call_count == 3
    client._update_schema_in_storage.assert_not_called()


def test_schema_update_skips_version_row_stored_by_concurrent_load(empty_schema: Schema) -> None:
    client = _mocked_client(empty_schema)
    client.get_storage_tables = mock.MagicMock(return_value=[(TABLE_NAME, dict(ALL_COLS))])  # type: ignore[method-assign]
    client.sql_client.execute_many = mock.MagicMock()  # type: ignore[method-assign]
    client.get_stored_schema_by_hash = mock.MagicMock(return_value=object())  # type: ignore[method-assign]
    client._update_schema_in_storage = mock.MagicMock()  # type: ignore[method-assign]

    client._execute_schema_update_sql([TABLE_NAME])

    client.sql_client.execute_many.assert_not_called()
    client._update_schema_in_storage.assert_not_called()


N_WORKERS = 4


TClientGen = Generator[DatabricksClient, None, None]


def _new_client(dataset_name: str) -> Tuple[DatabricksClient, TClientGen]:
    # connection is opened later, inside the worker thread
    gen = cast(
        TClientGen, yield_client("databricks", dataset_name=dataset_name, enter_client=False)
    )
    return next(gen), gen


def _sync_after_storage_read(client: DatabricksClient, barrier: threading.Barrier) -> None:
    original = client.get_storage_tables
    state = {"synced": False}

    def wrapper(table_names: Iterable[str]) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        result = list(original(table_names))
        if not state["synced"]:
            state["synced"] = True
            try:
                barrier.wait(timeout=120)
            except threading.BrokenBarrierError:
                pass
        return result

    client.get_storage_tables = wrapper  # type: ignore[method-assign]


def _run_concurrent_update(
    clients_gens: List[Tuple[DatabricksClient, TClientGen]],
    mutate: Callable[[Schema, int], Any],
) -> Dict[int, BaseException]:
    barrier = threading.Barrier(len(clients_gens))
    errors: Dict[int, BaseException] = {}

    def run(i: int, client: DatabricksClient) -> None:
        try:
            with client:
                mutate(client.schema, i)
                client.schema._bump_version()
                _sync_after_storage_read(client, barrier)
                client.update_stored_schema()
        except Exception as e:
            errors[i] = e

    threads = [threading.Thread(target=run, args=(i, c)) for i, (c, _) in enumerate(clients_gens)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def _drop_dataset(dataset_name: str) -> None:
    client, gen = _new_client(dataset_name)
    try:
        with client:
            client.sql_client.drop_dataset()
    except Exception:
        pass
    finally:
        gen.close()


def test_concurrent_create_table_converges() -> None:
    dataset = "conc_create_" + uniq_id()
    raced = "raced_" + uniq_id()
    columns: List[TColumnSchema] = [
        {"name": "id", "data_type": "bigint"},
        {"name": "val", "data_type": "text"},
    ]

    setup, setup_gen = _new_client(dataset)
    with setup:
        setup.initialize_storage()
        setup.update_stored_schema()
    setup_gen.close()

    clients_gens = [_new_client(dataset) for _ in range(N_WORKERS)]
    try:
        errors = _run_concurrent_update(
            clients_gens, lambda schema, i: schema.update_table(new_table(raced, columns=columns))
        )
        assert errors == {}, "concurrent CREATE raced: " + str(
            {i: str(e)[:140] for i, e in errors.items()}
        )
        verify, verify_gen = _new_client(dataset)
        with verify:
            _, storage_columns = list(verify.get_storage_tables([raced]))[0]
        verify_gen.close()
        assert {"id", "val"} <= set(storage_columns)
    finally:
        for _, gen in clients_gens:
            gen.close()
        _drop_dataset(dataset)


def test_concurrent_divergent_add_column_converges() -> None:
    dataset = "conc_addcol_" + uniq_id()
    raced = "raced_" + uniq_id()

    setup, setup_gen = _new_client(dataset)
    with setup:
        setup.initialize_storage()
        setup.schema.update_table(new_table(raced, columns=[{"name": "id", "data_type": "bigint"}]))
        setup.schema._bump_version()
        setup.update_stored_schema()
    setup_gen.close()

    clients_gens = [_new_client(dataset) for _ in range(N_WORKERS)]
    try:
        errors = _run_concurrent_update(
            clients_gens,
            lambda schema, i: schema.update_table(
                new_table(raced, columns=[{"name": f"col_{i}", "data_type": "bigint"}])
            ),
        )
        assert errors == {}, "divergent ADD COLUMN raced: " + str(
            {i: str(e)[:140] for i, e in errors.items()}
        )
        verify, verify_gen = _new_client(dataset)
        with verify:
            _, storage_columns = list(verify.get_storage_tables([raced]))[0]
        verify_gen.close()
        expected = {"id"} | {f"col_{i}" for i in range(N_WORKERS)}
        assert expected <= set(storage_columns)
    finally:
        for _, gen in clients_gens:
            gen.close()
        _drop_dataset(dataset)
