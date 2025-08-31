import pytest
from sqlalchemy import create_engine

import dlt
from dlt.sources.sql_database import sql_table
from dlt.sources.sql_database.helpers import TableBackend

from tests.load.sources.sql_database.postgres_source import PostgresSourceDB
from tests.pipeline.utils import load_table_counts


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_load_sql_table_resource_incremental_end_value(
    sql_source_db: PostgresSourceDB,
    backend: TableBackend,
) -> None:
    conn_str = sql_source_db.credentials.to_native_representation()
    engine = create_engine(conn_str)

    # get min and max on id column from chat_message table
    with engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT MIN(id), MAX(id) FROM {}.chat_message".format(sql_source_db.schema)
        )
        min_id, max_id = result.fetchone()

        # Read all IDs from source table for verification
        result = conn.exec_driver_sql(
            "SELECT id FROM {}.chat_message ORDER BY id".format(sql_source_db.schema)
        )
        source_ids = [row[0] for row in result.fetchall()]

    # create 8 equal ranges from min and max id + 1
    step = (max_id - min_id + 1) // 8
    ranges = [{"min_id": min_id + i * step, "max_id": min_id + (i + 1) * step} for i in range(8)]
    # we won't load the last element to demonstrate incremental load
    ranges[-1]["max_id"] = max_id  # + 1

    # create list of tables via list comprehensions
    # NOTE: you cannot run many resources with the same name in the same extraction pipe
    #  so we rename them but send the data to the same table
    # NOTE: we can run resources in parallel because they are not using state
    all_tables = [
        sql_table(
            credentials=sql_source_db.credentials,
            schema=sql_source_db.schema,
            table="chat_message",
            backend=backend,
            incremental=dlt.sources.incremental(
                "id",
                initial_value=r["min_id"],
                end_value=r["max_id"],
                range_start="closed",
                range_end="open",
            ),
        )
        .with_name(f"chat_message_{i}")
        .apply_hints(table_name="chat_message")
        .parallelize()
        for i, r in enumerate(ranges)
    ]

    pipeline = dlt.pipeline(
        "test_load_sql_table_resource_incremental_end_value", destination="duckdb"
    )
    pipeline.run(all_tables)

    # no state was generated
    assert "sources" not in pipeline.state
    # The total should match the number of rows in the chat_message table
    assert (
        load_table_counts(pipeline)["chat_message"]
        == sql_source_db.table_infos["chat_message"]["row_count"] - 1
    )

    incremental_table = sql_table(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table="chat_message",
        backend=backend,
        incremental=dlt.sources.incremental(
            "id",
            initial_value=max_id - 1,  # don't skip min_id
            range_start="open",  # use open range to disable deduplication
        ),
    )
    pipeline.run(incremental_table)
    assert "sources" in pipeline.state
    assert pipeline.last_trace.last_normalize_info.row_counts["chat_message"] == 1

    # compare all pks just in case
    duckdb_ids = [pk[0] for pk in pipeline.dataset().chat_message["id"].fetchall()]
    assert sorted(duckdb_ids) == sorted(source_ids)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_load_sql_table_split_loading(
    sql_source_db: PostgresSourceDB,
    backend: TableBackend,
) -> None:
    conn_str = sql_source_db.credentials.to_native_representation()
    engine = create_engine(conn_str)

    # get min and max on id column from chat_message table
    with engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT MIN(id), MAX(id) FROM {}.chat_message".format(sql_source_db.schema)
        )
        min_id, max_id = result.fetchone()

        # Read all IDs from source table for verification
        result = conn.exec_driver_sql(
            "SELECT id FROM {}.chat_message ORDER BY id".format(sql_source_db.schema)
        )
        source_ids = [row[0] for row in result.fetchall()]

    pipeline = dlt.pipeline("test_load_sql_table_split_loading", destination="duckdb")

    # Set up incremental extraction with a defined batch size
    batch_size = (max_id - min_id + 1) // 8  # Similar to the original test's partitioning
    current_id = min_id - 1  # Start from before the minimum ID

    # Create the incremental table resource with row_order to ensure we don't miss rows
    incremental_table = sql_table(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table="chat_message",
        backend=backend,
        chunk_size=1000,
        incremental=dlt.sources.incremental(
            "id",
            initial_value=current_id,
            row_order="asc",  # Critical to set row_order when doing split loading
            range_start="open",  # use open range to disable deduplication
        ),
    )

    # Process data in batches using a loop
    # TODO: count rows loaded by each pipeline.run
    # rows_loaded = 0
    while pipeline.run(incremental_table.add_limit(batch_size)).has_data:
        pass

    print(pipeline.state)

    # The total should match the number of rows in the chat_message table
    assert (
        load_table_counts(pipeline)["chat_message"]
        == sql_source_db.table_infos["chat_message"]["row_count"]
    )

    # Compare all pks just in case
    duckdb_ids = [pk[0] for pk in pipeline.dataset().chat_message["id"].fetchall()]
    assert sorted(duckdb_ids) == sorted(source_ids)
