import pytest
from sqlalchemy import create_engine, text

import dlt
from dlt.common import pendulum
from dlt.sources.sql_database import sql_table
from dlt.sources.sql_database.helpers import TableBackend

from tests.load.sources.sql_database.postgres_source import PostgresSourceDB
from tests.pipeline.utils import load_table_counts


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_load_sql_table_resource_incremental_end_value(
    postgres_db: PostgresSourceDB,
    backend: TableBackend,
) -> None:
    conn_str = postgres_db.credentials.to_native_representation()
    engine = create_engine(conn_str)

    # get min and max on id column from chat_message table
    with engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT MIN(id), MAX(id) FROM {}.chat_message".format(postgres_db.schema)
        )
        min_id, max_id = result.fetchone()

        # Read all IDs from source table for verification
        result = conn.exec_driver_sql(
            "SELECT id FROM {}.chat_message ORDER BY id".format(postgres_db.schema)
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
            credentials=postgres_db.credentials,
            schema=postgres_db.schema,
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
        == postgres_db.table_infos["chat_message"]["row_count"] - 1
    )

    incremental_table = sql_table(
        credentials=postgres_db.credentials,
        schema=postgres_db.schema,
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
def test_load_sql_table_partitioned(
    postgres_db: PostgresSourceDB,
    backend: TableBackend,
) -> None:
    conn_str = postgres_db.credentials.to_native_representation()
    engine = create_engine(conn_str)

    # update all messages in chat_message so `updated_at` belongs to 4 different days that we know in advance
    today = pendulum.now().start_of("day")
    num_days = 4

    # create date ranges in a loop
    days = [today.subtract(days=i) for i in range(num_days - 1, -1, -1)]
    date_ranges = [
        {"start": days[i], "end": days[i + 1] if i < num_days - 1 else days[i].add(days=1)}
        for i in range(num_days)
    ]

    # Get total count and message IDs to verify later
    with engine.begin() as conn:
        result = conn.exec_driver_sql(
            "SELECT COUNT(*), array_agg(id) FROM {}.chat_message".format(postgres_db.schema)
        )
        total_count, message_ids = result.fetchone()

        # Update the chat_message records to distribute across days
        case_clauses = []
        case_params = {}

        for i, day_range in enumerate(date_ranges):
            day_param = f"day{i}"
            case_clauses.append(f"WHEN id % {num_days} = {i} THEN :{day_param}")
            case_params[day_param] = day_range["start"]

        case_statement = " ".join(case_clauses)

        conn.execute(
            text(f"""
            UPDATE {postgres_db.schema}.chat_message
            SET updated_at = CASE
                {case_statement}
            END
            """),
            case_params,
        )

        # verify the distribution
        result = conn.exec_driver_sql(f"""
            SELECT date_trunc('day', updated_at) as day, COUNT(*)
            FROM {postgres_db.schema}.chat_message
            GROUP BY day
            ORDER BY day
            """)
        day_counts = {str(row[0]): row[1] for row in result.fetchall()}
        print(day_counts)
        assert len(day_counts) == 4

    # run dlt pipeline 4 times with incremental set to particular days (via initial_value and end_value)
    pipeline = dlt.pipeline("test_load_sql_table_partitioned", destination="duckdb")

    # load each date range separately
    for _, date_range in enumerate(date_ranges):
        incremental_table = sql_table(
            credentials=postgres_db.credentials,
            schema=postgres_db.schema,
            table="chat_message",
            backend=backend,
            incremental=dlt.sources.incremental(
                "updated_at",
                initial_value=date_range["start"],
                end_value=date_range["end"],
                range_start="closed",
                range_end="open",
            ),
        )

        print(pipeline.run(incremental_table))
        print(pipeline.last_trace.last_normalize_info)

    # make sure all records are present
    print(load_table_counts(pipeline))
    loaded_count = load_table_counts(pipeline)["chat_message"]
    assert loaded_count == total_count

    # Check that we have all the message IDs
    duckdb_ids = [pk[0] for pk in pipeline.dataset().chat_message[["id"]].fetchall()]
    assert sorted(duckdb_ids) == sorted(message_ids)

    # update one message updated_at to now()
    current_time = pendulum.now().add(days=1)
    with engine.begin() as conn:
        # Update the first message to have the current time
        first_id = conn.exec_driver_sql(
            f"SELECT MIN(id) FROM {postgres_db.schema}.chat_message"
        ).scalar_one()

        conn.execute(
            text(
                f"UPDATE {postgres_db.schema}.chat_message SET updated_at = :current_time WHERE"
                " id = :id"
            ),
            {"current_time": current_time, "id": first_id},
        )

    # run pipeline incrementally with the max day as initial_value and open range
    incremental_table = sql_table(
        credentials=postgres_db.credentials,
        schema=postgres_db.schema,
        table="chat_message",
        backend=backend,
        incremental=dlt.sources.incremental(
            "updated_at",
            initial_value=date_ranges[-1]["end"],  # Use the last day as the starting point
            range_start="closed",  # Include records from the last day
        ),
    )

    pipeline.run(incremental_table)
    print(pipeline.last_trace.last_normalize_info)

    # verify that we loaded the updated record
    # should have loaded only 1 record - the one we just updated
    assert pipeline.last_trace.last_normalize_info.row_counts["chat_message"] == 1

    # verify the total record count is still correct (one duplicate, or use merge loading)
    total_duckdb_count = len(pipeline.dataset().chat_message["id"].fetchall())
    assert total_duckdb_count == total_count + 1

    pipeline.run(incremental_table)
    assert "chat_message" not in pipeline.last_trace.last_normalize_info.row_counts


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_load_sql_table_split_loading(
    postgres_db: PostgresSourceDB,
    backend: TableBackend,
) -> None:
    conn_str = postgres_db.credentials.to_native_representation()
    engine = create_engine(conn_str)

    # get min and max on id column from chat_message table
    with engine.connect() as conn:
        # read all IDs from source table for verification
        result = conn.exec_driver_sql(
            "SELECT id FROM {}.chat_message ORDER BY id".format(postgres_db.schema)
        )
        source_ids = [row[0] for row in result.fetchall()]

    pipeline = dlt.pipeline("test_load_sql_table_split_loading", destination="duckdb")

    # create the incremental table resource with row_order to ensure we don't miss rows
    incremental_table = sql_table(
        credentials=postgres_db.credentials,
        schema=postgres_db.schema,
        table="chat_message",
        backend=backend,
        chunk_size=1000,  # in production use large chunk size
        incremental=dlt.sources.incremental(
            "id",
            row_order="asc",  # critical to set row_order when doing split loading
            range_start="open",  # use open range to disable deduplication
        ),
    )

    # we'll load 2 pages, 1000 rows max on each run
    while not pipeline.run(incremental_table.add_limit(2)).is_empty:
        pass

    # the total should match the number of rows in the chat_message table
    assert (
        load_table_counts(pipeline)["chat_message"]
        == postgres_db.table_infos["chat_message"]["row_count"]
    )

    # compare all pks just in case
    duckdb_ids = [pk[0] for pk in pipeline.dataset().chat_message["id"].fetchall()]
    assert sorted(duckdb_ids) == sorted(source_ids)
