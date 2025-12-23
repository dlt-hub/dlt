import random
from typing import Any, Dict, List, TypedDict, cast


import mimesis
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    func,
    Identity,
)
from sqlalchemy import text
from sqlalchemy.dialects.oracle import TIMESTAMP

from dlt.common.pendulum import pendulum, timedelta
from dlt.sources.credentials import ConnectionStringCredentials


class OracleSourceDB:
    def __init__(self, credentials: ConnectionStringCredentials, schema: str = None) -> None:
        self.credentials = credentials
        self.database_url = credentials.to_native_representation()
        # In Oracle, schema == user. Default to current user if not provided.
        self.schema = schema or (credentials.username or "DLT")
        self.engine = create_engine(self.database_url)
        self.metadata = MetaData(schema=self.schema)
        self.table_infos: Dict[str, OracleTableInfo] = {}

    def create_schema(self) -> None:
        """
        No-op for Oracle: schema maps to user. We're not creating and deleting the schema exery time,
        we're reusing the schema and dropping the tables every time instead.
        """
        pass

    def query(self, query: str) -> List[Dict[str, Any]]:
        with self.engine.begin() as conn:
            result = conn.execute(text(query))
            rows = []
            for row in result.fetchall():
                if hasattr(row, "_mapping"):
                    rows.append(dict(row._mapping))
                else:
                    rows.append(row._asdict())
            return rows

    def get_random_user_id(self) -> int:
        table = self.metadata.tables[f"{self.metadata.schema or ''}.app_user".lstrip(".")]
        query = f"SELECT id FROM {table.fullname}"
        with self.engine.begin() as conn:
            result = conn.execute(text(query)).fetchall()
        user_ids = [row[0] for row in result]
        return cast(int, random.choice(user_ids))

    def delete_row(self, conditions: str) -> None:
        table = self.metadata.tables[f"{self.metadata.schema or ''}.app_user".lstrip(".")]
        query = f"DELETE FROM {table.fullname} WHERE {conditions}"
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def update_row(self, updates: Dict[str, Any], conditions: str) -> None:
        table = self.metadata.tables[f"{self.metadata.schema or ''}.app_user".lstrip(".")]
        set_clause = ", ".join([f"{column} = :{column}" for column in updates.keys()])
        query = f"UPDATE {table.fullname} SET {set_clause} WHERE {conditions}"
        with self.engine.begin() as conn:
            conn.execute(text(query), updates)

    def create_tables(self, nullable: bool) -> None:
        from sqlalchemy import Boolean
        from sqlalchemy.dialects.oracle import BLOB, RAW

        Table(
            "app_user",
            self.metadata,
            Column("id", Integer(), Identity(), primary_key=True, nullable=False),
            Column("email", String(255), nullable=nullable, unique=True),
            Column("full_name", String(255), nullable=nullable),
            Column("first_name", String(255), nullable=nullable),
            Column("last_name", String(255), nullable=nullable),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=nullable,
                server_default=func.current_timestamp(),
            ),
            Column(
                "updated_at",
                DateTime(timezone=True),
                nullable=nullable,
                server_default=func.current_timestamp(),
            ),
            Column("some_integer", Integer(), nullable=nullable),
            Column("some_numeric", Numeric(10, 2), nullable=nullable),
            Column("some_boolean", Boolean(), nullable=nullable, server_default=text("TRUE")),
            Column("some_date", Date(), nullable=nullable),
            Column("some_timestamp_tz", TIMESTAMP(timezone=True), nullable=nullable),
            Column("some_timestamp_ntz", TIMESTAMP(timezone=False), nullable=nullable),
            Column("some_blob", BLOB, nullable=nullable),
        )
        self.metadata.create_all(bind=self.engine)

    def drop_tables(self) -> None:
        self.metadata.drop_all(bind=self.engine)

    def generate_users(self, n: int = 50) -> None:
        person = mimesis.Person()
        dt_gen = OracleIncrementingDate()
        table = self.metadata.tables[f"{(self.metadata.schema or '')}.app_user".lstrip(".")]
        info = self.table_infos.setdefault(
            "app_user",
            dict(row_count=0, ids=[], created_at=OracleIncrementingDate(), is_view=False),
        )
        all_rows = []
        for _ in range(n):
            created_at = next(dt_gen)
            updated_at = next(dt_gen)
            all_rows.append(
                dict(
                    email=person.email(unique=True),
                    full_name=person.full_name(),
                    first_name=person.first_name(),
                    last_name=person.last_name(),
                    created_at=created_at,
                    updated_at=updated_at,
                    some_integer=random.randint(1, 100),
                    some_numeric=round(random.uniform(0, 9999.99), 2),
                    some_boolean=random.choice([True, False]),
                    some_date=mimesis.Datetime().date(),
                    some_timestamp_tz=mimesis.Datetime().datetime(timezone="UTC"),
                    some_timestamp_ntz=mimesis.Datetime().datetime(),
                    some_blob=b"\x00\x01\x02",
                )
            )
        with self.engine.begin() as conn:
            conn.execute(table.insert(), all_rows)
        info["row_count"] += n


class OracleIncrementingDate:
    def __init__(self, start_value: pendulum.DateTime = None) -> None:
        self.current_value = start_value or pendulum.now()

    def __next__(self) -> pendulum.DateTime:
        value = self.current_value
        self.current_value += timedelta(seconds=random.randrange(0, 120))
        return value


class OracleTableInfo(TypedDict):
    row_count: int
    ids: List[int]
    created_at: OracleIncrementingDate
    is_view: bool
