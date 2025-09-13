import uuid
import random
from datetime import datetime, date, time
from typing import Any, Dict, List, TypedDict, cast
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, func
from sqlalchemy import schema as sqla_schema
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import mimesis

from dlt.common.pendulum import pendulum, timedelta
from dlt.common.utils import uniq_id
from dlt.sources.credentials import ConnectionStringCredentials


class MSSQLSourceDB:
    def __init__(self, credentials: ConnectionStringCredentials, schema: str = None) -> None:
        self.credentials = credentials
        self.master_database_url = credentials.to_native_representation()
        self.schema = schema or "my_dlt_source" + uniq_id()
        self.engine = create_engine(self.master_database_url)
        self.metadata = MetaData(schema=self.schema)
        self.table_infos: Dict[str, SQLServerTableInfo] = {}

    def _connect_to_database(self, database_name: str) -> None:
        self.credentials.database = database_name
        # create engine for new database
        self.engine.dispose()
        new_database_url = self.credentials.to_native_representation()
        self.engine = create_engine(new_database_url)
        self.metadata.bind = self.engine

    def create_database(self) -> None:
        database_name = "my_database" + uniq_id()
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"create database {database_name};"))
        self._connect_to_database(database_name)

    def create_schema(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(sqla_schema.CreateSchema(self.schema))

    def query(self, query: str) -> List[Dict[str, Any]]:
        with self.engine.begin() as conn:
            result = conn.execute(text(query))
            return [row._asdict() for row in result.fetchall()]

    def get_random_user_id(self) -> int:
        query = f"SELECT id FROM {self.schema}.app_user"
        with self.engine.begin() as conn:
            result = conn.execute(text(query)).fetchall()
        user_ids = [row[0] for row in result]
        return cast(int, random.choice(user_ids))

    def delete_row(self, conditions: str) -> None:
        query = f"DELETE FROM {self.schema}.app_user WHERE {conditions}"
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def update_row(self, updates: Dict[str, Any], conditions: str) -> None:
        set_clause = ", ".join([f"{column} = :{column}" for column in updates.keys()])
        query = f"UPDATE {self.schema}.app_user SET {set_clause} WHERE {conditions}"
        with self.engine.begin() as conn:
            conn.execute(text(query), updates)

    def create_tables(self, nullable: bool) -> None:
        from sqlalchemy import Boolean, Numeric, Date, Time
        from sqlalchemy.dialects.mssql import (
            UNIQUEIDENTIFIER,
            XML,
            VARBINARY,
            DATETIME2,
            DATETIMEOFFSET,
            SMALLDATETIME,
        )

        Table(
            "app_user",
            self.metadata,
            Column("id", Integer(), primary_key=True, nullable=False, autoincrement=True),
            Column("email", String(255), nullable=nullable, unique=True),
            Column("full_name", String(255), nullable=nullable),
            Column("first_name", String(255), nullable=nullable),
            Column("last_name", String(255), nullable=nullable),
            Column(
                "created_at", DateTime(timezone=True), nullable=nullable, server_default=func.now()
            ),
            Column(
                "updated_at", DateTime(timezone=True), nullable=nullable, server_default=func.now()
            ),
            Column("some_integer", Integer(), nullable=nullable),
            Column("some_numeric", Numeric(10, 2), nullable=nullable),
            Column("some_bit", Boolean(), nullable=nullable),  # maps to BIT
            Column("some_date", Date(), nullable=nullable),
            Column("some_datetime2", DATETIME2(), nullable=nullable),
            Column("some_datetimeoffset", DATETIMEOFFSET(), nullable=nullable),
            Column("some_smalldatetime", SMALLDATETIME(), nullable=nullable),
            Column("some_time", Time(), nullable=nullable),
            Column(
                "some_uniqueidentifier",
                UNIQUEIDENTIFIER(),
                nullable=nullable,
                server_default=text("NEWID()"),
            ),
            # Column("some_xml", XML(), nullable=nullable),
            Column("some_binary", VARBINARY(None), nullable=nullable),
        )
        self.metadata.create_all(bind=self.engine)

    def generate_users(self, n: int = 50) -> None:
        person = mimesis.Person()
        table = self.metadata.tables[f"{self.schema}.app_user"]
        info = self.table_infos.setdefault(
            "app_user",
            dict(row_count=0, ids=[], created_at=SQLServerIncrementingDate(), is_view=False),
        )
        dt = info["created_at"]
        all_rows = [
            dict(
                email=person.email(unique=True),
                full_name=person.full_name(),
                first_name=person.first_name(),
                last_name=person.last_name(),
                created_at=next(dt),
                updated_at=next(dt),
                some_integer=random.randint(1, 100),
                some_numeric=round(random.uniform(0, 9999.99), 2),
                some_bit=random.choice([True, False]),
                some_date=mimesis.Datetime().date(),
                some_datetime2=mimesis.Datetime().datetime(),
                # For datetimeoffset, we can just store the same datetime or rely on default
                some_datetimeoffset=mimesis.Datetime().datetime(timezone="UTC"),
                some_smalldatetime=mimesis.Datetime().datetime(),
                some_time=time(12, 0),
                some_uniqueidentifier=str(uuid.uuid4()),
                some_xml="<root><element>value</element></root>",
                some_json_array='["apple","banana"]',
                some_binary=b"\x00\x01\x02",
            )
            for _ in range(n)
        ]
        with self.engine.begin() as conn:
            conn.execute(table.insert(), all_rows)
        info["row_count"] += n


class SQLServerIncrementingDate:
    def __init__(self, start_value: pendulum.DateTime = None) -> None:
        self.current_value = start_value or pendulum.now()

    def __next__(self) -> pendulum.DateTime:
        value = self.current_value
        self.current_value += timedelta(seconds=random.randrange(0, 120))
        return value


class SQLServerTableInfo(TypedDict):
    row_count: int
    ids: List[int]
    created_at: SQLServerIncrementingDate
    is_view: bool
