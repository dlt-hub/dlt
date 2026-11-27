import decimal
import random
from datetime import datetime, timezone, tzinfo
from typing import Any, Dict, List, Optional, Tuple, TypedDict, cast
from zoneinfo import ZoneInfo

from typing_extensions import NotRequired


import mimesis
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
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
from sqlalchemy.dialects.oracle import BINARY_DOUBLE, BINARY_FLOAT, NUMBER, TIMESTAMP

from dlt.common.pendulum import pendulum, timedelta
from dlt.sources.credentials import ConnectionStringCredentials

from tests.load.sources.sql_database.utils import cursor_datetime


def _offset(hours: int, minutes: int = 0) -> timezone:
    sign = -1 if hours < 0 else 1
    return timezone(sign * timedelta(hours=abs(hours), minutes=minutes))


TZ_PROBE_DDL = """
CREATE TABLE {table} (
    id NUMBER(10) PRIMARY KEY,
    ts_naive TIMESTAMP(6),
    ts_tz TIMESTAMP(6) WITH TIME ZONE,
    ts9_tz TIMESTAMP(9) WITH TIME ZONE,
    ts_tz_named TIMESTAMP(6) WITH TIME ZONE,
    ts_ltz TIMESTAMP(6) WITH LOCAL TIME ZONE,
    d DATE
)
"""

# (id, wall clock, offset, region) from which every `tz_probe` column value is derived
TZ_PROBE_WALL_CLOCKS: List[Tuple[int, datetime, tzinfo, tzinfo]] = [
    (1, datetime(2024, 7, 2, 13, 45, 10, 123456), _offset(2), ZoneInfo("Europe/Warsaw")),
    (2, datetime(2024, 1, 2, 23, 30), _offset(-5), ZoneInfo("America/New_York")),
    (3, datetime(1950, 6, 15, 8, 0), _offset(5, 45), ZoneInfo("Asia/Kolkata")),
    (4, datetime(2024, 12, 31, 23, 59, 59, 999999), _offset(-9, 30), ZoneInfo("Pacific/Marquesas")),
    (5, datetime(2000, 2, 29, 12, 0, 0, 1), _offset(14), ZoneInfo("Pacific/Kiritimati")),
    (6, datetime(1969, 12, 31, 23, 59, 59, 500000), timezone.utc, ZoneInfo("UTC")),
    (7, datetime(2024, 7, 2, 13, 45, 10, 123456), _offset(-7), ZoneInfo("America/Los_Angeles")),
    (8, datetime(2024, 1, 1, 0, 30), _offset(1), ZoneInfo("Europe/London")),
]
# digits past microseconds written to `ts9_tz` only, python cannot hold them and drivers drop them
TZ_PROBE_NANOSECONDS = {7: "789"}

TZ_PROBE_COLUMNS = ("ts_naive", "ts_tz", "ts9_tz", "ts_tz_named", "ts_ltz", "d")


def oracle_timestamp_literal(value: datetime, extra_digits: str = "") -> str:
    """Renders `value` as an Oracle TIMESTAMP literal with its own offset or region name."""
    literal = f"{value:%Y-%m-%d %H:%M:%S.%f}{extra_digits}"
    if isinstance(value.tzinfo, ZoneInfo):
        literal += f" {value.tzinfo.key}"
    elif value.tzinfo is not None:
        literal += f" {value.isoformat()[-6:]}"
    return f"TIMESTAMP '{literal}'"


def as_naive_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Converts aware datetimes to naive UTC, naive ones are UTC in `dlt` and stay as they are."""
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


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
            Column("some_boolean", Boolean(), nullable=nullable, server_default=text("TRUE")),
            Column("some_date", Date(), nullable=nullable),
            Column("some_timestamp_tz", TIMESTAMP(timezone=True), nullable=nullable),
            Column("some_timestamp_ntz", TIMESTAMP(timezone=False), nullable=nullable),
            Column(
                "some_timestamp_ltz",
                # `local_timezone` is SQLAlchemy 2.0+, type stubs are still 1.4
                TIMESTAMP(local_timezone=True),  # type: ignore[call-arg]
                nullable=nullable,
            ),
            Column("some_blob", BLOB, nullable=nullable),
            Column("some_integer", Integer(), nullable=nullable),
            Column("some_number", NUMBER(), nullable=nullable),
            Column("some_number_precision", NUMBER(precision=10), nullable=nullable),
            Column("some_number_precision_scale", NUMBER(precision=10, scale=2), nullable=nullable),
            Column("some_float", Float(), nullable=nullable),
            Column("some_binary_float", BINARY_FLOAT(), nullable=nullable),
            Column("some_binary_double", BINARY_DOUBLE(), nullable=nullable),
        )
        self.metadata.create_all(bind=self.engine)
        self.create_tz_probe()

    def drop_tables(self) -> None:
        self.metadata.drop_all(bind=self.engine)
        self._drop_tz_probe()

    @property
    def tz_probe_table(self) -> str:
        return f"{self.schema}.tz_probe"

    def _drop_tz_probe(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
                    + self.tz_probe_table
                    + "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
                )
            )

    def create_tz_probe(self) -> None:
        """Creates `tz_probe` with every Oracle datetime type, written as SQL literals."""
        self._drop_tz_probe()
        rows: Dict[int, Dict[str, Optional[datetime]]] = {0: dict.fromkeys(TZ_PROBE_COLUMNS)}
        for id_, wall, offset, region in TZ_PROBE_WALL_CLOCKS:
            with_offset = wall.replace(tzinfo=offset)
            rows[id_] = dict(
                ts_naive=wall,
                ts_tz=with_offset,
                ts9_tz=with_offset,
                ts_tz_named=wall.replace(tzinfo=region),
                ts_ltz=with_offset,
                d=wall.replace(microsecond=0),
            )
        with self.engine.begin() as conn:
            conn.execute(text(TZ_PROBE_DDL.format(table=self.tz_probe_table)))
            for id_, row in rows.items():
                literals = [
                    (
                        "NULL"
                        if value is None
                        else oracle_timestamp_literal(
                            value, TZ_PROBE_NANOSECONDS.get(id_, "") if col == "ts9_tz" else ""
                        )
                    )
                    for col, value in row.items()
                ]
                # Oracle DATE has no TIMESTAMP literal
                literals[-1] = f"CAST({literals[-1]} AS DATE)"
                conn.execute(
                    text(f"INSERT INTO {self.tz_probe_table} VALUES ({id_}, {', '.join(literals)})")
                )
        self.table_infos["tz_probe"] = dict(
            row_count=len(rows), ids=sorted(rows), is_view=False, rows=rows
        )

    def tz_probe_source(self, column: str) -> Dict[int, Optional[datetime]]:
        """Python values `column` was written from, as naive UTC."""
        return {
            id_: as_naive_utc(row[column])
            for id_, row in self.table_infos["tz_probe"]["rows"].items()
        }

    def tz_probe_expected(self, column: str) -> Dict[int, Optional[datetime]]:
        """Values of `column` as naive datetimes computed by Oracle, without any driver conversion.

        Zoned columns give the UTC instant, `ts_naive` and `d` the stored wall clock.
        """
        expr = {"ts_naive": column, "d": f"CAST({column} AS TIMESTAMP)"}.get(
            column, f"SYS_EXTRACT_UTC({column})"
        )
        rows = self.query(
            f"SELECT id, TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS.FF9') AS v"
            f" FROM {self.tz_probe_table}"
        )
        # python datetimes have microseconds, drivers truncate the remaining digits
        return {
            int(r["id"]): datetime.strptime(r["v"][:26], "%Y-%m-%d %H:%M:%S.%f") if r["v"] else None
            for r in rows
        }

    def generate_users(self, n: int = 50) -> None:
        person = mimesis.Person()
        dt_gen = OracleIncrementingDate()
        table = self.metadata.tables[f"{(self.metadata.schema or '')}.app_user".lstrip(".")]
        info = self.table_infos.setdefault(
            "app_user",
            dict(row_count=0, ids=[], created_at=OracleIncrementingDate(), is_view=False, rows={}),
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
                    some_boolean=random.choice([True, False]),
                    some_date=mimesis.Datetime().date(),
                    some_timestamp_tz=None,
                    some_timestamp_ntz=None,
                    some_timestamp_ltz=None,
                    some_blob=b"\x00\x01\x02",
                    some_number=random.randint(1, 1000000),
                    some_number_precision=random.randint(1, 1000000),
                    some_number_precision_scale=decimal.Decimal(random.randint(1, 1000000)) / 100,
                    some_float=random.uniform(0, 1000000),
                    some_binary_float=random.uniform(1, 1000000),
                    some_binary_double=random.uniform(1, 1000000),
                )
            )
        with self.engine.begin() as conn:
            conn.execute(table.insert(), all_rows)
            all_ids = [row[0] for row in conn.execute(text(f"SELECT id FROM {table.fullname}"))]
            new_ids = sorted(set(all_ids) - set(info["ids"]))
            # cursor values are written as literals: a bound datetime loses its tzinfo
            for id_ in new_ids:
                naive = cursor_datetime(id_, zoned=False)
                zoned = cursor_datetime(id_, zoned=True)
                literal = oracle_timestamp_literal(zoned)
                conn.execute(
                    text(
                        f"UPDATE {table.fullname} SET some_timestamp_tz = {literal},"
                        f" some_timestamp_ltz = {literal},"
                        f" some_timestamp_ntz = {oracle_timestamp_literal(naive)} WHERE id = {id_}"
                    )
                )
                info["rows"][id_] = dict(
                    some_timestamp_tz=zoned, some_timestamp_ltz=zoned, some_timestamp_ntz=naive
                )
        info["ids"] += new_ids
        info["row_count"] += n


class OracleIncrementingDate:
    def __init__(self, start_value: pendulum.DateTime = None) -> None:
        self.current_value = start_value or pendulum.now()

    def __next__(self) -> pendulum.DateTime:
        value = self.current_value
        # never zero: equal cursor values make `range_start="open"` drop a row at a chunk boundary
        self.current_value += timedelta(seconds=random.randrange(1, 120))
        return value


class OracleTableInfo(TypedDict):
    row_count: int
    ids: List[int]
    created_at: NotRequired[OracleIncrementingDate]
    is_view: bool
    rows: Dict[int, Dict[str, Optional[datetime]]]
    """Source values of the datetime columns by id."""
