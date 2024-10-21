import random
from copy import deepcopy
from typing import Dict, List, TypedDict
from uuid import uuid4

import mimesis


from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    create_engine,
    func,
    text,
    ForeignKeyConstraint,
)

try:
    from sqlalchemy import Uuid  # type: ignore[attr-defined]
except ImportError:
    # sql alchemy 1.4
    Uuid = String

from sqlalchemy import (
    schema as sqla_schema,
)

from sqlalchemy.dialects.postgresql import DATERANGE, JSONB

from dlt.common.pendulum import pendulum, timedelta
from dlt.common.utils import chunks, uniq_id
from dlt.sources.credentials import ConnectionStringCredentials


class SQLAlchemySourceDB:
    def __init__(
        self,
        credentials: ConnectionStringCredentials,
        schema: str = None,
        with_unsupported_types: bool = False,
    ) -> None:
        self.credentials = credentials
        self.database_url = credentials.to_native_representation()
        self.schema = schema or "my_dlt_source" + uniq_id()
        self.engine = create_engine(self.database_url)
        self.metadata = MetaData(schema=self.schema)
        self.table_infos: Dict[str, TableInfo] = {}
        self.with_unsupported_types = with_unsupported_types

    def create_schema(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(sqla_schema.CreateSchema(self.schema, if_not_exists=True))

    def drop_schema(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(sqla_schema.DropSchema(self.schema, cascade=True, if_exists=True))

    def get_table(self, name: str) -> Table:
        return self.metadata.tables[f"{self.schema}.{name}"]

    def create_tables(self) -> None:
        Table(
            "app_user",
            self.metadata,
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column("email", Text(), nullable=False, unique=True),
            Column("display_name", Text(), nullable=False),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
            Column(
                "updated_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
        )
        Table(
            "chat_channel",
            self.metadata,
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
            Column("name", Text(), nullable=False),
            Column("active", Boolean(), nullable=False, server_default=text("true")),
            Column(
                "updated_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
        )
        Table(
            "chat_message",
            self.metadata,
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
            Column("content", Text(), nullable=False),
            Column(
                "user_id",
                Integer(),
                ForeignKey("app_user.id"),
                nullable=False,
                index=True,
            ),
            Column(
                "channel_id",
                Integer(),
                ForeignKey("chat_channel.id"),
                nullable=False,
                index=True,
            ),
            Column(
                "updated_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
        )
        Table(
            "has_composite_key",
            self.metadata,
            Column("a", Integer(), primary_key=True),
            Column("b", Integer(), primary_key=True),
            Column("c", Integer(), primary_key=True),
        )

        Table(
            "has_composite_foreign_key",
            self.metadata,
            Column("other_a", Integer()),
            Column("other_b", Integer()),
            Column("other_c", Integer()),
            Column("some_data", Text()),
            ForeignKeyConstraint(
                ["other_a", "other_b", "other_c"],
                ["has_composite_key.a", "has_composite_key.b", "has_composite_key.c"],
            ),
        )

        def _make_precision_table(table_name: str, nullable: bool) -> None:
            Table(
                table_name,
                self.metadata,
                Column("int_col", Integer(), nullable=nullable),
                Column("bigint_col", BigInteger(), nullable=nullable),
                Column("smallint_col", SmallInteger(), nullable=nullable),
                Column("numeric_col", Numeric(precision=10, scale=2), nullable=nullable),
                Column("numeric_default_col", Numeric(), nullable=nullable),
                Column("string_col", String(length=10), nullable=nullable),
                Column("string_default_col", String(), nullable=nullable),
                Column("datetime_tz_col", DateTime(timezone=True), nullable=nullable),
                Column("datetime_ntz_col", DateTime(timezone=False), nullable=nullable),
                Column("date_col", Date, nullable=nullable),
                Column("time_col", Time, nullable=nullable),
                Column("float_col", Float, nullable=nullable),
                Column("json_col", JSONB, nullable=nullable),
                Column("bool_col", Boolean, nullable=nullable),
            )

        _make_precision_table("has_precision", False)
        _make_precision_table("has_precision_nullable", True)

        if self.with_unsupported_types:
            Table(
                "has_unsupported_types",
                self.metadata,
                # Column("unsupported_daterange_1", DATERANGE, nullable=False),
                Column("supported_text", Text, nullable=False),
                Column("supported_int", Integer, nullable=False),
                Column("unsupported_array_1", ARRAY(Integer), nullable=False),
                # Column("supported_datetime", DateTime(timezone=True), nullable=False),
            )

        self.metadata.create_all(bind=self.engine)

        # Create a view
        q = f"""
        CREATE VIEW {self.schema}.chat_message_view AS
        SELECT
            cm.id,
            cm.content,
            cm.created_at as _created_at,
            cm.updated_at as _updated_at,
            au.email as user_email,
            au.display_name as user_display_name,
            cc.name as channel_name,
            CAST(NULL as TIMESTAMP) as _null_ts
        FROM {self.schema}.chat_message cm
        JOIN {self.schema}.app_user au ON cm.user_id = au.id
        JOIN {self.schema}.chat_channel cc ON cm.channel_id = cc.id
        """
        with self.engine.begin() as conn:
            conn.execute(text(q))

    def _fake_users(self, n: int = 8594) -> List[int]:
        person = mimesis.Person()
        user_ids: List[int] = []
        table = self.metadata.tables[f"{self.schema}.app_user"]
        info = self.table_infos.setdefault(
            "app_user",
            dict(row_count=0, ids=[], created_at=IncrementingDate(), is_view=False),
        )
        dt = info["created_at"]
        for chunk in chunks(range(n), 5000):
            rows = [
                dict(
                    email=person.email(unique=True),
                    display_name=person.name(),
                    created_at=next(dt),
                    updated_at=next(dt),
                )
                for i in chunk
            ]
            with self.engine.begin() as conn:
                result = conn.execute(table.insert().values(rows).returning(table.c.id))
                user_ids.extend(result.scalars())
        info["row_count"] += n
        info["ids"] += user_ids
        return user_ids

    def _fake_channels(self, n: int = 500) -> List[int]:
        _text = mimesis.Text()
        dev = mimesis.Development()
        table = self.metadata.tables[f"{self.schema}.chat_channel"]
        channel_ids: List[int] = []
        info = self.table_infos.setdefault(
            "chat_channel",
            dict(row_count=0, ids=[], created_at=IncrementingDate(), is_view=False),
        )
        dt = info["created_at"]
        for chunk in chunks(range(n), 5000):
            rows = [
                dict(
                    name=" ".join(_text.words()),
                    active=dev.boolean(),
                    created_at=next(dt),
                    updated_at=next(dt),
                )
                for i in chunk
            ]
            with self.engine.begin() as conn:
                result = conn.execute(table.insert().values(rows).returning(table.c.id))
                channel_ids.extend(result.scalars())
        info["row_count"] += n
        info["ids"] += channel_ids
        return channel_ids

    def fake_messages(self, n: int = 9402) -> List[int]:
        user_ids = self.table_infos["app_user"]["ids"]
        channel_ids = self.table_infos["chat_channel"]["ids"]
        _text = mimesis.Text()
        choice = mimesis.Choice()
        table = self.metadata.tables[f"{self.schema}.chat_message"]
        message_ids: List[int] = []
        info = self.table_infos.setdefault(
            "chat_message",
            dict(row_count=0, ids=[], created_at=IncrementingDate(), is_view=False),
        )
        dt = info["created_at"]
        for chunk in chunks(range(n), 5000):
            rows = [
                dict(
                    content=_text.random.choice(_text.extract(["questions"])),
                    user_id=choice(user_ids),
                    channel_id=choice(channel_ids),
                    created_at=next(dt),
                    updated_at=next(dt),
                )
                for i in chunk
            ]
            with self.engine.begin() as conn:
                result = conn.execute(table.insert().values(rows).returning(table.c.id))
                message_ids.extend(result.scalars())
        info["row_count"] += len(message_ids)
        info["ids"].extend(message_ids)
        # View is the same number of rows as the table
        view_info = deepcopy(info)
        view_info["is_view"] = True
        view_info = self.table_infos.setdefault("chat_message_view", view_info)
        view_info["row_count"] = info["row_count"]
        view_info["ids"] = info["ids"]
        return message_ids

    def _fake_precision_data(self, table_name: str, n: int = 100, null_n: int = 0) -> None:
        table = self.metadata.tables[f"{self.schema}.{table_name}"]
        self.table_infos.setdefault(table_name, dict(row_count=n + null_n, is_view=False))  # type: ignore[call-overload]
        rows = [
            dict(
                int_col=random.randrange(-2147483648, 2147483647),
                bigint_col=random.randrange(-9223372036854775808, 9223372036854775807),
                smallint_col=random.randrange(-32768, 32767),
                numeric_col=random.randrange(-9999999999, 9999999999) / 100,
                numeric_default_col=random.randrange(-9999999999, 9999999999) / 100,
                string_col=mimesis.Text().word()[:10],
                string_default_col=mimesis.Text().word(),
                datetime_tz_col=mimesis.Datetime().datetime(timezone="UTC"),
                datetime_ntz_col=mimesis.Datetime().datetime(),  # no timezone
                date_col=mimesis.Datetime().date(),
                time_col=mimesis.Datetime().time(),
                float_col=random.random(),
                json_col='{"data": [1, 2, 3]}',  # NOTE: can we do this?
                bool_col=random.randint(0, 1) == 1,
            )
            for _ in range(n + null_n)
        ]
        for row in rows[n:]:
            # all fields to None
            for field in row:
                row[field] = None
        with self.engine.begin() as conn:
            conn.execute(table.insert().values(rows))

    def _fake_chat_data(self, n: int = 9402) -> None:
        self._fake_users()
        self._fake_channels()
        self.fake_messages()

    def _fake_unsupported_data(self, n: int = 100) -> None:
        table = self.metadata.tables[f"{self.schema}.has_unsupported_types"]
        self.table_infos.setdefault("has_unsupported_types", dict(row_count=n, is_view=False))  # type: ignore[call-overload]
        rows = [
            dict(
                # unsupported_daterange_1="[2020-01-01, 2020-09-01]",
                supported_text=mimesis.Text().word(),
                supported_int=random.randint(0, 100),
                unsupported_array_1=[1, 2, 3],
                # supported_datetime="2015-08-12T01:25:22.468126+0100",
            )
            for _ in range(n)
        ]
        with self.engine.begin() as conn:
            conn.execute(table.insert().values(rows))

    def _fake_composite_foreign_key_data(self, n: int = 100) -> None:
        self.table_infos.setdefault("has_composite_key", dict(row_count=n, is_view=False))  # type: ignore[call-overload]
        self.table_infos.setdefault("has_composite_foreign_key", dict(row_count=n, is_view=False))  # type: ignore[call-overload]
        # Insert pkey records into has_composite_key first
        table_pk = self.metadata.tables[f"{self.schema}.has_composite_key"]
        rows_pk = [dict(a=i, b=i + 1, c=i + 2) for i in range(n)]
        # Insert fkey records into has_composite_foreign_key
        table_fk = self.metadata.tables[f"{self.schema}.has_composite_foreign_key"]
        rows_fk = [
            dict(
                other_a=i,
                other_b=i + 1,
                other_c=i + 2,
                some_data=mimesis.Text().word(),
            )
            for i in range(n)
        ]
        with self.engine.begin() as conn:
            conn.execute(table_pk.insert().values(rows_pk))
            conn.execute(table_fk.insert().values(rows_fk))

    def insert_data(self) -> None:
        self._fake_chat_data()
        self._fake_precision_data("has_precision")
        self._fake_precision_data("has_precision_nullable", null_n=10)
        self._fake_composite_foreign_key_data()
        if self.with_unsupported_types:
            self._fake_unsupported_data()


class IncrementingDate:
    def __init__(self, start_value: pendulum.DateTime = None) -> None:
        self.started = False
        self.start_value = start_value or pendulum.now()
        self.current_value = self.start_value

    def __next__(self) -> pendulum.DateTime:
        if not self.started:
            self.started = True
            return self.current_value
        self.current_value += timedelta(seconds=random.randrange(0, 120))
        return self.current_value


class TableInfo(TypedDict):
    row_count: int
    ids: List[int]
    created_at: IncrementingDate
    is_view: bool
