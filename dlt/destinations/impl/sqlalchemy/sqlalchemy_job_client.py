from typing import Iterable, Optional, Type, Dict, Any, Iterator, Sequence, List, Tuple, IO
from types import TracebackType
from contextlib import suppress
import inspect
import math

import sqlalchemy as sa
from sqlalchemy.sql import sqltypes

from dlt.common import logger
from dlt.common import pendulum
from dlt.common.exceptions import TerminalValueError
from dlt.common.destination.reference import (
    JobClientBase,
    LoadJob,
    RunnableLoadJob,
    StorageSchemaInfo,
    StateInfo,
)
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema import Schema, TTableSchema, TColumnSchema, TSchemaTables
from dlt.common.schema.typing import TColumnType, TTableFormat, TTableSchemaColumns
from dlt.common.schema.utils import pipeline_state_table, normalize_table_identifiers
from dlt.common.storages import FileStorage
from dlt.common.json import json, PY_DATETIME_DECODERS
from dlt.destinations.exceptions import DatabaseUndefinedRelation


# from dlt.destinations.impl.sqlalchemy.sql_client import SqlalchemyClient
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyClientConfiguration


class SqlaTypeMapper:
    # TODO: May be merged with TypeMapper as a generic
    def __init__(
        self, capabilities: DestinationCapabilitiesContext, dialect: sa.engine.Dialect
    ) -> None:
        self.capabilities = capabilities
        self.dialect = dialect

    def _db_integer_type(self, precision: Optional[int]) -> sa.types.TypeEngine:
        if precision is None:
            return sa.BigInteger()
        elif precision <= 16:
            return sa.SmallInteger()
        elif precision <= 32:
            return sa.Integer()
        elif precision <= 64:
            return sa.BigInteger()
        raise TerminalValueError(f"Unsupported precision for integer type: {precision}")

    def _create_date_time_type(self, sc_t: str, precision: Optional[int]) -> sa.types.TypeEngine:
        """Use the dialect specific datetime/time type if possible since the generic type doesn't accept precision argument"""
        precision = precision if precision is not None else self.capabilities.timestamp_precision
        if sc_t == "timestamp":
            base_type = sa.DateTime()
            if self.dialect.name == "mysql":  # type: ignore[attr-defined]
                # Special case, type_descriptor does not return the specifc datetime type
                from sqlalchemy.dialects.mysql import DATETIME

                return DATETIME(fsp=precision)
        elif sc_t == "time":
            base_type = sa.Time()

        dialect_type = type(
            self.dialect.type_descriptor(base_type)
        )  # Get the dialect specific subtype
        precision = precision if precision is not None else self.capabilities.timestamp_precision

        # Find out whether the dialect type accepts precision or fsp argument
        params = inspect.signature(dialect_type).parameters
        kwargs: Dict[str, Any] = dict(timezone=True)
        if "fsp" in params:
            kwargs["fsp"] = precision  # MySQL uses fsp for fractional seconds
        elif "precision" in params:
            kwargs["precision"] = precision
        return dialect_type(**kwargs)  # type: ignore[no-any-return]

    def _create_double_type(self) -> sa.types.TypeEngine:
        if dbl := getattr(sa, "Double", None):
            # Sqlalchemy 2 has generic double type
            return dbl()  # type: ignore[no-any-return]
        elif self.dialect.name == "mysql":
            # MySQL has a specific double type
            from sqlalchemy.dialects.mysql import DOUBLE
        return sa.Float(precision=53)  # Otherwise use float

    def _to_db_decimal_type(self, column: TColumnSchema) -> sa.types.TypeEngine:
        precision, scale = column.get("precision"), column.get("scale")
        if precision is None and scale is None:
            precision, scale = self.capabilities.decimal_precision
        return sa.Numeric(precision, scale)

    def to_db_type(self, column: TColumnSchema, table_format: TTableSchema) -> sa.types.TypeEngine:
        sc_t = column["data_type"]
        precision = column.get("precision")
        # TODO: Precision and scale for supported types
        if sc_t == "text":
            length = precision
            if length is None and column.get("unique"):
                length = 128
            if length is None:
                return sa.Text()  # type: ignore[no-any-return]
            return sa.String(length=length)  # type: ignore[no-any-return]
        elif sc_t == "double":
            return self._create_double_type()
        elif sc_t == "bool":
            return sa.Boolean()
        elif sc_t == "timestamp":
            return self._create_date_time_type(sc_t, precision)
        elif sc_t == "bigint":
            return self._db_integer_type(precision)
        elif sc_t == "binary":
            return sa.LargeBinary(length=precision)
        elif sc_t == "complex":
            return sa.JSON(none_as_null=True)
        elif sc_t == "decimal":
            return self._to_db_decimal_type(column)
        elif sc_t == "wei":
            wei_precision, wei_scale = self.capabilities.wei_precision
            return sa.Numeric(precision=wei_precision, scale=wei_scale)
        elif sc_t == "date":
            return sa.Date()
        elif sc_t == "time":
            return self._create_date_time_type(sc_t, precision)
        raise TerminalValueError(f"Unsupported data type: {sc_t}")

    def _from_db_integer_type(self, db_type: sa.Integer) -> TColumnType:
        if isinstance(db_type, sa.SmallInteger):
            return dict(data_type="bigint", precision=16)
        elif isinstance(db_type, sa.Integer):
            return dict(data_type="bigint", precision=32)
        elif isinstance(db_type, sa.BigInteger):
            return dict(data_type="bigint")
        return dict(data_type="bigint")

    def _from_db_decimal_type(self, db_type: sa.Numeric) -> TColumnType:
        precision, scale = db_type.precision, db_type.scale
        if (precision, scale) == self.capabilities.wei_precision:
            return dict(data_type="wei")

        return dict(data_type="decimal", precision=precision, scale=scale)

    def from_db_type(self, db_type: sa.types.TypeEngine) -> TColumnType:
        # TODO: pass the sqla type through dialect.type_descriptor before instance check
        # Possibly need to check both dialect specific and generic types
        if isinstance(db_type, sa.String):
            return dict(data_type="text")
        elif isinstance(db_type, sa.Float):
            return dict(data_type="double")
        elif isinstance(db_type, sa.Boolean):
            return dict(data_type="bool")
        elif isinstance(db_type, sa.DateTime):
            return dict(data_type="timestamp")
        elif isinstance(db_type, sa.Integer):
            return self._from_db_integer_type(db_type)
        elif isinstance(db_type, sqltypes._Binary):
            return dict(data_type="binary", precision=db_type.length)
        elif isinstance(db_type, sa.JSON):
            return dict(data_type="complex")
        elif isinstance(db_type, sa.Numeric):
            return self._from_db_decimal_type(db_type)
        elif isinstance(db_type, sa.Date):
            return dict(data_type="date")
        elif isinstance(db_type, sa.Time):
            return dict(data_type="time")
        raise TerminalValueError(f"Unsupported db type: {db_type}")


class SqlalchemyJsonLInsertJob(RunnableLoadJob):
    def __init__(self, file_path: str, table: sa.Table) -> None:
        super().__init__(file_path)
        self._job_client: "SqlalchemyJobClient" = None
        self.table = table

    def _open_load_file(self) -> IO[bytes]:
        return FileStorage.open_zipsafe_ro(self._file_path, "rb")

    def _iter_data_items(self) -> Iterator[Dict[str, Any]]:
        all_cols = {col.name: None for col in self.table.columns}
        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            for line in f:
                # Decode date/time to py datetime objects. Some drivers have issues with pendulum objects
                for item in json.typed_loadb(line, decoders=PY_DATETIME_DECODERS):
                    # Fill any missing columns in item with None. Bulk insert fails when items have different keys
                    if item.keys() != all_cols.keys():
                        yield {**all_cols, **item}
                    else:
                        yield item

    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        max_rows = self._job_client.capabilities.max_rows_per_insert or math.inf
        # Limit by max query length should not be needed,
        # bulk insert generates an INSERT template with a single VALUES tuple of placeholders
        # If any dialects don't do that we need to check the str length of the query
        # TODO: Max params may not be needed. Limits only apply to placeholders in sql string (mysql/sqlite)
        max_params = self._job_client.capabilities.max_query_parameters or math.inf
        chunk: List[Dict[str, Any]] = []
        params_count = 0
        for item in self._iter_data_items():
            if len(chunk) + 1 == max_rows or params_count + len(item) > max_params:
                # Rotate chunk
                yield chunk
                chunk = []
                params_count = 0
            params_count += len(item)
            chunk.append(item)

        if chunk:
            yield chunk

    def run(self) -> None:
        _sql_client = self._job_client.sql_client

        with _sql_client.begin_transaction():
            for chunk in self._iter_data_item_chunks():
                _sql_client.execute_sql(self.table.insert(), chunk)


class SqlalchemyParquetInsertJob(SqlalchemyJsonLInsertJob):
    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        from dlt.common.libs.pyarrow import ParquetFile

        num_cols = len(self.table.columns)
        max_rows = self._job_client.capabilities.max_rows_per_insert or None
        max_params = self._job_client.capabilities.max_query_parameters or None
        read_limit = None

        with ParquetFile(self._file_path) as reader:
            if max_params is not None:
                read_limit = math.floor(max_params / num_cols)

            if max_rows is not None:
                if read_limit is None:
                    read_limit = max_rows
                else:
                    read_limit = min(read_limit, max_rows)

            if read_limit is None:
                yield reader.read().to_pylist()
                return

            for chunk in reader.iter_batches(batch_size=read_limit):
                yield chunk.to_pylist()


class SqlalchemyJobClient(SqlJobClientBase):
    sql_client: SqlalchemyClient  # type: ignore[assignment]

    def __init__(
        self,
        schema: Schema,
        config: SqlalchemyClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        self.sql_client = SqlalchemyClient(
            config.normalize_dataset_name(schema),
            None,
            config.credentials,
            capabilities,
            engine_args=config.engine_args,
        )

        self.schema = schema
        self.capabilities = capabilities
        self.config = config
        self.type_mapper = SqlaTypeMapper(capabilities, self.sql_client.dialect)

    def _to_table_object(self, schema_table: TTableSchema) -> sa.Table:
        existing = self.sql_client.get_existing_table(schema_table["name"])
        if existing is not None:
            existing_col_names = set(col.name for col in existing.columns)
            new_col_names = set(schema_table["columns"])
            # Re-generate the table if columns have changed
            if existing_col_names == new_col_names:
                return existing
        return sa.Table(
            schema_table["name"],
            self.sql_client.metadata,
            *[
                self._to_column_object(col, schema_table)
                for col in schema_table["columns"].values()
            ],
            extend_existing=True,
            schema=self.sql_client.dataset_name,
        )

    def _to_column_object(
        self, schema_column: TColumnSchema, table_format: TTableSchema
    ) -> sa.Column:
        return sa.Column(
            schema_column["name"],
            self.type_mapper.to_db_type(schema_column, table_format),
            nullable=schema_column.get("nullable", True),
            unique=schema_column.get("unique", False),
        )

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        if file_path.endswith(".typed-jsonl"):
            table_obj = self._to_table_object(table)
            return SqlalchemyJsonLInsertJob(file_path, table_obj)
        elif file_path.endswith(".parquet"):
            table_obj = self._to_table_object(table)
            return SqlalchemyParquetInsertJob(file_path, table_obj)
        return None

    def complete_load(self, load_id: str) -> None:
        loads_table = self._to_table_object(self.schema.tables[self.schema.loads_table_name])
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            loads_table.insert().values(
                (
                    load_id,
                    self.schema.name,
                    0,
                    now_ts,
                    self.schema.version_hash,
                )
            )
        )

    def _get_table_key(self, name: str, schema: Optional[str]) -> str:
        if schema is None:
            return name
        else:
            return schema + "." + name

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        metadata = sa.MetaData()
        for table_name in table_names:
            table_obj = self.sql_client.reflect_table(table_name, metadata)
            if table_obj is None:
                yield table_name, {}
                continue
            yield table_name, {
                col.name: {
                    "name": col.name,
                    "nullable": col.nullable,
                    **self.type_mapper.from_db_type(col.type),
                }
                for col in table_obj.columns
            }

    def update_stored_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
    ) -> Optional[TSchemaTables]:
        # super().update_stored_schema(only_tables, expected_update)
        JobClientBase.update_stored_schema(self, only_tables, expected_update)

        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is not None:
            logger.info(
                "Schema with hash %s inserted at %s found in storage, no upgrade required",
                self.schema.stored_version_hash,
                schema_info.inserted_at,
            )
            return {}
        else:
            logger.info(
                "Schema with hash %s not found in storage, upgrading",
                self.schema.stored_version_hash,
            )

        # Create all schema tables in metadata
        for table_name in only_tables or self.schema.tables:
            self._to_table_object(self.schema.tables[table_name])

        schema_update: TSchemaTables = {}
        tables_to_create: List[sa.Table] = []
        columns_to_add: List[sa.Column] = []

        for table_name in only_tables or self.schema.tables:
            table = self.schema.tables[table_name]
            table_obj, new_columns, exists = self.sql_client.compare_storage_table(table["name"])
            if not new_columns:  # Nothing to do, don't create table without columns
                continue
            if not exists:
                tables_to_create.append(table_obj)
            else:
                columns_to_add.extend(new_columns)
            partial_table = self.prepare_load_table(table_name)
            new_column_names = set(col.name for col in new_columns)
            partial_table["columns"] = {
                col_name: col_def
                for col_name, col_def in partial_table["columns"].items()
                if col_name in new_column_names
            }
            schema_update[table_name] = partial_table

        with self.sql_client.begin_transaction():
            for table_obj in tables_to_create:
                self.sql_client.create_table(table_obj)
            for col in columns_to_add:
                alter = "ALTER TABLE {} ADD COLUMN {}".format(
                    self.sql_client.make_qualified_table_name(col.table.name),
                    self.sql_client.compile_column_def(col),
                )
                self.sql_client.execute_sql(alter)

            self._update_schema_in_storage(self.schema)

        return schema_update

    def _delete_schema_in_storage(self, schema: Schema) -> None:
        version_table = schema.tables[schema.version_table_name]
        table_obj = self._to_table_object(version_table)
        schema_name_col = schema.naming.normalize_identifier("schema_name")
        self.sql_client.execute_sql(
            table_obj.delete().where(table_obj.c[schema_name_col] == schema.name)
        )

    def _update_schema_in_storage(self, schema: Schema) -> None:
        version_table = schema.tables[schema.version_table_name]
        table_obj = self._to_table_object(version_table)
        schema_str = json.dumps(schema.to_dict())

        schema_mapping = StorageSchemaInfo(
            version=schema.version,
            engine_version=schema.ENGINE_VERSION,
            schema_name=schema.name,
            version_hash=schema.stored_version_hash,
            schema=schema_str,
            inserted_at=pendulum.now(),
        ).to_normalized_mapping(schema.naming)

        self.sql_client.execute_sql(table_obj.insert().values(schema_mapping))

    def _get_stored_schema(
        self, version_hash: Optional[str] = None, schema_name: Optional[str] = None
    ) -> Optional[StorageSchemaInfo]:
        version_table = self.schema.tables[self.schema.version_table_name]
        table_obj = self._to_table_object(version_table)
        with suppress(DatabaseUndefinedRelation):
            q = sa.select([table_obj])
            if version_hash is not None:
                version_hash_col = self.schema.naming.normalize_identifier("version_hash")
                q = q.where(table_obj.c[version_hash_col] == version_hash)
            if schema_name is not None:
                schema_name_col = self.schema.naming.normalize_identifier("schema_name")
                q = q.where(table_obj.c[schema_name_col] == schema_name)
            inserted_at_col = self.schema.naming.normalize_identifier("inserted_at")
            q = q.order_by(table_obj.c[inserted_at_col].desc())
            with self.sql_client.execute_query(q) as cur:
                row = cur.fetchone()
                if row is None:
                    return None

                # TODO: Decode compressed schema str if needed
                return StorageSchemaInfo.from_normalized_mapping(row._mapping, self.schema.naming)

    def get_stored_schema_by_hash(self, version_hash: str) -> Optional[StorageSchemaInfo]:
        return self._get_stored_schema(version_hash)

    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Get the latest stored schema"""
        return self._get_stored_schema(schema_name=self.schema.name)

    def get_stored_state(self, pipeline_name: str) -> StateInfo:
        state_table = self.schema.tables.get(
            self.schema.state_table_name
        ) or normalize_table_identifiers(pipeline_state_table(), self.schema.naming)
        state_table_obj = self._to_table_object(state_table)
        loads_table = self.schema.tables[self.schema.loads_table_name]
        loads_table_obj = self._to_table_object(loads_table)

        c_load_id, c_dlt_load_id, c_pipeline_name, c_status = map(
            self.schema.naming.normalize_identifier,
            ("load_id", "_dlt_load_id", "pipeline_name", "status"),
        )

        query = (
            sa.select(state_table_obj)
            .join(loads_table_obj, loads_table_obj.c[c_load_id] == state_table_obj.c[c_dlt_load_id])
            .where(
                sa.and_(
                    state_table_obj.c[c_pipeline_name] == pipeline_name,
                    loads_table_obj.c[c_status] == 0,
                )
            )
            .order_by(loads_table_obj.c[c_load_id].desc())
        )

        with self.sql_client.execute_query(query) as cur:
            row = cur.fetchone()
            if not row:
                return None
            mapping = dict(row._mapping)

        return StateInfo.from_normalized_mapping(mapping, self.schema.naming)

    def _from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        raise NotImplementedError()

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        raise NotImplementedError()
