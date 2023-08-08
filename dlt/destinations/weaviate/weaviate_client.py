from functools import wraps
from types import TracebackType
from typing import (
    ClassVar,
    Optional,
    Sequence,
    List,
    Dict,
    Type,
    Iterable,
    Any,
    IO,
    Tuple,
)

from dlt.common.exceptions import (
    DestinationUndefinedEntity,
    DestinationTransientException,
    DestinationTerminalException,
)

import weaviate
from weaviate.util import generate_uuid5

from dlt.common import json, pendulum, logger
from dlt.common.typing import TFun
from dlt.common.schema import Schema, TTableSchema, TSchemaTables, TTableSchemaColumns
from dlt.common.schema.typing import TColumnSchema, TColumnSchemaBase
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    JobClientBase,
)
from dlt.common.data_types import TDataType
from dlt.common.storages import FileStorage
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.job_client_impl import StorageSchemaInfo

from dlt.destinations.weaviate import capabilities
from dlt.destinations.weaviate.configuration import WeaviateClientConfiguration

from dlt.destinations.weaviate.exceptions import WeaviateBatchError

from dlt.common.time import ensure_pendulum_datetime


SCT_TO_WT: Dict[TDataType, str] = {
    "text": "text",
    "double": "number",
    "bool": "boolean",
    "timestamp": "date",
    "date": "date",
    "bigint": "int",
    "binary": "blob",
    "decimal": "text",
    "wei": "number",
    "complex": "text"
}

WT_TO_SCT: Dict[str, TDataType] = {
    "text": "text",
    "number": "double",
    "boolean": "bool",
    "date": "timestamp",
    "int": "bigint",
    "blob": "binary",
}

def table_name_to_class_name(table_name: str) -> str:
    # Weaviate requires class names to be written with
    # a capital letter first:
    # https://weaviate.io/developers/weaviate/config-refs/schema#class
    # For dlt tables strip the underscore from the name
    # and make it all caps
    # For non dlt tables make the class name camel case
    return table_name


def wrap_weaviate_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        # those look like terminal exceptions
        except (
            weaviate.exceptions.ObjectAlreadyExistsException,
            weaviate.exceptions.ObjectAlreadyExistsException,
            weaviate.exceptions.SchemaValidationException,
            weaviate.exceptions.WeaviateEmbeddedInvalidVersion,
        ) as term_ex:
            print(term_ex)
            raise DestinationTerminalException(term_ex) from term_ex
        except weaviate.exceptions.UnexpectedStatusCodeException as status_ex:
            print(status_ex)
            # special handling for non existing objects/classes
            if status_ex.status_code == 404:
                raise DestinationUndefinedEntity(status_ex) from status_ex
            # looks like there are no more terminal exceptions
            if status_ex.status_code in (403, 422):
                raise DestinationTerminalException(status_ex)
            raise DestinationTransientException(status_ex)
        except weaviate.exceptions.WeaviateBaseError as we_ex:
            print(we_ex)
            # also includes 401 as transient
            raise DestinationTransientException(we_ex)

    return _wrap  # type: ignore


def wrap_batch_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        # those look like terminal exceptions
        except WeaviateBatchError as batch_ex:
            errors = batch_ex.args[0]
            message = errors["error"][0]["message"]
            # TODO: actually put the job in failed/retry state and prepare exception message with full info on failing item
            if "invalid" in message and "property" in message and "on class" in message:
                raise DestinationTerminalException(f'Batch failed {errors} AND WILL BE RETRIED')
            raise DestinationTransientException(f'Batch failed {errors} AND WILL BE RETRIED')
        except Exception:
            raise DestinationTransientException(f'Batch failed AND WILL BE RETRIED')
    return _wrap  # type: ignore


class LoadWeaviateJob(LoadJob):
    def __init__(
        self,
        table_schema: TTableSchema,
        local_path: str,
        db_client: weaviate.Client,
        client_config: WeaviateClientConfiguration,
        load_id: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)
        self.client_config = client_config
        self.db_client = db_client
        self.class_name = table_name_to_class_name(table_schema["name"])
        self.unique_identifiers = self.list_unique_identifiers(table_schema)
        self.complex_indices = [i for i, field in table_schema["columns"].items() if field["data_type"] == "complex"]
        self.date_indices = [i for i, field in table_schema["columns"].items() if field["data_type"] == "date"]

        with FileStorage.open_zipsafe_ro(local_path) as f:
            self.load_batch(f)

    @wrap_weaviate_error
    def load_batch(self, f: IO[str]) -> None:
        """Load all the lines from stream `f` in automatic Weaviate batches.
        Weaviate batch supports retries so we do not need to do that.
        """

        @wrap_batch_error
        def check_batch_result(results: Dict[str, Any]) -> None:
            """This kills batch on first error reported"""
            if results is not None:
                for result in results:
                    if 'result' in result and 'errors' in result['result']:
                        if 'error' in result['result']['errors']:
                            raise WeaviateBatchError(result['result']['errors'])

        with self.db_client.batch(
            batch_size=self.client_config.batch_size,
            timeout_retries=self.client_config.batch_retries,
            connection_error_retries=self.client_config.batch_retries,
            weaviate_error_retries=weaviate.WeaviateErrorRetryConf(
                self.client_config.batch_retries
            ),
            consistency_level=weaviate.ConsistencyLevel[
                self.client_config.batch_consistency
            ],
            num_workers=self.client_config.batch_workers,
            callback=check_batch_result,
        ) as batch:
            for line in f:
                data = json.loads(line)
                # make complex to strings
                for key in self.complex_indices:
                    if key in data:
                        data[key] = json.dumps(data[key])
                for key in self.date_indices:
                    if key in data:
                        data[key] = str(ensure_pendulum_datetime(data[key]))
                if self.unique_identifiers:
                    uuid = self.generate_uuid(
                        data, self.unique_identifiers, self.class_name
                    )
                else:
                    uuid = None

                batch.add_data_object(data, self.class_name, uuid=uuid)

    def list_unique_identifiers(self, table_schema: TTableSchema) -> Sequence[str]:
        if table_schema.get("write_disposition") == "merge":
            primary_keys = get_columns_names_with_prop(table_schema, "primary_key")
            if primary_keys:
                return primary_keys
        return get_columns_names_with_prop(table_schema, "unique")

    def generate_uuid(
        self, data: Dict[str, Any], unique_identifiers: Sequence[str], class_name: str
    ) -> str:
        data_id = "_".join([str(data[key]) for key in unique_identifiers])
        return generate_uuid5(data_id, class_name)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class WeaviateClient(JobClientBase):
    """Weaviate client implementation."""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: WeaviateClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: WeaviateClientConfiguration = config
        self.db_client = self.create_db_client(config)

        self._vectorizer_config = {
            "vectorizer": config.vectorizer,
            "moduleConfig": config.module_config,
        }

    @staticmethod
    def create_db_client(config: WeaviateClientConfiguration) -> weaviate.Client:
        return weaviate.Client(
            url=config.credentials.url,
            auth_client_secret=weaviate.AuthApiKey(api_key=config.credentials.api_key),
            additional_headers=config.credentials.additional_headers,
        )

    @wrap_weaviate_error
    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if truncate_tables:
            for table_name in truncate_tables:
                class_name = table_name_to_class_name(table_name)
                try:
                    class_schema = self.db_client.schema.get(class_name)
                except weaviate.exceptions.UnexpectedStatusCodeException as e:
                    if e.status_code == 404:
                        continue
                    raise

                self.db_client.schema.delete_class(class_name)
                self.db_client.schema.create_class(class_schema)

    @wrap_weaviate_error
    def is_storage_initialized(self) -> bool:
        return True

    @wrap_weaviate_error
    def update_storage_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
    ) -> Optional[TSchemaTables]:
        # Retrieve the schema from Weaviate
        applied_update: TSchemaTables = {}
        schema_info = self.get_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"not found in the storage. upgrading"
            )
            self._execute_schema_update(only_tables)
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"inserted at {schema_info.inserted_at} found "
                f"in storage, no upgrade required"
            )

        return applied_update

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            exists, existing_columns = self.get_storage_table(table_name)
            new_columns = self.schema.get_new_table_columns(
                table_name, existing_columns
            )
            logger.info(
                f"Found {len(new_columns)} updates for {table_name} in {self.schema.name}"
            )
            if len(new_columns) > 0:
                if exists:
                    class_name = table_name_to_class_name(table_name)
                    for column in new_columns:
                        prop = self._make_property_schema(column['name'], column, True)
                        self.db_client.schema.property.create(
                            class_name, prop
                        )
                else:
                    table = self.schema.tables[table_name]
                    class_schema = self.make_weaviate_class_schema(table)
                    self.db_client.schema.create_class(class_schema)
        self._update_schema_in_storage(self.schema)

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        class_name = table_name_to_class_name(table_name)
        table_schema: TTableSchemaColumns = {}
        try:
            class_schema = self.db_client.schema.get(class_name)
        except weaviate.exceptions.UnexpectedStatusCodeException as e:
            if e.status_code == 404:
                return False, table_schema
            raise

        # Convert Weaviate class schema to dlt table schema
        for prop in class_schema["properties"]:
            schema_c: TColumnSchemaBase = {
                "name": prop["name"],
                "data_type": self._from_db_type(prop["dataType"][0]),
            }
            table_schema[prop["name"]] = schema_c
        return True, table_schema

    def get_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        version_class_name = table_name_to_class_name(self.schema.version_table_name)

        try:
            self.db_client.schema.get(version_class_name)
        except weaviate.exceptions.UnexpectedStatusCodeException as e:
            if e.status_code == 404:
                return None
            raise

        properties = [
            "version_hash",
            "schema_name",
            "version",
            "engine_version",
            "inserted_at",
            "schema",
        ]

        response = (
            self.db_client.query.get(version_class_name, properties)
            .with_where(
                {
                    "path": ["version_hash"],
                    "operator": "Equal",
                    "valueString": schema_hash,
                }
            )
            .with_limit(1)
            .do()
        )

        try:
            record = response["data"]["Get"][version_class_name][0]
        except IndexError:
            return None
        return StorageSchemaInfo(**record)

    def make_weaviate_class_schema(self, table: TTableSchema) -> Dict[str, Any]:
        """Creates a Weaviate class schema from a table schema."""
        table_name = table["name"]

        class_name = table_name_to_class_name(table_name)

        if table_name.startswith(self.schema._dlt_tables_prefix):
            return self._make_non_vectorized_class_schema(class_name, table)

        return self._make_vectorized_class_schema(class_name, table)

    def _make_properties(
        self, table: TTableSchema, is_vectorized_class: bool = True
    ) -> List[Dict[str, Any]]:
        """Creates a Weaviate properties schema from a table schema.

        Args:
            table: The table schema.
            is_vectorized_class: Controls whether the `moduleConfig` should be
                added to the properties schema. This is only needed for
                vectorized classes.
        """

        return [
            self._make_property_schema(column_name, column, is_vectorized_class)
            for column_name, column in table["columns"].items()
        ]

    def _make_property_schema(
        self, column_name: str, column: TColumnSchema, is_vectorized_class: bool
    ) -> Dict[str, Any]:
        extra_kv = {}

        if is_vectorized_class:
            vectorizer_name = self._vectorizer_config["vectorizer"]

            # x-vectorize: (bool) means that this field should be vectorized
            if not column.get("x-vectorize", False):
                extra_kv["moduleConfig"] = {
                    vectorizer_name: {
                        "skip": True,
                    }
                }

            # x-tokenization: (str) specifies the method to use
            # for tokenization
            if column.get("x-tokenization"):
                extra_kv["tokenization"] = column["x-tokenization"]

        return {
            "name": column_name,
            "dataType": [self._to_db_type(column["data_type"])],
            **extra_kv,
        }

    def _make_vectorized_class_schema(
        self, class_name: str, table: TTableSchema
    ) -> Dict[str, Any]:
        properties = self._make_properties(table)

        return {
            "class": class_name,
            "properties": properties,
            **self._vectorizer_config,
        }

    def _make_non_vectorized_class_schema(
        self, class_name: str, table: TTableSchema
    ) -> Dict[str, Any]:
        properties = self._make_properties(table, is_vectorized_class=False)

        return {
            "class": class_name,
            "properties": properties,
            "vectorizer": "none",
            "vectorIndexConfig": {
                "skip": True,
            },
        }

    def start_file_load(
        self, table: TTableSchema, file_path: str, load_id: str
    ) -> LoadJob:
        return LoadWeaviateJob(
            table,
            file_path,
            db_client=self.db_client,
            client_config=self.config,
            load_id=load_id,
        )

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    @wrap_weaviate_error
    def complete_load(self, load_id: str) -> None:
        load_table_name = table_name_to_class_name(self.schema.loads_table_name)
        properties = {
            "load_id": load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": str(pendulum.now()),
        }
        self.db_client.data_object.create(properties, load_table_name)

    def __enter__(self) -> "WeaviateClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def _update_schema_in_storage(self, schema: Schema) -> None:
        now_ts = str(pendulum.now())
        schema_str = json.dumps(schema.to_dict())
        version_class_name = table_name_to_class_name(self.schema.version_table_name)
        properties = {
            "version_hash": schema.stored_version_hash,
            "schema_name": schema.name,
            "version": schema.version,
            "engine_version": schema.ENGINE_VERSION,
            "inserted_at": now_ts,
            "schema": schema_str,
        }

        self.db_client.data_object.create(properties, version_class_name)

    @staticmethod
    def _to_db_type(sc_t: TDataType) -> str:
        return SCT_TO_WT[sc_t]

    @staticmethod
    def _from_db_type(wt_t: str) -> TDataType:
        return WT_TO_SCT[wt_t]
