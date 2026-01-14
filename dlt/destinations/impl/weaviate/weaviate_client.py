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
    cast,
)

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTransientException,
    DestinationTerminalException,
)

import weaviate
from weaviate.classes.config import (
    Configure,
    Property,
    DataType,
    Tokenization,
)
from weaviate.classes.query import Filter, Sort
from weaviate.util import generate_uuid5
from weaviate.exceptions import (
    WeaviateConnectionError,
    WeaviateClosedClientError,
    WeaviateInvalidInputError,
    UnexpectedStatusCodeError,
)

from dlt.common import logger
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.typing import StrAny, TFun
from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.schema import Schema, TSchemaTables, TTableSchemaColumns
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    C_DLT_LOADS_TABLE_LOAD_ID,
    TColumnSchema,
    TColumnType,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    loads_table,
    normalize_table_identifiers,
    version_table,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    PreparedTableSchema,
    RunnableLoadJob,
    JobClientBase,
    WithStateSync,
    LoadJob,
)
from dlt.common.storages import FileStorage

from dlt.destinations.impl.weaviate.weaviate_adapter import VECTORIZE_HINT, TOKENIZATION_HINT
from dlt.destinations.job_client_impl import StorageSchemaInfo, StateInfo
from dlt.destinations.impl.weaviate.configuration import WeaviateClientConfiguration
from dlt.destinations.impl.weaviate.exceptions import PropertyNameConflict, WeaviateBatchError
from dlt.destinations.utils import get_pipeline_state_query_columns


# Type mapping from Weaviate type strings (output of WeaviateTypeMapper) to v4 DataType enums
# The WeaviateTypeMapper in factory.py converts dlt types to these Weaviate type strings:
#   dlt type -> Weaviate string -> DataType enum
#   text -> "text" -> TEXT
#   double -> "number" -> NUMBER
#   bool -> "boolean" -> BOOL
#   timestamp/date -> "date" -> DATE
#   bigint -> "int" -> INT
#   binary -> "blob" -> BLOB (binary data like images, PDFs, etc.)
#   json -> "text" -> TEXT (complex nested objects are JSON-serialized)
#   decimal -> "text" -> TEXT (decimal precision preserved as string)
#   wei -> "number" -> NUMBER (Ethereum currency values as floats)
#   time -> "text" -> TEXT (time values as ISO strings)
WEAVIATE_TYPE_TO_DATATYPE: Dict[str, DataType] = {
    "text": DataType.TEXT,
    "number": DataType.NUMBER,
    "boolean": DataType.BOOL,
    "date": DataType.DATE,
    "int": DataType.INT,
    "blob": DataType.BLOB,
}


def wrap_weaviate_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: JobClientBase, *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except WeaviateInvalidInputError as term_ex:
            raise DestinationTerminalException(term_ex) from term_ex
        except UnexpectedStatusCodeError as status_ex:
            # special handling for non existing objects/collections
            if status_ex.status_code == 404:
                raise DestinationUndefinedEntity(status_ex)
            if status_ex.status_code == 403:
                raise DestinationTerminalException(status_ex)
            if status_ex.status_code == 422:
                if "conflict for property" in str(status_ex) or "none vectorizer module" in str(
                    status_ex
                ):
                    raise PropertyNameConflict(str(status_ex))
                raise DestinationTerminalException(status_ex)
            # looks like there are no more terminal exceptions
            raise DestinationTransientException(status_ex)
        except (WeaviateConnectionError, WeaviateClosedClientError) as conn_ex:
            raise DestinationTransientException(conn_ex)

    return _wrap  # type: ignore


def wrap_batch_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except WeaviateBatchError as batch_ex:
            errors = batch_ex.args[0]
            message = str(errors)
            # TODO: actually put the job in failed/retry state and prepare exception message with full info on failing item
            if "invalid" in message and "property" in message:
                raise DestinationTerminalException(
                    f"Batch failed `{errors}` AND WILL **NOT** BE RETRIED"
                )
            if "conflict for property" in message:
                raise PropertyNameConflict(message)
            # v4 API gives different error for case-sensitive property conflicts
            if "no such prop with name" in message.lower() and "in class" in message.lower():
                raise PropertyNameConflict(message)
            raise DestinationTransientException(f"Batch failed `{errors}` AND WILL BE RETRIED")
        except Exception:
            raise DestinationTransientException("Batch failed AND WILL BE RETRIED")

    return _wrap  # type: ignore


class LoadWeaviateJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
        collection_name: str,
    ) -> None:
        super().__init__(file_path)
        self._job_client: WeaviateClient = None
        self._collection_name = collection_name

    def run(self) -> None:
        self._db_client = self._job_client.db_client
        self._client_config = self._job_client.config
        self.unique_identifiers = self.list_unique_identifiers(self._load_table)
        self.nested_indices = [
            i
            for i, field in self._schema.get_table_columns(self.load_table_name).items()
            if field["data_type"] == "json"
        ]
        self.date_indices = [
            i
            for i, field in self._schema.get_table_columns(self.load_table_name).items()
            if field["data_type"] == "date"
        ]
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            self.load_batch(f)

    @wrap_weaviate_error
    def load_batch(self, f: IO[str]) -> None:
        """Load all the lines from stream `f` using Weaviate batch.
        Weaviate batch supports retries so we do not need to do that.
        """
        collection = self._db_client.collections.get(self._collection_name)

        @wrap_batch_error
        def check_batch_result(failed_objects: List[Any]) -> None:
            """Check for failed objects and raise error if any"""
            if failed_objects:
                # Collect error messages from failed objects
                errors = []
                for obj in failed_objects:
                    if hasattr(obj, "message"):
                        errors.append(obj.message)
                    else:
                        errors.append(str(obj))
                if errors:
                    raise WeaviateBatchError({"error": [{"message": err} for err in errors]})

        # Collect all objects to insert
        objects_to_insert = []
        for line in f:
            data = json.loads(line)
            # serialize json types
            for key in self.nested_indices:
                if key in data:
                    data[key] = json.dumps(data[key])
            for key in self.date_indices:
                if key in data:
                    data[key] = ensure_pendulum_datetime_utc(data[key]).isoformat()
            if self.unique_identifiers:
                uuid = self.generate_uuid(data, self.unique_identifiers, self._collection_name)
            else:
                uuid = None

            objects_to_insert.append({"properties": data, "uuid": uuid})

        # Use batch insert with the v4 API
        with collection.batch.dynamic() as batch:
            for obj in objects_to_insert:
                batch.add_object(
                    properties=obj["properties"],
                    uuid=obj["uuid"],
                )

        # Check for failed objects
        failed_objects = collection.batch.failed_objects
        if failed_objects:
            check_batch_result(failed_objects)

    def list_unique_identifiers(self, table_schema: PreparedTableSchema) -> Sequence[str]:
        if table_schema.get("write_disposition") == "merge":
            primary_keys = get_columns_names_with_prop(table_schema, "primary_key")
            if primary_keys:
                return primary_keys
        return get_columns_names_with_prop(table_schema, "unique")

    def generate_uuid(
        self, data: Dict[str, Any], unique_identifiers: Sequence[str], collection_name: str
    ) -> str:
        data_id = "_".join([str(data[key]) for key in unique_identifiers])
        return str(generate_uuid5(data_id, collection_name))


class WeaviateClient(JobClientBase, WithStateSync):
    """Weaviate client implementation using v4 API."""

    def __init__(
        self,
        schema: Schema,
        config: WeaviateClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        # get definitions of the dlt tables, normalize column names and keep for later use
        version_table_ = normalize_table_identifiers(version_table(), schema.naming)
        self.version_collection_properties = list(version_table_["columns"].keys())
        loads_table_ = normalize_table_identifiers(loads_table(), schema.naming)
        self.loads_collection_properties = list(loads_table_["columns"].keys())
        state_table_ = normalize_table_identifiers(
            get_pipeline_state_query_columns(), schema.naming
        )
        self.pipeline_state_properties = list(state_table_["columns"].keys())

        self.config: WeaviateClientConfiguration = config
        self.db_client: weaviate.WeaviateClient = None

        self._vectorizer_config = config.vectorizer
        self._module_config = config.module_config
        self.type_mapper = self.capabilities.get_type_mapper()

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

    @property
    def sentinel_collection(self) -> str:
        # if no dataset name is provided we still want to create sentinel collection
        return self.dataset_name or "DltSentinelClass"

    @staticmethod
    def create_db_client(config: WeaviateClientConfiguration) -> weaviate.WeaviateClient:
        """Create a Weaviate client using v4 API.

        Supports three connection types:
        - "cloud": For Weaviate Cloud Services (uses connect_to_weaviate_cloud)
        - "local": For local Docker instances (uses connect_to_local)
        - "custom": For self-hosted instances (uses connect_to_custom)

        If connection_type is not specified, it's auto-detected from the URL:
        - URLs containing ".weaviate.cloud" -> "cloud"
        - URLs with "localhost" or "127.0.0.1" -> "local"
        - Other URLs -> "custom" (requires http_port and grpc_port)
        """
        url = config.credentials.url
        api_key = config.credentials.api_key
        headers = config.credentials.additional_headers or {}
        connection_type = config.connection_type

        # Create auth config if API key is provided
        auth_config = None
        if api_key:
            auth_config = weaviate.auth.AuthApiKey(api_key)

        # Auto-detect connection type if not specified
        if connection_type is None:
            if ".weaviate.cloud" in url or ".wcs.api.weaviate.io" in url:
                connection_type = "cloud"
            elif "localhost" in url or "127.0.0.1" in url:
                connection_type = "local"
            else:
                connection_type = "custom"

        if connection_type == "cloud":
            # Use connect_to_weaviate_cloud for Weaviate Cloud Services
            # Ensure URL has https:// prefix
            cluster_url = url if url.startswith("https://") else f"https://{url}"
            return weaviate.connect_to_weaviate_cloud(
                cluster_url=cluster_url,
                auth_credentials=auth_config,
                headers=headers,
                skip_init_checks=True,
            )
        elif connection_type == "local":
            # Use connect_to_local for local Docker instances
            # Parse host from URL
            host = url.replace("http://", "").replace("https://", "").split(":")[0]
            http_port = config.credentials.http_port or 8080
            grpc_port = config.credentials.grpc_port or 50051
            return weaviate.connect_to_local(
                host=host,
                port=http_port,
                grpc_port=grpc_port,
                headers=headers,
                auth_credentials=auth_config,
            )
        else:  # custom
            # Use connect_to_custom for self-hosted instances
            # Require explicit ports for custom connections
            http_port = config.credentials.http_port
            grpc_port = config.credentials.grpc_port
            if http_port is None or grpc_port is None:
                raise ConfigurationValueError(
                    "http_port and grpc_port",
                    "http_port and grpc_port are required when connection_type is 'custom'. "
                    "Set them in [destination.weaviate.credentials] or use connection_type='local' "
                    "for default ports (http: 8080, grpc: 50051).",
                )
            host = url.replace("http://", "").replace("https://", "").split(":")[0]
            is_secure = url.startswith("https")
            return weaviate.connect_to_custom(
                http_host=host,
                http_port=http_port,
                http_secure=is_secure,
                grpc_host=host,
                grpc_port=grpc_port,
                grpc_secure=is_secure,
                auth_credentials=auth_config,
                headers=headers,
                skip_init_checks=True,
            )

    def make_qualified_collection_name(self, table_name: str) -> str:
        """Make a full Weaviate collection name from a table name by prepending
        the dataset name if it exists.
        """
        dataset_separator = self.config.dataset_separator

        return (
            f"{self.dataset_name}{dataset_separator}{table_name}"
            if self.dataset_name
            else table_name
        )

    def get_collection_schema(self, table_name: str) -> Dict[str, Any]:
        """Get the Weaviate collection schema for a table."""
        collection_name = self.make_qualified_collection_name(table_name)
        try:
            collection = self.db_client.collections.get(collection_name)
            config = collection.config.get()
            return self._config_to_dict(config)
        except UnexpectedStatusCodeError as e:
            if e.status_code == 404:
                raise
            raise
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                # Re-raise as UnexpectedStatusCodeError with status_code attribute
                err = UnexpectedStatusCodeError(f"Collection {collection_name} not found", None)
                raise err from e
            raise

    def _config_to_dict(self, config: Any) -> Dict[str, Any]:
        """Convert a collection config to a dictionary format similar to v3"""
        # Get vectorizer name - in v4 it's directly a Vectorizers enum
        vectorizer_name = "none"
        if config.vectorizer:
            if hasattr(config.vectorizer, "value"):
                vectorizer_name = config.vectorizer.value
            else:
                vectorizer_name = str(config.vectorizer)

        result: Dict[str, Any] = {
            "class": config.name,
            "properties": [],
            "vectorizer": vectorizer_name,
        }

        # Add vectorIndexConfig if available
        if config.vector_index_config:
            result["vectorIndexConfig"] = {
                "skip": getattr(config.vector_index_config, "skip", False),
            }

        # Add properties
        for prop in config.properties:
            # Get data type - handle both enum and string
            data_type_value = (
                prop.data_type.value if hasattr(prop.data_type, "value") else str(prop.data_type)
            )
            # Clean up data type value if it looks like "DataType.TEXT" -> "text"
            if "." in data_type_value:
                data_type_value = data_type_value.split(".")[-1].lower()

            prop_dict: Dict[str, Any] = {
                "name": prop.name,
                "dataType": [data_type_value],
            }
            if prop.tokenization:
                prop_dict["tokenization"] = (
                    prop.tokenization.value
                    if hasattr(prop.tokenization, "value")
                    else str(prop.tokenization)
                )

            # Add moduleConfig for vectorization skip info
            # In v4 API, the skip value is in prop.vectorizer_config.skip
            if vectorizer_name != "none":
                skip_vectorization = False
                if hasattr(prop, "vectorizer_config") and prop.vectorizer_config:
                    skip_vectorization = getattr(prop.vectorizer_config, "skip", False)
                prop_dict["moduleConfig"] = {
                    vectorizer_name: {
                        "skip": skip_vectorization,
                    }
                }
            result["properties"].append(prop_dict)

        return result

    def create_collection(
        self, collection_config: Dict[str, Any], full_collection_name: Optional[str] = None
    ) -> None:
        """Create a Weaviate collection.

        Args:
            collection_config: The collection configuration to create.
            full_collection_name: The full name of the collection to create. If not
                provided, the collection name will be prepended with the dataset name
                if it exists.
        """
        name = (
            self.make_qualified_collection_name(collection_config["class"])
            if full_collection_name is None
            else full_collection_name
        )

        # Build properties
        properties = []
        for prop in collection_config.get("properties", []):
            prop_config = self._make_property_config(prop)
            properties.append(prop_config)

        # Determine vectorizer configuration
        vectorizer = collection_config.get("vectorizer", "none")
        vectorizer_config = None

        if vectorizer == "none":
            # No vectorization
            vectorizer_config = Configure.Vectorizer.none()
        else:
            # Use the specified vectorizer
            vectorizer_config = self._get_vectorizer_config(vectorizer)

        # Create the collection
        self.db_client.collections.create(
            name=name,
            properties=properties,
            vectorizer_config=vectorizer_config,
        )

    def _make_property_config(self, prop: Dict[str, Any]) -> Property:
        """Create a Property config from a property dict"""
        data_type_str = (
            prop["dataType"][0] if isinstance(prop["dataType"], list) else prop["dataType"]
        )
        data_type = WEAVIATE_TYPE_TO_DATATYPE.get(data_type_str.lower(), DataType.TEXT)

        # Handle tokenization
        tokenization = None
        if "tokenization" in prop:
            token_map = {
                "word": Tokenization.WORD,
                "lowercase": Tokenization.LOWERCASE,
                "whitespace": Tokenization.WHITESPACE,
                "field": Tokenization.FIELD,
            }
            tokenization = token_map.get(prop["tokenization"])

        # Handle skip vectorization - this is per-property in v4
        skip_vectorization = False
        if "moduleConfig" in prop:
            for _module_name, module_config in prop["moduleConfig"].items():
                if module_config.get("skip", False):
                    skip_vectorization = True
                    break

        return Property(
            name=prop["name"],
            data_type=data_type,
            tokenization=tokenization,
            skip_vectorization=skip_vectorization,
            # Disable property name vectorization to avoid contextionary issues with
            # property names that contain underscores or are not English words
            vectorize_property_name=False,
        )

    def _get_vectorizer_config(self, vectorizer: str) -> Any:
        """Get the vectorizer configuration based on vectorizer name"""
        # Map vectorizer names to Configure.Vectorizer methods
        vectorizer_map = {
            "text2vec-openai": Configure.Vectorizer.text2vec_openai,
            "text2vec-cohere": Configure.Vectorizer.text2vec_cohere,
            "text2vec-contextionary": Configure.Vectorizer.text2vec_contextionary,
            "text2vec-huggingface": Configure.Vectorizer.text2vec_huggingface,
            "text2vec-palm": Configure.Vectorizer.text2vec_palm,
        }

        if vectorizer in vectorizer_map:
            # Get the module config if available
            module_conf = self._module_config.get(vectorizer, {}) if self._module_config else {}
            try:
                # Filter out incompatible config keys for each vectorizer
                if vectorizer == "text2vec-contextionary":
                    # Contextionary accepts vectorize_collection_name
                    filtered_conf = {
                        k: v
                        for k, v in module_conf.items()
                        if k in ["vectorize_collection_name", "vectorizeClassName"]
                    }
                    # Map old v3 config key names to v4
                    if "vectorizeClassName" in filtered_conf:
                        filtered_conf["vectorize_collection_name"] = filtered_conf.pop(
                            "vectorizeClassName"
                        )
                    return (
                        vectorizer_map[vectorizer](**filtered_conf)
                        if filtered_conf
                        else vectorizer_map[vectorizer]()
                    )
                else:
                    return vectorizer_map[vectorizer](**module_conf)
            except TypeError:
                # If module_conf keys don't match, try without config
                return vectorizer_map[vectorizer]()

        # Default to none if not found
        return Configure.Vectorizer.none()

    def add_property_to_collection(self, collection_name: str, prop_schema: Dict[str, Any]) -> None:
        """Add a property to an existing Weaviate collection.

        Args:
            collection_name: The name of the collection to add the property to.
            prop_schema: The property schema to add.
        """
        full_name = self.make_qualified_collection_name(collection_name)
        collection = self.db_client.collections.get(full_name)
        prop_config = self._make_property_config(prop_schema)
        collection.config.add_property(prop_config)

    def delete_collection(self, collection_name: str) -> None:
        """Delete a Weaviate collection.

        Args:
            collection_name: The name of the collection to delete.
        """
        self.db_client.collections.delete(self.make_qualified_collection_name(collection_name))

    def delete_all_collections(self) -> None:
        """Delete all Weaviate collections from Weaviate instance and all data
        associated with it.
        """
        self.db_client.collections.delete_all()

    def query_collection(
        self, collection_name: str, properties: List[str], limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query a Weaviate collection.

        Args:
            collection_name: The name of the collection to query.
            properties: The properties to return.
            limit: Maximum number of results to return.

        Returns:
            A list of objects from the collection.
        """
        full_name = self.make_qualified_collection_name(collection_name)
        collection = self.db_client.collections.get(full_name)
        response = collection.query.fetch_objects(
            limit=limit,
            return_properties=properties,
        )
        return [{"properties": obj.properties, "uuid": str(obj.uuid)} for obj in response.objects]

    def create_object(self, obj: Dict[str, Any], collection_name: str) -> None:
        """Create a Weaviate object.

        Args:
            obj: The object to create.
            collection_name: The name of the collection to create the object in.
        """
        full_name = self.make_qualified_collection_name(collection_name)
        collection = self.db_client.collections.get(full_name)
        collection.data.insert(properties=obj)

    def drop_storage(self) -> None:
        """Drop the dataset from Weaviate instance.

        Deletes all collections in the dataset and all data associated with them.
        Deletes the sentinel collection as well.

        If dataset name was not provided, it deletes all the tables in the current schema
        """
        collections = self.db_client.collections.list_all()
        collection_names = list(collections.keys())

        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"

            for collection_name in collection_names:
                if collection_name.startswith(prefix):
                    self.db_client.collections.delete(collection_name)
        else:
            # in case of no dataset prefix do our best and delete all tables in the schema
            for table_name in self.schema.tables.keys():
                if table_name in collection_names:
                    self.db_client.collections.delete(table_name)

        self._delete_sentinel_collection()

    @wrap_weaviate_error
    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self._create_sentinel_collection()
        elif truncate_tables:
            for table_name in truncate_tables:
                try:
                    collection_schema = self.get_collection_schema(table_name)
                except UnexpectedStatusCodeError as e:
                    if hasattr(e, "status_code") and e.status_code == 404:
                        continue
                    raise
                except Exception as e:
                    if "404" in str(e) or "not found" in str(e).lower():
                        continue
                    raise

                self.delete_collection(table_name)
                self.create_collection(
                    collection_schema, full_collection_name=collection_schema["class"]
                )

    @wrap_weaviate_error
    def is_storage_initialized(self) -> bool:
        try:
            collections = self.db_client.collections.list_all()
            return self.sentinel_collection in collections
        except Exception:
            return False

    def _create_sentinel_collection(self) -> None:
        """Create an empty collection to indicate that the storage is initialized."""
        self.db_client.collections.create(
            name=self.sentinel_collection,
            vectorizer_config=Configure.Vectorizer.none(),
        )

    def _delete_sentinel_collection(self) -> None:
        """Delete the sentinel collection."""
        try:
            self.db_client.collections.delete(self.sentinel_collection)
        except Exception:
            pass  # Ignore if not found

    @wrap_weaviate_error
    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        applied_update = super().update_stored_schema(only_tables, expected_update)
        # Retrieve the schema from Weaviate
        try:
            schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        except DestinationUndefinedEntity:
            schema_info = None
        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                "not found in the storage. upgrading"
            )
            # TODO: return a real updated table schema (like in SQL job client)
            self._execute_schema_update(only_tables)
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"inserted at {schema_info.inserted_at} found "
                "in storage, no upgrade required"
            )

        return applied_update

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables.keys():
            exists, existing_columns = self.get_storage_table(table_name)
            # TODO: detect columns where vectorization was added or removed and modify it. currently we ignore change of hints
            new_columns = self.schema.get_new_table_columns(
                table_name,
                existing_columns,
                case_sensitive=self.capabilities.generates_case_sensitive_identifiers(),
            )
            logger.info(f"Found {len(new_columns)} updates for {table_name} in {self.schema.name}")
            if len(new_columns) > 0:
                if exists:
                    is_collection_vectorized = self._is_collection_vectorized(table_name)
                    for column in new_columns:
                        prop = self._make_property_schema(
                            column["name"], column, is_collection_vectorized
                        )
                        self.add_property_to_collection(table_name, prop)
                else:
                    collection_config = self.make_weaviate_collection_schema(table_name)
                    self.create_collection(collection_config)
        self._update_schema_in_storage(self.schema)

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_schema: TTableSchemaColumns = {}

        try:
            collection_schema = self.get_collection_schema(table_name)
        except UnexpectedStatusCodeError as e:
            if hasattr(e, "status_code") and e.status_code == 404:
                return False, table_schema
            raise
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                return False, table_schema
            raise

        # Convert Weaviate collection schema to dlt table schema
        for prop in collection_schema["properties"]:
            schema_c: TColumnSchema = {
                "name": prop["name"],
                **self._from_db_type(prop["dataType"][0], None, None),
            }
            table_schema[prop["name"]] = schema_c
        return True, table_schema

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage"""
        # normalize properties
        p_load_id = self.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID)
        p_dlt_load_id = self.schema.naming.normalize_identifier(C_DLT_LOAD_ID)
        p_pipeline_name = self.schema.naming.normalize_identifier("pipeline_name")
        p_status = self.schema.naming.normalize_identifier("status")

        # we need to find a stored state that matches a load id that was completed
        # we retrieve the state in blocks of 10 for this
        stepsize = 10
        offset = 0
        while True:
            state_records = self.get_records(
                self.schema.state_table_name,
                # search by package load id which is guaranteed to increase over time
                sort_by=p_dlt_load_id,
                sort_order="desc",
                filter_field=p_pipeline_name,
                filter_value=pipeline_name,
                limit=stepsize,
                offset=offset,
                properties=self.pipeline_state_properties,
            )
            offset += stepsize
            if len(state_records) == 0:
                return None
            for state in state_records:
                load_id = state[p_dlt_load_id]
                load_records = self.get_records(
                    self.schema.loads_table_name,
                    filter_field=p_load_id,
                    filter_value=load_id,
                    limit=1,
                    properties=[p_load_id, p_status],
                )
                # if there is a load for this state which was successful, return the state
                if len(load_records):
                    return StateInfo(**state)

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage"""
        p_schema_name = self.schema.naming.normalize_identifier("schema_name")
        p_inserted_at = self.schema.naming.normalize_identifier("inserted_at")

        try:
            records = self.get_records(
                self.schema.version_table_name,
                sort_by=p_inserted_at,
                sort_order="desc",
                filter_field=p_schema_name if schema_name else None,
                filter_value=schema_name if schema_name else None,
                limit=1,
            )
            if records:
                return StorageSchemaInfo(**records[0])
            return None
        except Exception:
            return None

    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        p_version_hash = self.schema.naming.normalize_identifier("version_hash")
        try:
            records = self.get_records(
                self.schema.version_table_name,
                filter_field=p_version_hash,
                filter_value=schema_hash,
                limit=1,
            )
            if records:
                return StorageSchemaInfo(**records[0])
            return None
        except Exception:
            return None

    @wrap_weaviate_error
    def get_records(
        self,
        table_name: str,
        filter_field: str = None,
        filter_value: Any = None,
        sort_by: str = None,
        sort_order: str = "asc",
        limit: int = 0,
        offset: int = 0,
        properties: List[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get records from a collection using v4 API"""
        # fail if collection does not exist
        self.get_collection_schema(table_name)

        full_name = self.make_qualified_collection_name(table_name)
        collection = self.db_client.collections.get(full_name)

        # build query
        if not properties:
            properties = list(self.schema.get_table_columns(table_name).keys())

        # Build filter if provided
        filters = None
        if filter_field and filter_value is not None:
            filters = Filter.by_property(filter_field).equal(filter_value)

        # Build sort if provided
        sort = None
        if sort_by:
            if sort_order == "desc":
                sort = Sort.by_property(sort_by, ascending=False)
            else:
                sort = Sort.by_property(sort_by, ascending=True)

        # Execute query
        response = collection.query.fetch_objects(
            filters=filters,
            sort=sort,
            limit=limit if limit > 0 else None,
            offset=offset if offset > 0 else None,
            return_properties=properties,
        )

        records = [obj.properties for obj in response.objects]
        if records is None:
            raise DestinationTransientException(f"Could not obtain records for `{full_name}`")
        return cast(List[Dict[str, Any]], records)

    def make_weaviate_collection_schema(self, table_name: str) -> Dict[str, Any]:
        """Creates a Weaviate collection schema from a table schema."""
        collection_schema: Dict[str, Any] = {
            "class": table_name,
            "properties": self._make_properties(table_name),
        }

        # check if any column requires vectorization
        if self._is_collection_vectorized(table_name):
            collection_schema["vectorizer"] = self._vectorizer_config
            # Only include module config for the active vectorizer
            if self._module_config and self._vectorizer_config in self._module_config:
                collection_schema["moduleConfig"] = {
                    self._vectorizer_config: self._module_config[self._vectorizer_config]
                }
        else:
            collection_schema["vectorizer"] = "none"
            collection_schema["vectorIndexConfig"] = {"skip": True}

        return collection_schema

    def _is_collection_vectorized(self, table_name: str) -> bool:
        """Tells is any of the columns has vectorize hint set"""
        return (
            len(get_columns_names_with_prop(self.schema.get_table(table_name), VECTORIZE_HINT)) > 0
        )

    def _make_properties(self, table_name: str) -> List[Dict[str, Any]]:
        """Creates a Weaviate properties schema from a table schema.

        Args:
            table: The table name for which columns should be converted to properties
        """
        is_collection_vectorized = self._is_collection_vectorized(table_name)
        return [
            self._make_property_schema(column_name, column, is_collection_vectorized)
            for column_name, column in self.schema.get_table_columns(table_name).items()
        ]

    def _make_property_schema(
        self, column_name: str, column: TColumnSchema, is_collection_vectorized: bool
    ) -> Dict[str, Any]:
        extra_kv = {}

        vectorizer_name = self._vectorizer_config
        # x-weaviate-vectorize: (bool) means that this field should be vectorized
        if is_collection_vectorized and not column.get(VECTORIZE_HINT, False):
            # tell weaviate explicitly to not vectorize when column has no vectorize hint
            extra_kv["moduleConfig"] = {
                vectorizer_name: {
                    "skip": True,
                }
            }

        # x-weaviate-tokenization: (str) specifies the method to use
        # for tokenization
        if TOKENIZATION_HINT in column:
            extra_kv["tokenization"] = column[TOKENIZATION_HINT]  # type: ignore

        return {
            "name": column_name,
            "dataType": [self.type_mapper.to_destination_type(column, None)],
            **extra_kv,
        }

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        return LoadWeaviateJob(
            file_path,
            collection_name=self.make_qualified_collection_name(table["name"]),
        )

    @wrap_weaviate_error
    def complete_load(self, load_id: str) -> None:
        # corresponds to order of the columns in loads_table()
        values = [
            load_id,
            self.schema.name,
            0,
            pendulum.now().isoformat(),
            self.schema.version_hash,
        ]
        assert len(values) == len(self.loads_collection_properties)
        properties = {k: v for k, v in zip(self.loads_collection_properties, values)}
        self.create_object(properties, self.schema.loads_table_name)

    def __enter__(self) -> "WeaviateClient":
        self.db_client = self.create_db_client(self.config)
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        if self.db_client:
            self.db_client.close()
            self.db_client = None

    def _update_schema_in_storage(self, schema: Schema) -> None:
        schema_str = json.dumps(schema.to_dict())
        # corresponds to order of the columns in version_table()
        values = [
            schema.version,
            schema.ENGINE_VERSION,
            str(pendulum.now().isoformat()),
            schema.name,
            schema.stored_version_hash,
            schema_str,
        ]
        assert len(values) == len(self.version_collection_properties)
        properties = {k: v for k, v in zip(self.version_collection_properties, values)}
        self.create_object(properties, self.schema.version_table_name)

    def _from_db_type(
        self, wt_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(wt_t, precision, scale)

    # Backwards compatibility aliases
    @property
    def sentinel_class(self) -> str:
        """Alias for sentinel_collection for backwards compatibility"""
        return self.sentinel_collection

    def make_qualified_class_name(self, table_name: str) -> str:
        """Alias for make_qualified_collection_name for backwards compatibility"""
        return self.make_qualified_collection_name(table_name)

    def get_class_schema(self, table_name: str) -> Dict[str, Any]:
        """Alias for get_collection_schema for backwards compatibility"""
        return self.get_collection_schema(table_name)

    def create_class(
        self, class_schema: Dict[str, Any], full_class_name: Optional[str] = None
    ) -> None:
        """Alias for create_collection for backwards compatibility"""
        return self.create_collection(class_schema, full_class_name)

    def create_class_property(self, class_name: str, prop_schema: Dict[str, Any]) -> None:
        """Alias for add_property_to_collection for backwards compatibility"""
        return self.add_property_to_collection(class_name, prop_schema)

    def delete_class(self, class_name: str) -> None:
        """Alias for delete_collection for backwards compatibility"""
        return self.delete_collection(class_name)

    def query_class(self, class_name: str, properties: List[str]) -> Any:
        """Query a collection and return results in v3-compatible format for backwards compatibility"""
        full_name = self.make_qualified_collection_name(class_name)
        collection = self.db_client.collections.get(full_name)

        # Return a wrapper that provides .do() method for compatibility
        class QueryWrapper:
            def __init__(self, coll: Any, props: List[str]) -> None:
                self._collection = coll
                self._properties = props
                self._filters: Optional[Any] = None
                self._sort: Optional[Any] = None
                self._limit: Optional[int] = None
                self._offset: Optional[int] = None

            def with_where(self, where: Dict[str, Any]) -> "QueryWrapper":
                # Convert v3 where format to v4 filter
                path = where.get("path", [])
                operator = where.get("operator", "Equal")
                value = (
                    where.get("valueString")
                    or where.get("valueNumber")
                    or where.get("valueBoolean")
                )
                if path and value is not None:
                    prop_name = path[0]
                    if operator == "Equal":
                        self._filters = Filter.by_property(prop_name).equal(value)
                return self

            def with_sort(self, sort: Dict[str, Any]) -> "QueryWrapper":
                path = sort.get("path", [])
                order = sort.get("order", "asc")
                if path:
                    if order == "desc":
                        self._sort = Sort.by_property(path[0], ascending=False)
                    else:
                        self._sort = Sort.by_property(path[0], ascending=True)
                return self

            def with_limit(self, limit: int) -> "QueryWrapper":
                self._limit = limit
                return self

            def with_offset(self, offset: int) -> "QueryWrapper":
                self._offset = offset
                return self

            def do(self) -> Dict[str, Any]:
                response = self._collection.query.fetch_objects(
                    filters=self._filters,
                    sort=self._sort,
                    limit=self._limit,
                    offset=self._offset,
                    return_properties=self._properties,
                )
                # Convert to v3 format
                objects = [obj.properties for obj in response.objects]
                return {"data": {"Get": {self._collection.name: objects}}}

        return QueryWrapper(collection, properties)
