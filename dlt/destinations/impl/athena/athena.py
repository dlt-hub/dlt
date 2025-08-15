from typing import (
    Optional,
    Any,
    Sequence,
    Tuple,
    List,
    Dict,
    Iterable,
    Union,
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from mypy_boto3_lakeformation import LakeFormationClient
    from mypy_boto3_lakeformation.type_defs import (
        AddLFTagsToResourceResponseTypeDef,
        GetResourceLFTagsResponseTypeDef,
        RemoveLFTagsFromResourceResponseTypeDef,
        ResourceTypeDef,
    )
else:
    LakeFormationClient = Any
    AddLFTagsToResourceResponseTypeDef = Any
    GetResourceLFTagsResponseTypeDef = Any
    RemoveLFTagsFromResourceResponseTypeDef = Any
    ResourceTypeDef = Any

from threading import Lock

from dlt.common import logger
from dlt.common.utils import uniq_id
from dlt.common.schema import TColumnSchema, Schema
from dlt.common.schema.typing import (
    TColumnType,
    TSchemaTables,
    TSortOrder,
)
from dlt.common.destination import DestinationCapabilitiesContext, PreparedTableSchema
from dlt.common.destination.client import FollowupJobRequest, SupportsStagingDestination, LoadJob
from dlt.destinations.sql_jobs import (
    SqlStagingCopyFollowupJob,
    SqlStagingReplaceFollowupJob,
    SqlMergeFollowupJob,
)

from dlt.destinations import path_utils
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.utils import get_deterministic_temp_table_name
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs, FinalizedLoadJob
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration
from dlt.destinations.impl.athena.athena_adapter import PARTITION_HINT
from dlt.destinations.impl.athena.configuration import LakeformationConfig
from dlt.destinations.impl.athena.sql_client import AthenaSQLClient


boto3_client_lock = Lock()


class AthenaMergeJob(SqlMergeFollowupJob):
    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        # reproducible name so we know which table to drop
        with sql_client.with_staging_dataset():
            return sql_client.make_qualified_table_name(
                cls._shorten_table_name(
                    get_deterministic_temp_table_name(table_name, op), sql_client
                )
            )

    @classmethod
    def _to_temp_table(cls, select_sql: str, temp_table_name: str, unique_column: str) -> str:
        # regular table because Athena does not support temporary tables
        return f"CREATE TABLE {temp_table_name} AS {select_sql}"

    @classmethod
    def gen_insert_temp_table_sql(
        cls,
        table_name: str,
        staging_root_table_name: str,
        sql_client: SqlClientBase[Any],
        primary_keys: Sequence[str],
        unique_column: str,
        dedup_sort: Tuple[str, TSortOrder] = None,
        condition: str = None,
        condition_columns: Sequence[str] = None,
        skip_dedup: bool = False,
    ) -> Tuple[List[str], str]:
        sql, temp_table_name = super().gen_insert_temp_table_sql(
            table_name,
            staging_root_table_name,
            sql_client,
            primary_keys,
            unique_column,
            dedup_sort,
            condition,
            condition_columns,
            skip_dedup,
        )
        # DROP needs backtick as escape identifier
        sql.insert(0, f"""DROP TABLE IF EXISTS {temp_table_name.replace('"', '`')}""")
        return sql, temp_table_name

    @classmethod
    def gen_delete_temp_table_sql(
        cls,
        table_name: str,
        unique_column: str,
        key_table_clauses: Sequence[str],
        sql_client: SqlClientBase[Any],
    ) -> Tuple[List[str], str]:
        sql, temp_table_name = super().gen_delete_temp_table_sql(
            table_name, unique_column, key_table_clauses, sql_client
        )
        # DROP needs backtick as escape identifier
        sql.insert(0, f"""DROP TABLE IF EXISTS {temp_table_name.replace('"', '`')}""")
        return sql, temp_table_name

    @classmethod
    def gen_concat_sql(cls, columns: Sequence[str]) -> str:
        # Athena requires explicit casting
        columns = [f"CAST({c} AS VARCHAR)" for c in columns]
        return f"CONCAT({', '.join(columns)})"

    @classmethod
    def requires_temp_table_for_delete(cls) -> bool:
        return True


class LfTagsManager:
    """Class used to manage lakeformation tags on glue/athena database.

    Heavily inspired by dbt-athena implementation:
    https://github.com/dbt-labs/dbt-athena/blob/main/dbt-athena/src/dbt/adapters/athena/lakeformation.py
    """

    def __init__(
        self,
        lf_client: LakeFormationClient,
        lf_config: LakeformationConfig,
        dataset: str,
        table: Optional[str] = None,
    ):
        self.lf_client = lf_client
        self.database = dataset
        self.table = table
        self.enabled = lf_config.enabled
        self.lf_tags = lf_config.tags

    def process_lf_tags_database(self) -> None:
        """Add or remove lf tags from athena database."""
        database_resource: ResourceTypeDef = {"Database": {"Name": self.database}}
        if self.enabled and self.lf_tags:
            add_response = self.lf_client.add_lf_tags_to_resource(
                Resource={"Database": {"Name": self.database}},
                LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in self.lf_tags.items()],
            )
            self._parse_and_log_lf_response(add_response, None, self.lf_tags)
        elif not self.enabled:
            get_response: GetResourceLFTagsResponseTypeDef = self.lf_client.get_resource_lf_tags(
                Resource=database_resource
            )
            if "LFTagOnDatabase" not in get_response:
                return
            tags = get_response["LFTagOnDatabase"]
            self.lf_client.remove_lf_tags_from_resource(Resource=database_resource, LFTags=tags)

        else:
            logger.debug("Lakeformation is enabled but no tags are set")

    def _parse_and_log_lf_response(
        self,
        response: Union[
            AddLFTagsToResourceResponseTypeDef, RemoveLFTagsFromResourceResponseTypeDef
        ],
        columns: Optional[List[str]] = None,
        lf_tags: Optional[Dict[str, str]] = None,
        verb: str = "add",
    ) -> None:
        table_appendix = f".{self.table}" if self.table else ""
        columns_appendix = f" for columns {columns}" if columns else ""
        resource_msg = self.database + table_appendix + columns_appendix
        if failures := response.get("Failures", []):
            base_msg = f"Failed to {verb} LF tags: {lf_tags} to " + resource_msg
            for failure in failures:
                tag = failure.get("LFTag", {}).get("TagKey")
                error = failure.get("Error", {}).get("ErrorMessage")
                logger.error(f"Failed to {verb} {tag} for " + resource_msg + f" - {error}")
            raise RuntimeError(base_msg)
        logger.debug(f"Success: {verb} LF tags {lf_tags} to " + resource_msg)


class AthenaClient(SqlJobClientWithStagingDataset, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: AthenaClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # verify if staging layout is valid for Athena
        # this will raise if the table prefix is not properly defined
        # we actually make sure that {table_name} is first, no {schema_name} is allowed
        if config.staging_config:
            self.table_prefix_layout = path_utils.get_table_prefix_layout(
                config.staging_config.layout,
                supported_prefix_placeholders=[],
                table_needs_own_folder=True,
            )

        sql_client = AthenaSQLClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.sql_client: AthenaSQLClient = sql_client  # type: ignore
        self.config: AthenaClientConfiguration = config
        self.type_mapper = self.capabilities.get_type_mapper()

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # only truncate tables in iceberg mode
        truncate_tables = []
        super().initialize_storage(truncate_tables)

    def _from_db_type(
        self, hive_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(hive_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        return (
            f"{self.sql_client.escape_ddl_identifier(c['name'])} {self.type_mapper.to_destination_type(c, table)}"
        )

    def _iceberg_partition_clause(self, partition_hints: Optional[Dict[str, str]]) -> str:
        if not partition_hints:
            return ""
        formatted_strings = []
        for column_name, template in partition_hints.items():
            formatted_strings.append(
                template.format(column_name=self.sql_client.escape_ddl_identifier(column_name))
            )
        return f"PARTITIONED BY ({', '.join(formatted_strings)})"

    def _iceberg_table_properties(self) -> str:
        table_properties = (
            self.config.table_properties.copy() if self.config.table_properties else {}
        )
        mandatory_properties = {"table_type": "ICEBERG", "format": "parquet"}
        table_properties.update(mandatory_properties)
        return ", ".join([f"'{k}'='{v}'" for k, v in table_properties.items()])

    def manage_lf_tags(self) -> None:
        """Manage Lakeformation tags on the glue database."""
        lf_config = self.config.lakeformation_config

        with boto3_client_lock:
            lf_client = self.config.credentials._to_botocore_session().create_client(
                "lakeformation"
            )

        manager = LfTagsManager(
            lf_client,
            lf_config=lf_config,
            dataset=self.sql_client.dataset_name,
            table=None,
        )
        try:
            manager.process_lf_tags_database()
        except Exception as e:
            logger.error(f"Failed to manage lakeformation tags: {e}")
            raise e

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        bucket = self.config.staging_config.bucket_url
        dataset = self.sql_client.dataset_name

        sql: List[str] = []

        # for the system tables we need to create empty iceberg tables to be able to run, DELETE and UPDATE queries
        # or if we are in iceberg mode, we create iceberg tables for all tables
        table = self.prepare_load_table(table_name)
        # do not create iceberg tables on staging dataset
        create_iceberg = self._is_iceberg_table(table, self.in_staging_dataset_mode)
        columns = ", ".join([self._get_column_def_sql(c, table) for c in new_columns])

        # use qualified table names
        qualified_table_name = self.sql_client.make_qualified_ddl_table_name(table_name)
        if generate_alter:
            # alter table to add new columns at the end
            sql.append(f"""ALTER TABLE {qualified_table_name} ADD COLUMNS ({columns})""")
        else:
            table_prefix = self.table_prefix_layout.format(table_name=table_name)
            if create_iceberg:
                partition_clause = self._iceberg_partition_clause(
                    cast(Optional[Dict[str, str]], table.get(PARTITION_HINT))
                )
                # create unique tag for iceberg table so it is never recreated in the same folder
                # athena requires some kind of special cleaning (or that is a bug) so we cannot refresh
                # iceberg tables without it, by default tag is not added
                location = f"{bucket}/" + self.config.table_location_layout.format(
                    dataset_name=dataset,
                    table_name=table_prefix.rstrip("/"),
                    location_tag=uniq_id(6),
                )
                table_properties = self._iceberg_table_properties()
                logger.info(f"Will create ICEBERG table {table_name} in {location}")
                # this will fail if the table prefix is not properly defined
                sql.append(f"""{self._make_create_table(qualified_table_name, table)}
                        ({columns})
                        {partition_clause}
                        LOCATION '{location.rstrip('/')}'
                        TBLPROPERTIES ({table_properties})""")
            # elif table_format == "jsonl":
            #     sql.append(f"""CREATE EXTERNAL TABLE {qualified_table_name}
            #             ({columns})
            #             ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            #             LOCATION '{location}'""")
            else:
                # external tables always here
                location = f"{bucket}/{dataset}/{table_prefix}"
                logger.info(f"Will create EXTERNAL table {table_name} from {location}")
                sql.append(f"""CREATE EXTERNAL TABLE {qualified_table_name}
                        ({columns})
                        STORED AS PARQUET
                        LOCATION '{location}'""")
        return sql

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        applied_update = super().update_stored_schema(only_tables, expected_update=expected_update)
        # here we could apply tags only if any migration happened, right now we do it on each run
        # NOTE: tags are applied before any data is loaded
        if self.config.lakeformation_config is not None:
            self.manage_lf_tags()
        return applied_update

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            job = (
                FinalizedLoadJobWithFollowupJobs(file_path)
                if self._is_iceberg_table(table)
                else FinalizedLoadJob(file_path)
            )
        return job

    def _create_append_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        if self._is_iceberg_table(table_chain[0]):
            return [SqlStagingCopyFollowupJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_append_followup_jobs(table_chain)

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        if self._is_iceberg_table(table_chain[0]):
            return [SqlStagingReplaceFollowupJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [AthenaMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _is_iceberg_table(
        self, table: PreparedTableSchema, is_staging_dataset: bool = False
    ) -> bool:
        table_format = table.get("table_format")
        # all dlt tables that are not loaded via files are iceberg tables, no matter if they are on staging or regular dataset
        # all other iceberg tables are HIVE (external) tables on staging dataset
        table_format_iceberg = table_format == "iceberg"
        return (table_format_iceberg and not is_staging_dataset) or table[
            "write_disposition"
        ] == "skip"

    def should_load_data_to_staging_dataset(self, table_name: str) -> bool:
        # all iceberg tables need staging
        table = self.prepare_load_table(table_name)
        if self._is_iceberg_table(table):
            return True
        return super().should_load_data_to_staging_dataset(table_name)

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        # on athena we only truncate replace tables that are not iceberg
        table = self.prepare_load_table(table_name)
        if table["write_disposition"] == "replace" and not self._is_iceberg_table(table):
            return True
        return False

    def should_load_data_to_staging_dataset_on_staging_destination(self, table_name: str) -> bool:
        """iceberg table data goes into staging on staging destination"""
        table = self.prepare_load_table(table_name)
        if self._is_iceberg_table(table):
            return True
        return super().should_load_data_to_staging_dataset_on_staging_destination(table_name)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        from pyathena.error import Error

        return isinstance(ex, Error)
