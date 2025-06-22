from copy import deepcopy
from typing import Type, TYPE_CHECKING

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations.impl.cratedb.configuration import CrateDbClientConfiguration
from dlt.destinations.impl.cratedb.utils import escape_cratedb_literal
from dlt.destinations.impl.postgres.factory import PostgresTypeMapper, postgres

if TYPE_CHECKING:
    from dlt.destinations.impl.cratedb.cratedb import CrateDbClient


class CrateDbTypeMapper(PostgresTypeMapper):
    """
    Adjust type mappings for CrateDB.

    - CrateDB uses `object(dynamic)` instead of `json` or `jsonb`.
    - CrateDB does not support `timestamp(6) without time zone`.
    - CrateDB does not support `time` for storing.
    - CrateDB does not support `binary` or `bytea`.
    """

    def __new__(cls, *args, **kwargs):
        cls.sct_to_unbound_dbt = deepcopy(PostgresTypeMapper.sct_to_unbound_dbt)
        cls.sct_to_unbound_dbt["json"] = "object(dynamic)"
        cls.sct_to_unbound_dbt["binary"] = "text"

        cls.sct_to_dbt = deepcopy(PostgresTypeMapper.sct_to_dbt)
        cls.sct_to_dbt["timestamp"] = "timestamp with time zone"
        del cls.sct_to_dbt["time"]

        cls.dbt_to_sct = deepcopy(PostgresTypeMapper.dbt_to_sct)
        cls.dbt_to_sct["jsonb"] = "object(dynamic)"
        cls.dbt_to_sct["bytea"] = "text"

        return super().__new__(cls)

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        """
        CrateDB does not support `timestamp(6) without time zone`.
        To not render the SQL clause like this, nullify the `precision` attribute.
        """
        column["precision"] = None
        return super().to_db_datetime_type(column, table)


class cratedb(postgres, Destination[CrateDbClientConfiguration, "CrateDbClient"]):
    spec = CrateDbClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        """
        Tune down capabilities for CrateDB.
        """
        caps = super()._raw_capabilities()

        # CrateDB's type mapping needs adjustments compared to PostgreSQL.
        caps.type_mapper = CrateDbTypeMapper
        # CrateDB does not support transactions.
        caps.supports_transactions = False
        caps.supports_ddl_transactions = False

        # CrateDB does not support `TRUNCATE TABLE`, use `DELETE FROM` instead.
        caps.supports_truncate_command = False

        # CrateDB needs a slightly adjusted escaping of literals.
        # TODO: Escaping might need further adjustments, to be explored using integration tests.
        caps.escape_literal = escape_cratedb_literal

        # CrateDB does not support direct data loading using advanced formats.
        # TODO: Explore adding more formats for staged imports.
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values"]
        caps.loader_file_format_selector = None

        return caps

    @property
    def client_class(self) -> Type["CrateDbClient"]:
        """
        Provide a different client for CrateDB.
        """
        from dlt.destinations.impl.cratedb.cratedb import CrateDbClient

        return CrateDbClient


cratedb.register()
