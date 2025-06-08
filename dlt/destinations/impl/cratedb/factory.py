from typing import Type, TYPE_CHECKING

from dlt.common.data_writers import escape_redshift_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations.impl.cratedb.configuration import CrateDbClientConfiguration
from dlt.destinations.impl.postgres.factory import PostgresTypeMapper, postgres

if TYPE_CHECKING:
    from dlt.destinations.impl.cratedb.cratedb import CrateDbClient


class CrateDbTypeMapper(PostgresTypeMapper):
    """
    Adjust type mappings for CrateDB.

    - CrateDB does not support `timestamp(6) without time zone`.
    - CrateDB does not support `time` for storing.
    """

    # Override `timestamp`, disable `time`.
    sct_to_dbt = {
        "text": "varchar(%i)",
        # "timestamp": "timestamp (%i) with time zone",
        "timestamp": "timestamp with time zone",
        "decimal": "numeric(%i,%i)",
        # "time": "time (%i) without time zone",
        "wei": "numeric(%i,%i)",
    }

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

        # CrateDB does not support `TRUNCATE TABLE`, use `DELETE FROM` instead.
        caps.supports_truncate_command = False

        # CrateDB needs a slightly adjusted escaping of literals.
        # Examples: "L'Aupillon" and "Pizzas d'Anarosa" from `sys.summits`.
        # It possibly also doesn't support the `E'` prefix as employed by the PostgreSQL escaper?
        # Fortunately, the Redshift escaper came to the rescue, providing a reasonable baseline.
        # TODO: Escaping might need further adjustments, to be explored using integration tests.
        caps.escape_literal = escape_redshift_literal

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
