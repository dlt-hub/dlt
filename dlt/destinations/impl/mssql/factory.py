import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.common.normalizers.naming.naming import NamingConvention
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials, MsSqlClientConfiguration
from dlt.destinations.impl.mssql import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.mssql.mssql import MsSqlJobClient


class mssql(Destination[MsSqlClientConfiguration, "MsSqlJobClient"]):
    spec = MsSqlClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["MsSqlJobClient"]:
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient

        return MsSqlJobClient

    def __init__(
        self,
        credentials: t.Union[MsSqlCredentials, t.Dict[str, t.Any], str] = None,
        create_indexes: bool = False,
        has_case_sensitive_identifiers: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the MsSql destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the mssql database. Can be an instance of `MsSqlCredentials` or
                a connection string in the format `mssql://user:password@host:port/database`
            create_indexes: Should unique indexes be created
            has_case_sensitive_identifiers: Are identifiers used by mssql database case sensitive (following the collation)
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: MsSqlClientConfiguration,
        naming: NamingConvention,
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)
