import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.common.normalizers.naming import NamingConvention
from dlt.destinations.impl.redshift.configuration import (
    RedshiftCredentials,
    RedshiftClientConfiguration,
)
from dlt.destinations.impl.redshift import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.redshift.redshift import RedshiftClient


class redshift(Destination[RedshiftClientConfiguration, "RedshiftClient"]):
    spec = RedshiftClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["RedshiftClient"]:
        from dlt.destinations.impl.redshift.redshift import RedshiftClient

        return RedshiftClient

    def __init__(
        self,
        credentials: t.Union[RedshiftCredentials, t.Dict[str, t.Any], str] = None,
        staging_iam_role: t.Optional[str] = None,
        has_case_sensitive_identifiers: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Redshift destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the redshift database. Can be an instance of `RedshiftCredentials` or
                a connection string in the format `redshift://user:password@host:port/database`
            staging_iam_role: IAM role to use for staging data in S3
            has_case_sensitive_identifiers: Are case sensitive identifiers enabled for a database
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            staging_iam_role=staging_iam_role,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: RedshiftClientConfiguration,
        naming: NamingConvention,
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)
