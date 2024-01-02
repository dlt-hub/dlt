import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

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
        create_indexes: bool = True,
        staging_iam_role: t.Optional[str] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Redshift destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the redshift database. Can be an instance of `RedshiftCredentials` or
                a connection string in the format `redshift://user:password@host:port/database`
            create_indexes: Should unique indexes be created
            staging_iam_role: IAM role to use for staging data in S3
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            staging_iam_role=staging_iam_role,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
