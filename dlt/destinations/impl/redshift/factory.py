import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.redshift.configuration import RedshiftCredentials, RedshiftClientConfiguration
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
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials, create_indexes=create_indexes, staging_iam_role=staging_iam_role, **kwargs
        )
