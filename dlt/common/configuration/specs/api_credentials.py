from typing import ClassVar, List, Union, Optional

from dlt.common.typing import TSecretValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec


@configspec
class OAuth2Credentials(CredentialsConfiguration):
    client_id: str
    client_secret: TSecretValue
    refresh_token: Optional[TSecretValue]
    scopes: Optional[List[str]] = None

    token: Optional[TSecretValue] = None
    """Access token"""

    # add refresh_token when generating config samples
    __config_gen_annotations__: ClassVar[List[str]] = ["refresh_token"]


    def auth(self, scopes: Union[str, List[str]] = None, redirect_url: str = None) -> None:
        """Authorizes the client using the available credentials

        Uses the `refresh_token` grant if refresh token is available. Note that `scopes` and `redirect_url` are ignored in this flow.
        Otherwise obtains refresh_token via web flow and authorization code grant.

        Sets `token` and `access_token` fields in the credentials on successful authorization.

        Args:
            scopes (Union[str, List[str]], optional): Additional scopes to add to configured scopes. To be used in web flow. Defaults to None.
            redirect_url (str, optional): Redirect url in case of web flow. Defaults to None.
        """
        raise NotImplementedError()

    def add_scopes(self, scopes: Union[str, List[str]]) -> None:
        if not self.scopes:
            if isinstance(scopes, str):
                self.scopes = [scopes]
            else:
                self.scopes = scopes
        else:
            if isinstance(scopes, str):
                if scopes not in self.scopes:
                    self.scopes += [scopes]
            elif scopes:
                self.scopes = list(set(self.scopes + scopes))

