from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.organization_response import OrganizationResponse
    from ..models.workspace_response import WorkspaceResponse


T = TypeVar("T", bound="MeResponse")


@_attrs_define
class MeResponse:
    """
    Attributes:
        default_organization (OrganizationResponse): The default organization of the current user
        default_workspace (WorkspaceResponse): The default workspace of the current user
        email (str): The email of the current user
        identity_id (UUID): The ID of the current identity of the default organization for this user.
        user_id (UUID): The ID of the current user
    """

    default_organization: "OrganizationResponse"
    default_workspace: "WorkspaceResponse"
    email: str
    identity_id: UUID
    user_id: UUID
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.organization_response import OrganizationResponse
        from ..models.workspace_response import WorkspaceResponse

        default_organization = self.default_organization.to_dict()

        default_workspace = self.default_workspace.to_dict()

        email = self.email

        identity_id = str(self.identity_id)

        user_id = str(self.user_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "default_organization": default_organization,
                "default_workspace": default_workspace,
                "email": email,
                "identity_id": identity_id,
                "user_id": user_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.organization_response import OrganizationResponse
        from ..models.workspace_response import WorkspaceResponse

        d = dict(src_dict)
        default_organization = OrganizationResponse.from_dict(d.pop("default_organization"))

        default_workspace = WorkspaceResponse.from_dict(d.pop("default_workspace"))

        email = d.pop("email")

        identity_id = UUID(d.pop("identity_id"))

        user_id = UUID(d.pop("user_id"))

        me_response = cls(
            default_organization=default_organization,
            default_workspace=default_workspace,
            email=email,
            identity_id=identity_id,
            user_id=user_id,
        )

        me_response.additional_properties = d
        return me_response

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
