import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, Union, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="ProfileResponse")


@_attrs_define
class ProfileResponse:
    """
    Attributes:
        active (bool): Whether the profile is active and may be used to run scripts
        config (str): The config.toml of the profile
        created_by (UUID): The ID of the identity who created the profile
        date_added (datetime.datetime): The date the entity was added
        date_updated (datetime.datetime): The date the entity was updated
        id (UUID): The uniqueID of the entity
        name (str): The name of the profile
        secrets (str): The secrets.toml of the profile
        version (int): The current version of the profile
        workspace_id (UUID): The ID of the workspace the profile belongs to
        description (Union[None, Unset, str]): The description of the profile
    """

    active: bool
    config: str
    created_by: UUID
    date_added: datetime.datetime
    date_updated: datetime.datetime
    id: UUID
    name: str
    secrets: str
    version: int
    workspace_id: UUID
    description: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        active = self.active

        config = self.config

        created_by = str(self.created_by)

        date_added = self.date_added.isoformat()

        date_updated = self.date_updated.isoformat()

        id = str(self.id)

        name = self.name

        secrets = self.secrets

        version = self.version

        workspace_id = str(self.workspace_id)

        description: Union[None, Unset, str]
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "active": active,
                "config": config,
                "created_by": created_by,
                "date_added": date_added,
                "date_updated": date_updated,
                "id": id,
                "name": name,
                "secrets": secrets,
                "version": version,
                "workspace_id": workspace_id,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        active = d.pop("active")

        config = d.pop("config")

        created_by = UUID(d.pop("created_by"))

        date_added = isoparse(d.pop("date_added"))

        date_updated = isoparse(d.pop("date_updated"))

        id = UUID(d.pop("id"))

        name = d.pop("name")

        secrets = d.pop("secrets")

        version = d.pop("version")

        workspace_id = UUID(d.pop("workspace_id"))

        def _parse_description(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        description = _parse_description(d.pop("description", UNSET))

        profile_response = cls(
            active=active,
            config=config,
            created_by=created_by,
            date_added=date_added,
            date_updated=date_updated,
            id=id,
            name=name,
            secrets=secrets,
            version=version,
            workspace_id=workspace_id,
            description=description,
        )

        profile_response.additional_properties = d
        return profile_response

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
