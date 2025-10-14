import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="ConfigurationResponse")


@_attrs_define
class ConfigurationResponse:
    """
    Attributes:
        config (str): The configuration of the profile
        created_by (UUID): The ID of the identity who created the configuration
        date_added (datetime.datetime): The date the entity was added
        date_updated (datetime.datetime): The date the entity was updated
        id (UUID): The uniqueID of the entity
        profile_id (UUID): The ID of the profile the configuration belongs to
        secrets (str): The secrets of the profile
        version (int): The version of the configuration
    """

    config: str
    created_by: UUID
    date_added: datetime.datetime
    date_updated: datetime.datetime
    id: UUID
    profile_id: UUID
    secrets: str
    version: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        config = self.config

        created_by = str(self.created_by)

        date_added = self.date_added.isoformat()

        date_updated = self.date_updated.isoformat()

        id = str(self.id)

        profile_id = str(self.profile_id)

        secrets = self.secrets

        version = self.version

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "config": config,
                "created_by": created_by,
                "date_added": date_added,
                "date_updated": date_updated,
                "id": id,
                "profile_id": profile_id,
                "secrets": secrets,
                "version": version,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        config = d.pop("config")

        created_by = UUID(d.pop("created_by"))

        date_added = isoparse(d.pop("date_added"))

        date_updated = isoparse(d.pop("date_updated"))

        id = UUID(d.pop("id"))

        profile_id = UUID(d.pop("profile_id"))

        secrets = d.pop("secrets")

        version = d.pop("version")

        configuration_response = cls(
            config=config,
            created_by=created_by,
            date_added=date_added,
            date_updated=date_updated,
            id=id,
            profile_id=profile_id,
            secrets=secrets,
            version=version,
        )

        configuration_response.additional_properties = d
        return configuration_response

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
