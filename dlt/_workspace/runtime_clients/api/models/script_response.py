import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, Union, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="ScriptResponse")


@_attrs_define
class ScriptResponse:
    """
    Attributes:
        date_added (datetime.datetime): The date the entity was added
        date_updated (datetime.datetime): The date the entity was updated
        description (str): The description of the script
        id (UUID): The uniqueID of the entity
        name (str): The name of the script
        workspace_id (UUID): The ID of the workspace the script belongs to
        active (Union[Unset, bool]): Whether the script is active Default: True.
    """

    date_added: datetime.datetime
    date_updated: datetime.datetime
    description: str
    id: UUID
    name: str
    workspace_id: UUID
    active: Union[Unset, bool] = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        date_added = self.date_added.isoformat()

        date_updated = self.date_updated.isoformat()

        description = self.description

        id = str(self.id)

        name = self.name

        workspace_id = str(self.workspace_id)

        active = self.active

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "date_added": date_added,
                "date_updated": date_updated,
                "description": description,
                "id": id,
                "name": name,
                "workspace_id": workspace_id,
            }
        )
        if active is not UNSET:
            field_dict["active"] = active

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        date_added = isoparse(d.pop("date_added"))

        date_updated = isoparse(d.pop("date_updated"))

        description = d.pop("description")

        id = UUID(d.pop("id"))

        name = d.pop("name")

        workspace_id = UUID(d.pop("workspace_id"))

        active = d.pop("active", UNSET)

        script_response = cls(
            date_added=date_added,
            date_updated=date_updated,
            description=description,
            id=id,
            name=name,
            workspace_id=workspace_id,
            active=active,
        )

        script_response.additional_properties = d
        return script_response

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
