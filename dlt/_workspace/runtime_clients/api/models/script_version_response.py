from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, TextIO, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.script_type import ScriptType
from ..types import UNSET, Unset

T = TypeVar("T", bound="ScriptVersionResponse")


@_attrs_define
class ScriptVersionResponse:
    """
    Attributes:
        active (bool): Whether the profile is active and may be used to run scripts
        created_by (UUID): The ID of the identity who created the profile
        date_added (datetime.datetime): The date the entity was added
        date_updated (datetime.datetime): The date the entity was updated
        default_profile_id (UUID): The ID of the profile to use for the script
        description (str): The description of the script
        entry_point (str): The entry point of the script. Will usually be the path to a python file in the uploaded
            tarball
        id (UUID): The uniqueID of the entity
        name (str): The name of the script
        script_id (UUID): The ID of the script the script version belongs to
        script_type (ScriptType):
        version (int): The current version of the profile
        schedule (None | str | Unset): The schedule of the script. Use 'cron' format for cron jobs
    """

    active: bool
    created_by: UUID
    date_added: datetime.datetime
    date_updated: datetime.datetime
    default_profile_id: UUID
    description: str
    entry_point: str
    id: UUID
    name: str
    script_id: UUID
    script_type: ScriptType
    version: int
    schedule: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        active = self.active

        created_by = str(self.created_by)

        date_added = self.date_added.isoformat()

        date_updated = self.date_updated.isoformat()

        default_profile_id = str(self.default_profile_id)

        description = self.description

        entry_point = self.entry_point

        id = str(self.id)

        name = self.name

        script_id = str(self.script_id)

        script_type = self.script_type.value

        version = self.version

        schedule: None | str | Unset
        if isinstance(self.schedule, Unset):
            schedule = UNSET
        else:
            schedule = self.schedule

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "active": active,
                "created_by": created_by,
                "date_added": date_added,
                "date_updated": date_updated,
                "default_profile_id": default_profile_id,
                "description": description,
                "entry_point": entry_point,
                "id": id,
                "name": name,
                "script_id": script_id,
                "script_type": script_type,
                "version": version,
            }
        )
        if schedule is not UNSET:
            field_dict["schedule"] = schedule

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        active = d.pop("active")

        created_by = UUID(d.pop("created_by"))

        date_added = isoparse(d.pop("date_added"))

        date_updated = isoparse(d.pop("date_updated"))

        default_profile_id = UUID(d.pop("default_profile_id"))

        description = d.pop("description")

        entry_point = d.pop("entry_point")

        id = UUID(d.pop("id"))

        name = d.pop("name")

        script_id = UUID(d.pop("script_id"))

        script_type = ScriptType(d.pop("script_type"))

        version = d.pop("version")

        def _parse_schedule(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        schedule = _parse_schedule(d.pop("schedule", UNSET))

        script_version_response = cls(
            active=active,
            created_by=created_by,
            date_added=date_added,
            date_updated=date_updated,
            default_profile_id=default_profile_id,
            description=description,
            entry_point=entry_point,
            id=id,
            name=name,
            script_id=script_id,
            script_type=script_type,
            version=version,
            schedule=schedule,
        )

        script_version_response.additional_properties = d
        return script_version_response

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
