# Python internals
from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

# Other libraries
from attrs import define as _attrs_define, field as _attrs_field

from ..models.script_type import ScriptType
from ..types import UNSET, Unset

T = TypeVar("T", bound="CreateScriptRequest")


@_attrs_define
class CreateScriptRequest:
    """
    Attributes:
        description (str): The description of the script
        entry_point (str): The entry point of the script. Will usually be the path to a python file in the uploaded
            tarball
        name (str): The name of the script
        script_type (ScriptType):
        active (Union[Unset, bool]): Whether the script is active Default: True.
        default_profile_id_or_name (Union[Unset, str]): The ID or name of the profile to use for the script Default:
            'default'.
        schedule (Union[None, Unset, str]): The schedule of the script. Use 'cron' format for cron jobs
    """

    description: str
    entry_point: str
    name: str
    script_type: ScriptType
    active: Union[Unset, bool] = True
    default_profile_id_or_name: Union[Unset, str] = "default"
    schedule: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        description = self.description

        entry_point = self.entry_point

        name = self.name

        script_type = self.script_type.value

        active = self.active

        default_profile_id_or_name = self.default_profile_id_or_name

        schedule: Union[None, Unset, str]
        if isinstance(self.schedule, Unset):
            schedule = UNSET
        else:
            schedule = self.schedule

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "description": description,
                "entry_point": entry_point,
                "name": name,
                "script_type": script_type,
            }
        )
        if active is not UNSET:
            field_dict["active"] = active
        if default_profile_id_or_name is not UNSET:
            field_dict["default_profile_id_or_name"] = default_profile_id_or_name
        if schedule is not UNSET:
            field_dict["schedule"] = schedule

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        description = d.pop("description")

        entry_point = d.pop("entry_point")

        name = d.pop("name")

        script_type = ScriptType(d.pop("script_type"))

        active = d.pop("active", UNSET)

        default_profile_id_or_name = d.pop("default_profile_id_or_name", UNSET)

        def _parse_schedule(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        schedule = _parse_schedule(d.pop("schedule", UNSET))

        create_script_request = cls(
            description=description,
            entry_point=entry_point,
            name=name,
            script_type=script_type,
            active=active,
            default_profile_id_or_name=default_profile_id_or_name,
            schedule=schedule,
        )

        create_script_request.additional_properties = d
        return create_script_request

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
