from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, TextIO, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="CreateRunRequest")


@_attrs_define
class CreateRunRequest:
    """
    Attributes:
        script_id_or_name (str): The ID or name of the script to run
        profile_id_or_name (None | str | Unset): The ID or name of the profile to use for the run, will default to the
            default profile of the script
    """

    script_id_or_name: str
    profile_id_or_name: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        script_id_or_name = self.script_id_or_name

        profile_id_or_name: None | str | Unset
        if isinstance(self.profile_id_or_name, Unset):
            profile_id_or_name = UNSET
        else:
            profile_id_or_name = self.profile_id_or_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "script_id_or_name": script_id_or_name,
            }
        )
        if profile_id_or_name is not UNSET:
            field_dict["profile_id_or_name"] = profile_id_or_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        script_id_or_name = d.pop("script_id_or_name")

        def _parse_profile_id_or_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        profile_id_or_name = _parse_profile_id_or_name(d.pop("profile_id_or_name", UNSET))

        create_run_request = cls(
            script_id_or_name=script_id_or_name,
            profile_id_or_name=profile_id_or_name,
        )

        create_run_request.additional_properties = d
        return create_run_request

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
