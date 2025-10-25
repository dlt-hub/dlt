from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.list_profile_versions_response_404_extra import (
        ListProfileVersionsResponse404Extra,
    )


T = TypeVar("T", bound="ListProfileVersionsResponse404")


@_attrs_define
class ListProfileVersionsResponse404:
    """Entity Not Found Exception

    Attributes:
        detail (str):
        status_code (int):
        extra (Union[Unset, ListProfileVersionsResponse404Extra]):
    """

    detail: str
    status_code: int
    extra: Union[Unset, "ListProfileVersionsResponse404Extra"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.list_profile_versions_response_404_extra import (
            ListProfileVersionsResponse404Extra,
        )

        detail = self.detail

        status_code = self.status_code

        extra: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.extra, Unset):
            extra = self.extra.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "detail": detail,
                "status_code": status_code,
            }
        )
        if extra is not UNSET:
            field_dict["extra"] = extra

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.list_profile_versions_response_404_extra import (
            ListProfileVersionsResponse404Extra,
        )

        d = dict(src_dict)
        detail = d.pop("detail")

        status_code = d.pop("status_code")

        _extra = d.pop("extra", UNSET)
        extra: Union[Unset, ListProfileVersionsResponse404Extra]
        if isinstance(_extra, Unset):
            extra = UNSET
        else:
            extra = ListProfileVersionsResponse404Extra.from_dict(_extra)

        list_profile_versions_response_404 = cls(
            detail=detail,
            status_code=status_code,
            extra=extra,
        )

        list_profile_versions_response_404.additional_properties = d
        return list_profile_versions_response_404

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
