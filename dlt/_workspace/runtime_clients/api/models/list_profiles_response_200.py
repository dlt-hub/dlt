# Python internals
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

# Other libraries
from attrs import define as _attrs_define, field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.profile_response import ProfileResponse


T = TypeVar("T", bound="ListProfilesResponse200")


@_attrs_define
class ListProfilesResponse200:
    """
    Attributes:
        items (Union[Unset, list['ProfileResponse']]):
        limit (Union[Unset, int]): Maximal number of items to send.
        offset (Union[Unset, int]): Offset from the beginning of the query.
        total (Union[Unset, int]): Total number of items.
    """

    items: Union[Unset, list["ProfileResponse"]] = UNSET
    limit: Union[Unset, int] = UNSET
    offset: Union[Unset, int] = UNSET
    total: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        items: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.items, Unset):
            items = []
            for items_item_data in self.items:
                items_item = items_item_data.to_dict()
                items.append(items_item)

        limit = self.limit

        offset = self.offset

        total = self.total

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if items is not UNSET:
            field_dict["items"] = items
        if limit is not UNSET:
            field_dict["limit"] = limit
        if offset is not UNSET:
            field_dict["offset"] = offset
        if total is not UNSET:
            field_dict["total"] = total

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.profile_response import ProfileResponse

        d = dict(src_dict)
        items = []
        _items = d.pop("items", UNSET)
        for items_item_data in _items or []:
            items_item = ProfileResponse.from_dict(items_item_data)

            items.append(items_item)

        limit = d.pop("limit", UNSET)

        offset = d.pop("offset", UNSET)

        total = d.pop("total", UNSET)

        list_profiles_response_200 = cls(
            items=items,
            limit=limit,
            offset=offset,
            total=total,
        )

        list_profiles_response_200.additional_properties = d
        return list_profiles_response_200

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
