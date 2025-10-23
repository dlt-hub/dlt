# Python internals
from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast
from uuid import UUID

# Other libraries
from attrs import define as _attrs_define, field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="LogsResponse")


@_attrs_define
class LogsResponse:
    """
    Attributes:
        run_id (UUID): The ID of the run the logs belong to
        logs (Union[None, Unset, str]): The logs of the run. Set to none if no logs are available
    """

    run_id: UUID
    logs: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        run_id = str(self.run_id)

        logs: Union[None, Unset, str]
        if isinstance(self.logs, Unset):
            logs = UNSET
        else:
            logs = self.logs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "run_id": run_id,
            }
        )
        if logs is not UNSET:
            field_dict["logs"] = logs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        run_id = UUID(d.pop("run_id"))

        def _parse_logs(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        logs = _parse_logs(d.pop("logs", UNSET))

        logs_response = cls(
            run_id=run_id,
            logs=logs,
        )

        logs_response.additional_properties = d
        return logs_response

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
