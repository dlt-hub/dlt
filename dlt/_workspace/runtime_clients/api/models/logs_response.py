from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.detailed_run_response import DetailedRunResponse


T = TypeVar("T", bound="LogsResponse")


@_attrs_define
class LogsResponse:
    """
    Attributes:
        run (DetailedRunResponse):
        logs (Union[None, Unset, str]): The logs of the run. Set to none if no logs are available
    """

    run: "DetailedRunResponse"
    logs: Union[None, Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.detailed_run_response import DetailedRunResponse

        run = self.run.to_dict()

        logs: Union[None, Unset, str]
        if isinstance(self.logs, Unset):
            logs = UNSET
        else:
            logs = self.logs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "run": run,
            }
        )
        if logs is not UNSET:
            field_dict["logs"] = logs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.detailed_run_response import DetailedRunResponse

        d = dict(src_dict)
        run = DetailedRunResponse.from_dict(d.pop("run"))

        def _parse_logs(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        logs = _parse_logs(d.pop("logs", UNSET))

        logs_response = cls(
            run=run,
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
