import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, Union, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.run_status import RunStatus
from ..models.run_trigger_type import RunTriggerType
from ..types import UNSET, Unset

T = TypeVar("T", bound="RunResponse")


@_attrs_define
class RunResponse:
    """
    Attributes:
        config_id (UUID): The ID of the configuration that will be used when running the script
        date_added (datetime.datetime): The date the entity was added
        date_updated (datetime.datetime): The date the entity was updated
        deployment_id (UUID): The ID of the deployment that will be used when running the script
        id (UUID): The uniqueID of the entity
        number (int): The number of the run. Will increment for each new run of the script
        script_version_id (UUID): The ID of the script version that will be used when running the script
        status (RunStatus):
        trigger (RunTriggerType):
        end_time (Union[None, Unset, datetime.datetime]): The time the run ended
        execution_time (Union[None, Unset, int]): The time the run took in seconds
        logs (Union[None, Unset, str]): A link to the logs of the run
        start_time (Union[None, Unset, datetime.datetime]): The time the run started
        triggered_by (Union[None, UUID, Unset]): The ID of the identity who triggered the run if triggered manually
    """

    config_id: UUID
    date_added: datetime.datetime
    date_updated: datetime.datetime
    deployment_id: UUID
    id: UUID
    number: int
    script_version_id: UUID
    status: RunStatus
    trigger: RunTriggerType
    end_time: Union[None, Unset, datetime.datetime] = UNSET
    execution_time: Union[None, Unset, int] = UNSET
    logs: Union[None, Unset, str] = UNSET
    start_time: Union[None, Unset, datetime.datetime] = UNSET
    triggered_by: Union[None, UUID, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        config_id = str(self.config_id)

        date_added = self.date_added.isoformat()

        date_updated = self.date_updated.isoformat()

        deployment_id = str(self.deployment_id)

        id = str(self.id)

        number = self.number

        script_version_id = str(self.script_version_id)

        status = self.status.value

        trigger = self.trigger.value

        end_time: Union[None, Unset, str]
        if isinstance(self.end_time, Unset):
            end_time = UNSET
        elif isinstance(self.end_time, datetime.datetime):
            end_time = self.end_time.isoformat()
        else:
            end_time = self.end_time

        execution_time: Union[None, Unset, int]
        if isinstance(self.execution_time, Unset):
            execution_time = UNSET
        else:
            execution_time = self.execution_time

        logs: Union[None, Unset, str]
        if isinstance(self.logs, Unset):
            logs = UNSET
        else:
            logs = self.logs

        start_time: Union[None, Unset, str]
        if isinstance(self.start_time, Unset):
            start_time = UNSET
        elif isinstance(self.start_time, datetime.datetime):
            start_time = self.start_time.isoformat()
        else:
            start_time = self.start_time

        triggered_by: Union[None, Unset, str]
        if isinstance(self.triggered_by, Unset):
            triggered_by = UNSET
        elif isinstance(self.triggered_by, UUID):
            triggered_by = str(self.triggered_by)
        else:
            triggered_by = self.triggered_by

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "config_id": config_id,
                "date_added": date_added,
                "date_updated": date_updated,
                "deployment_id": deployment_id,
                "id": id,
                "number": number,
                "script_version_id": script_version_id,
                "status": status,
                "trigger": trigger,
            }
        )
        if end_time is not UNSET:
            field_dict["end_time"] = end_time
        if execution_time is not UNSET:
            field_dict["execution_time"] = execution_time
        if logs is not UNSET:
            field_dict["logs"] = logs
        if start_time is not UNSET:
            field_dict["start_time"] = start_time
        if triggered_by is not UNSET:
            field_dict["triggered_by"] = triggered_by

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        config_id = UUID(d.pop("config_id"))

        date_added = isoparse(d.pop("date_added"))

        date_updated = isoparse(d.pop("date_updated"))

        deployment_id = UUID(d.pop("deployment_id"))

        id = UUID(d.pop("id"))

        number = d.pop("number")

        script_version_id = UUID(d.pop("script_version_id"))

        status = RunStatus(d.pop("status"))

        trigger = RunTriggerType(d.pop("trigger"))

        def _parse_end_time(data: object) -> Union[None, Unset, datetime.datetime]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                end_time_type_0 = isoparse(data)

                return end_time_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, datetime.datetime], data)

        end_time = _parse_end_time(d.pop("end_time", UNSET))

        def _parse_execution_time(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        execution_time = _parse_execution_time(d.pop("execution_time", UNSET))

        def _parse_logs(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        logs = _parse_logs(d.pop("logs", UNSET))

        def _parse_start_time(data: object) -> Union[None, Unset, datetime.datetime]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                start_time_type_0 = isoparse(data)

                return start_time_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, datetime.datetime], data)

        start_time = _parse_start_time(d.pop("start_time", UNSET))

        def _parse_triggered_by(data: object) -> Union[None, UUID, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                triggered_by_type_0 = UUID(data)

                return triggered_by_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, UUID, Unset], data)

        triggered_by = _parse_triggered_by(d.pop("triggered_by", UNSET))

        run_response = cls(
            config_id=config_id,
            date_added=date_added,
            date_updated=date_updated,
            deployment_id=deployment_id,
            id=id,
            number=number,
            script_version_id=script_version_id,
            status=status,
            trigger=trigger,
            end_time=end_time,
            execution_time=execution_time,
            logs=logs,
            start_time=start_time,
            triggered_by=triggered_by,
        )

        run_response.additional_properties = d
        return run_response

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
