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
        date_added (datetime.datetime): The date the entity was added
        date_updated (datetime.datetime): The date the entity was updated
        deployment_id (UUID): The ID of the deployment that will be used when running the script
        id (UUID): The uniqueID of the entity
        number (int): The number of the run. Will increment for each new run of the script
        profile_version_id (UUID): The ID of the profile version that will be used when running the script
        script_version_id (UUID): The ID of the script version that will be used when running the script
        status (RunStatus):
        trigger (RunTriggerType):
        workspace_id (UUID): The ID of the workspace the run belongs to
        duration (Union[None, Unset, int]): The time the run took in seconds
        logs (Union[None, Unset, str]): A link to the logs of the run
        time_ended (Union[None, Unset, datetime.datetime]): The time the run ended
        time_started (Union[None, Unset, datetime.datetime]): The time the run started
        triggered_by (Union[None, UUID, Unset]): The ID of the identity who triggered the run if triggered manually
    """

    date_added: datetime.datetime
    date_updated: datetime.datetime
    deployment_id: UUID
    id: UUID
    number: int
    profile_version_id: UUID
    script_version_id: UUID
    status: RunStatus
    trigger: RunTriggerType
    workspace_id: UUID
    duration: Union[None, Unset, int] = UNSET
    logs: Union[None, Unset, str] = UNSET
    time_ended: Union[None, Unset, datetime.datetime] = UNSET
    time_started: Union[None, Unset, datetime.datetime] = UNSET
    triggered_by: Union[None, UUID, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        date_added = self.date_added.isoformat()

        date_updated = self.date_updated.isoformat()

        deployment_id = str(self.deployment_id)

        id = str(self.id)

        number = self.number

        profile_version_id = str(self.profile_version_id)

        script_version_id = str(self.script_version_id)

        status = self.status.value

        trigger = self.trigger.value

        workspace_id = str(self.workspace_id)

        duration: Union[None, Unset, int]
        if isinstance(self.duration, Unset):
            duration = UNSET
        else:
            duration = self.duration

        logs: Union[None, Unset, str]
        if isinstance(self.logs, Unset):
            logs = UNSET
        else:
            logs = self.logs

        time_ended: Union[None, Unset, str]
        if isinstance(self.time_ended, Unset):
            time_ended = UNSET
        elif isinstance(self.time_ended, datetime.datetime):
            time_ended = self.time_ended.isoformat()
        else:
            time_ended = self.time_ended

        time_started: Union[None, Unset, str]
        if isinstance(self.time_started, Unset):
            time_started = UNSET
        elif isinstance(self.time_started, datetime.datetime):
            time_started = self.time_started.isoformat()
        else:
            time_started = self.time_started

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
                "date_added": date_added,
                "date_updated": date_updated,
                "deployment_id": deployment_id,
                "id": id,
                "number": number,
                "profile_version_id": profile_version_id,
                "script_version_id": script_version_id,
                "status": status,
                "trigger": trigger,
                "workspace_id": workspace_id,
            }
        )
        if duration is not UNSET:
            field_dict["duration"] = duration
        if logs is not UNSET:
            field_dict["logs"] = logs
        if time_ended is not UNSET:
            field_dict["time_ended"] = time_ended
        if time_started is not UNSET:
            field_dict["time_started"] = time_started
        if triggered_by is not UNSET:
            field_dict["triggered_by"] = triggered_by

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        date_added = isoparse(d.pop("date_added"))

        date_updated = isoparse(d.pop("date_updated"))

        deployment_id = UUID(d.pop("deployment_id"))

        id = UUID(d.pop("id"))

        number = d.pop("number")

        profile_version_id = UUID(d.pop("profile_version_id"))

        script_version_id = UUID(d.pop("script_version_id"))

        status = RunStatus(d.pop("status"))

        trigger = RunTriggerType(d.pop("trigger"))

        workspace_id = UUID(d.pop("workspace_id"))

        def _parse_duration(data: object) -> Union[None, Unset, int]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, int], data)

        duration = _parse_duration(d.pop("duration", UNSET))

        def _parse_logs(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        logs = _parse_logs(d.pop("logs", UNSET))

        def _parse_time_ended(data: object) -> Union[None, Unset, datetime.datetime]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                time_ended_type_0 = isoparse(data)

                return time_ended_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, datetime.datetime], data)

        time_ended = _parse_time_ended(d.pop("time_ended", UNSET))

        def _parse_time_started(data: object) -> Union[None, Unset, datetime.datetime]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                time_started_type_0 = isoparse(data)

                return time_started_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, datetime.datetime], data)

        time_started = _parse_time_started(d.pop("time_started", UNSET))

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
            date_added=date_added,
            date_updated=date_updated,
            deployment_id=deployment_id,
            id=id,
            number=number,
            profile_version_id=profile_version_id,
            script_version_id=script_version_id,
            status=status,
            trigger=trigger,
            workspace_id=workspace_id,
            duration=duration,
            logs=logs,
            time_ended=time_ended,
            time_started=time_started,
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
