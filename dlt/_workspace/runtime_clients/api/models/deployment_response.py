import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Optional, TextIO, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

T = TypeVar("T", bound="DeploymentResponse")


@_attrs_define
class DeploymentResponse:
    """
    Attributes:
        content_hash (str): The hash of the file of the uploaded tarball
        created_by (UUID): The ID of the identity who created the deployment
        date_added (datetime.datetime): The date the entity was added
        date_updated (datetime.datetime): The date the entity was updated
        file_count (int): The number of files in the uploaded tarball
        file_names (str): The names of the files in the uploaded tarball
        id (UUID): The uniqueID of the entity
        size (int): The size of the uploaded tarball in bytes
        version (int): The version of the deployment. Will increment for each new version of the deployment
        workspace_id (UUID): The ID of the workspace the deployment belongs to
    """

    content_hash: str
    created_by: UUID
    date_added: datetime.datetime
    date_updated: datetime.datetime
    file_count: int
    file_names: str
    id: UUID
    size: int
    version: int
    workspace_id: UUID
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        content_hash = self.content_hash

        created_by = str(self.created_by)

        date_added = self.date_added.isoformat()

        date_updated = self.date_updated.isoformat()

        file_count = self.file_count

        file_names = self.file_names

        id = str(self.id)

        size = self.size

        version = self.version

        workspace_id = str(self.workspace_id)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "content_hash": content_hash,
                "created_by": created_by,
                "date_added": date_added,
                "date_updated": date_updated,
                "file_count": file_count,
                "file_names": file_names,
                "id": id,
                "size": size,
                "version": version,
                "workspace_id": workspace_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        content_hash = d.pop("content_hash")

        created_by = UUID(d.pop("created_by"))

        date_added = isoparse(d.pop("date_added"))

        date_updated = isoparse(d.pop("date_updated"))

        file_count = d.pop("file_count")

        file_names = d.pop("file_names")

        id = UUID(d.pop("id"))

        size = d.pop("size")

        version = d.pop("version")

        workspace_id = UUID(d.pop("workspace_id"))

        deployment_response = cls(
            content_hash=content_hash,
            created_by=created_by,
            date_added=date_added,
            date_updated=date_updated,
            file_count=file_count,
            file_names=file_names,
            id=id,
            size=size,
            version=version,
            workspace_id=workspace_id,
        )

        deployment_response.additional_properties = d
        return deployment_response

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
