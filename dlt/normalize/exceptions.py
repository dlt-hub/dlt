from typing import Any, List
from dlt.common.exceptions import DltException


class NormalizeException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class NormalizeJobFailed(NormalizeException):
    def __init__(
        self, load_id: str, job_id: str, failed_message: str, writer_metrics: List[Any]
    ) -> None:
        self.load_id = load_id
        self.job_id = job_id
        self.failed_message = failed_message
        self.writer_metrics = writer_metrics
        super().__init__(
            f"Job for `{job_id=:}` failed terminally in load with `{load_id=:}` with message:"
            f" {failed_message}."
        )
