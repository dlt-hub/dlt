from enum import Enum


class RunStatus(str, Enum):
    CANCELLED = "cancelled"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    FINALIZING = "finalizing"
    PENDING = "pending"
    RUNNING = "running"
    SKIPPED = "skipped"
    STARTING = "starting"

    def __str__(self) -> str:
        return str(self.value)
