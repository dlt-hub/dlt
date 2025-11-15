from enum import Enum


class RunStatus(str, Enum):
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
    RUNNING = "running"
    STARTING = "starting"

    def __str__(self) -> str:
        return str(self.value)
