from enum import Enum


class RunTriggerType(str, Enum):
    MANUAL = "manual"
    SCHEDULE = "schedule"

    def __str__(self) -> str:
        return str(self.value)
