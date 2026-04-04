from typing import Any, Dict, List, NamedTuple, Type

from dlt._workspace.exceptions import WorkspaceException


class DeploymentException(WorkspaceException):
    pass


class JobValidationResult(NamedTuple):
    errors: List[str]
    warnings: List[str]


class InvalidJobDefinition(ValueError, DeploymentException):
    def __init__(self, job_ref: str, validation: JobValidationResult) -> None:
        self.job_ref = job_ref
        self.validation = validation
        msg = f"Invalid job definition {job_ref!r}:\n"
        msg += "\n".join(f"  - {e}" for e in validation.errors)
        if validation.warnings:
            msg += "\nWarnings:\n" + "\n".join(f"  - {w}" for w in validation.warnings)
        super().__init__(msg)


class ManifestValidationResult(NamedTuple):
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    unresolved_triggers: Dict[str, List[str]]
    """Maps job_ref -> list of unresolved upstream job refs from triggers."""


class InvalidManifest(DeploymentException):
    def __init__(self, validation: ManifestValidationResult) -> None:
        self.validation = validation
        msg = "Invalid deployment manifest:\n" + "\n".join(f"  - {e}" for e in validation.errors)
        super().__init__(msg)


class InvalidJobRef(DeploymentException, ValueError):
    def __init__(self, ref: str, reason: str) -> None:
        self.ref = ref
        super().__init__(f"Invalid job ref {ref!r}: {reason}")


class JobRefNotFound(DeploymentException, KeyError):
    def __init__(self, ref: str) -> None:
        self.ref = ref
        super().__init__(f"job ref {ref!r} not found in manifest")


class AmbiguousJobRef(DeploymentException):
    def __init__(self, name: str, matches: List[str]) -> None:
        self.name = name
        self.matches = matches
        super().__init__(f"ambiguous job name {name!r}, matches: {', '.join(matches)}")


class InvalidTrigger(DeploymentException, ValueError):
    def __init__(self, trigger: str, reason: str) -> None:
        self.trigger = trigger
        super().__init__(f"Invalid trigger {trigger!r}: {reason}")


class InvalidFreshnessConstraint(DeploymentException, ValueError):
    def __init__(self, constraint: str, reason: str) -> None:
        self.constraint = constraint
        super().__init__(f"Invalid freshness constraint {constraint!r}: {reason}")


class JobResolutionError(DeploymentException):
    def __init__(self, ref: str, reason: str) -> None:
        self.ref = ref
        super().__init__(f"Cannot resolve job {ref!r}: {reason}")
