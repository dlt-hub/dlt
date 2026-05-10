from typing import TYPE_CHECKING, Dict, List, NamedTuple, Sequence, Tuple

from dlt._workspace.exceptions import WorkspaceException

if TYPE_CHECKING:
    from dlt._workspace.deployment.typing import TJobDefinition


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

    @classmethod
    def from_message(cls, message: str) -> "InvalidManifest":
        result = ManifestValidationResult(
            is_valid=False, errors=[message], warnings=[], unresolved_triggers={}
        )
        return cls(result)


class InvalidJobRef(DeploymentException, ValueError):
    def __init__(self, ref: str, reason: str) -> None:
        self.ref = ref
        super().__init__(f"Invalid job ref {ref!r}: {reason}")


class InvalidJobName(DeploymentException, ValueError):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(
            f"Job name {name!r} is not a valid Python identifier. Job names become"
            " parts of job references and configuration sections, so they must use"
            " only letters, digits and underscores and may not start with a digit."
            " If you want a human-friendly label for the UI, set it via"
            " `expose={'display_name': '...'}` and keep `name` a Python identifier."
        )


class InvalidJobSection(DeploymentException, ValueError):
    def __init__(self, section: str) -> None:
        self.section = section
        super().__init__(
            f"Job section {section!r} is not a valid Python identifier. Sections"
            " become parts of job references and configuration sections, so they"
            " must use only letters, digits and underscores and may not start with"
            " a digit. Sections default to the module name when not provided."
        )


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


class ManifestImportError(DeploymentException):
    """Deployment module cannot be loaded for manifest generation."""

    def __init__(self, name_or_path: str, file_path: str, inner: Exception, kind: str) -> None:
        self.name_or_path = name_or_path
        self.file_path = file_path
        self.inner = inner
        self.kind = kind
        if kind == "import_failed":
            msg = f"Failed to import {file_path!r}: {type(inner).__name__}: {inner}"
        elif kind == "default_missing":
            msg = (
                f"No {name_or_path!r}.py file found in the workspace. Create one and"
                " import your job declarations and notebook modules into it."
            )
        else:
            msg = (
                f"Could not import module {name_or_path!r}. Check that the file"
                " exists and is a valid Python module."
            )
        super().__init__(msg)


class AmbiguousJobSelector(DeploymentException):
    """Selector matched multiple jobs; narrow with a more specific selector or `--job-ref`."""

    def __init__(self, matches: Sequence[Tuple["TJobDefinition", str]]) -> None:
        self.matches = list(matches)
        job_list = "\n".join(f"  - {jd['job_ref']} (trigger: {t})" for jd, t in matches)
        super().__init__(
            "Multiple jobs matched. Use a more specific selector or pass"
            f" `--job-ref <ref>` to pick one:\n{job_list}"
        )


class JobRefNotInCandidates(DeploymentException):
    """`--job-ref` doesn't match any candidate from the selector match-set."""

    def __init__(self, job_ref: str, candidates: Sequence[Tuple["TJobDefinition", str]]) -> None:
        self.job_ref = job_ref
        self.candidates = list(candidates)
        refs = ", ".join(jd["job_ref"] for jd, _ in candidates)
        super().__init__(
            f"Job ref {job_ref!r} is not among the matched candidates: [{refs}]."
            " Re-run with a `--job-ref` value from this list, or change the selector."
        )


class NoMatchingJobs(DeploymentException, LookupError):
    """No job matched the given selectors; lists candidates to choose from instead."""

    def __init__(
        self,
        selectors: Sequence[str],
        available: Sequence[Tuple["TJobDefinition", str]],
    ) -> None:
        self.selectors = list(selectors)
        self.available = list(available)
        selector_str = ", ".join(selectors) or "<none>"
        if available:
            job_list = "\n".join(f"  - {jd['job_ref']} (trigger: {t})" for jd, t in available)
            body = f"Available jobs:\n{job_list}"
        else:
            body = "No matching jobs declared in the manifest."
        super().__init__(f"No jobs matched selector(s): {selector_str}.\n{body}")
