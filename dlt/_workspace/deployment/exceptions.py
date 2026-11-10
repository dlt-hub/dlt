from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Sequence, Tuple, cast

from dlt._workspace.exceptions import WorkspaceException
from dlt._workspace.workspace_definition import format_data_access

if TYPE_CHECKING:
    from dlt._workspace.deployment.typing import TJobDefinition, TJobResult


class DeploymentException(WorkspaceException):
    pass


class DeploymentValidationError(DeploymentException, ValueError):
    """Invalid deployment input: manifest, job definition, trigger, job ref or agent definition."""

    def __init__(
        self,
        message: str,
        errors: Optional[Sequence[str]] = None,
        warnings: Optional[Sequence[str]] = None,
        subject: Optional[str] = None,
    ) -> None:
        self.errors = list(errors) if errors else [message]
        self.warnings = list(warnings or [])
        self.subject = subject
        """What was invalid, when it is one thing: a job ref, a trigger, a path."""
        super().__init__(message)

    @classmethod
    def from_message(cls, message: str) -> "DeploymentValidationError":
        """Rebuilds an instance from its message alone, as a worker process reports it."""
        exc = cls.__new__(cls)
        DeploymentValidationError.__init__(exc, message)
        return exc


class JobValidationResult(NamedTuple):
    errors: List[str]
    warnings: List[str]


class InvalidJobSchema(DeploymentValidationError):
    def __init__(self, source: str, reason: str) -> None:
        self.source = source
        super().__init__(
            f"Cannot read the schema of {source!r}: {reason}", errors=[reason], subject=source
        )


class InvalidJobDefinition(DeploymentValidationError):
    def __init__(self, job_ref: str, validation: JobValidationResult) -> None:
        self.job_ref = job_ref
        self.validation = validation
        msg = f"Invalid job definition {job_ref!r}:\n"
        msg += "\n".join(f"  - {e}" for e in validation.errors)
        if validation.warnings:
            msg += "\nWarnings:\n" + "\n".join(f"  - {w}" for w in validation.warnings)
        super().__init__(
            msg, errors=validation.errors, warnings=validation.warnings, subject=job_ref
        )


class ManifestValidationResult(NamedTuple):
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    unresolved_triggers: Dict[str, List[str]]
    """Maps job_ref -> list of unresolved upstream job refs from triggers."""


class InvalidManifest(DeploymentValidationError):
    def __init__(self, validation: ManifestValidationResult) -> None:
        self.validation = validation
        msg = "Invalid deployment manifest:\n" + "\n".join(f"  - {e}" for e in validation.errors)
        super().__init__(msg, errors=validation.errors, warnings=validation.warnings)

    @classmethod
    def from_message(cls, message: str) -> "InvalidManifest":
        exc = cast("InvalidManifest", super().from_message(message))
        exc.validation = ManifestValidationResult(
            is_valid=False, errors=[message], warnings=[], unresolved_triggers={}
        )
        return exc


class ManifestEngineNoUpgradePath(DeploymentValidationError):
    def __init__(self, subject: str, from_engine: int, to_engine: int) -> None:
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(
            f"No {subject} migration path from engine {from_engine} to {to_engine}. A manifest"
            " written by a newer dlt cannot be downgraded — upgrade dlt to read it.",
            subject=subject,
        )


class ProfileDataAccessNotAvailable(DeploymentValidationError):
    """The environment that the deployment is made to has no profile with settings files that
    gives the data access a job declares."""

    # `from_message` skips `__init__`, so the attributes of the profile exceptions default here
    job_ref: str = None
    env: str = None
    data_access: str = None
    profiles: Dict[str, str] = None

    def __init__(
        self,
        job_ref: str,
        env: str,
        data_access: str,
        profiles: Dict[str, str],
        default: Optional[str] = None,
    ) -> None:
        self.job_ref = job_ref
        self.env = env
        self.data_access = data_access
        self.profiles = dict(profiles)
        if default:
            problem = (
                f"and `{default}`, its default profile for it, has no settings files. Create them"
                f" with `dlthub profile add {default}`."
            )
        else:
            deployed = ", ".join(f"{n} ({format_data_access(a)})" for n, a in profiles.items())
            problem = (
                f"and has no profile for it: {deployed}. Declare one with `dlthub profile add"
                f" {env}-{data_access} --access {data_access} --env {env}`."
            )
        super().__init__(
            f"Job `{job_ref}` declares {format_data_access(data_access)}."
            f" Environment `{env}` is strict about access {problem}"
            " Or declare the access that the job needs with `access=` on the job."
            f" Or set `strict_access: false` on environment `{env}`."
            " Then the job gets the closest default profile with more data access.",
            subject=job_ref,
        )


class JobProfileAccessMismatch(DeploymentValidationError):
    """A job declares a profile that does not give exactly the data access that the job declares,
    or has no settings files, in an environment that is strict about access."""

    job_ref: str = None
    profile: str = None
    profile_data_access: str = None
    data_access: str = None
    env: str = None

    def __init__(
        self,
        job_ref: str,
        profile: str,
        profile_data_access: str,
        data_access: str,
        env: str,
        configured: bool = True,
    ) -> None:
        self.job_ref = job_ref
        self.profile = profile
        self.profile_data_access = profile_data_access
        self.data_access = data_access
        self.env = env
        if profile_data_access != data_access:
            problem = (
                f"Job `{job_ref}` declares profile `{profile}` with"
                f" {format_data_access(profile_data_access)} and needs"
                f" {format_data_access(data_access)}."
            )
            remedy = (
                " Declare the access that the job needs with `access=` on the job, or declare a"
                f" profile with {format_data_access(data_access)} on it."
            )
        else:
            problem = f"Job `{job_ref}` declares profile `{profile}`, which has no settings files."
            remedy = f" Create them with `dlthub profile add {profile}`."
        if not configured and profile_data_access != data_access:
            remedy += f" `{profile}` also has no settings files."
        super().__init__(
            f"{problem} Environment `{env}` is strict about access: the profile of a job gives"
            f" exactly the access that the job declares.{remedy}"
            f" Or set `strict_access: false` on environment `{env}`.",
            subject=job_ref,
        )


class JobProfileNotInEnvironment(DeploymentValidationError):
    """A job declares a profile that is not one of the environment deployed to: undeclared, local
    or of another environment."""

    job_ref: str = None
    profile: str = None
    data_access: str = None
    env: str = None

    def __init__(
        self, job_ref: str, profile: str, data_access: str, env: str, profiles: Sequence[str]
    ) -> None:
        self.job_ref = job_ref
        self.profile = profile
        self.data_access = data_access
        self.env = env
        super().__init__(
            f"Job `{job_ref}` declares profile `{profile}`, which is not a profile of environment"
            f" `{env}` ({', '.join(profiles)})."
            " A deployment uses only the profiles of its environment, never a local profile."
            f" If `{profile}` is not declared, declare it with `dlthub profile add {profile}"
            f" --access {data_access} --env {env}`: the job needs"
            f" {format_data_access(data_access)}."
            " If it belongs to another environment, deploy there with `--environment`, or edit"
            f" `.dlt/.workspace` and move its declaration to environment `{env}`."
            f" Or declare a profile of `{env}` on the job with `require={{'profile': ...}}`.",
            subject=job_ref,
        )


class InvalidJobRef(DeploymentValidationError):
    def __init__(self, ref: str, reason: str) -> None:
        self.ref = ref
        super().__init__(f"Invalid job ref {ref!r}: {reason}", errors=[reason], subject=ref)


class InvalidJobName(DeploymentValidationError):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(
            f"Job name {name!r} is not a valid Python identifier. Job names become"
            " parts of job references and configuration sections, so they must use"
            " only letters, digits and underscores and may not start with a digit."
            " If you want a human-friendly label for the UI, set it via"
            " `expose={'display_name': '...'}` and keep `name` a Python identifier.",
            subject=name,
        )


class InvalidJobSection(DeploymentValidationError):
    def __init__(self, section: str) -> None:
        self.section = section
        super().__init__(
            f"Job section {section!r} is not a valid Python identifier. Sections"
            " become parts of job references and configuration sections, so they"
            " must use only letters, digits and underscores and may not start with"
            " a digit. Sections default to the module name when not provided.",
            subject=section,
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


class InvalidTrigger(DeploymentValidationError):
    def __init__(self, trigger: str, reason: str) -> None:
        self.trigger = trigger
        super().__init__(f"Invalid trigger {trigger!r}: {reason}", errors=[reason], subject=trigger)


class InvalidFreshnessConstraint(DeploymentValidationError):
    def __init__(self, constraint: str, reason: str) -> None:
        self.constraint = constraint
        super().__init__(
            f"Invalid freshness constraint {constraint!r}: {reason}",
            errors=[reason],
            subject=constraint,
        )


class JobResolutionError(DeploymentException):
    def __init__(self, ref: str, reason: str) -> None:
        self.ref = ref
        super().__init__(f"Cannot resolve job {ref!r}: {reason}")


class JobAbortedException(DeploymentException):
    """A job ended its run deliberately without completing its task."""

    def __init__(self, summary: str, result: "TJobResult") -> None:
        self.summary = summary
        self.result = result
        super().__init__(f"Job aborted: {summary}")


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
