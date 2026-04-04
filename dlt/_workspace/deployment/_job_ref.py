from typing import List, Optional, Tuple

from dlt._workspace.deployment.exceptions import AmbiguousJobRef, InvalidJobRef, JobRefNotFound
from dlt._workspace.deployment.typing import TJobDefinition, TJobRef

JOB_REF_PREFIX = "jobs."


def make_job_ref(section: str, name: str) -> TJobRef:
    """Build canonical job_ref from section and name.

    Args:
        section: Module section (e.g. `"batch_jobs"`). Empty for module-level jobs.
        name: Job name (e.g. `"backfill"`).
    """
    if section:
        return TJobRef(f"jobs.{section}.{name}")
    return TJobRef(f"jobs.{name}")


def parse_job_ref(ref: TJobRef) -> Tuple[str, str]:
    """Parse a job_ref into (section, name).

    Returns:
        Tuple of (section, name). Section is empty for 2-part module-level refs.

    Raises:
        InvalidJobRef: If ref is not a valid job_ref.
    """
    if not ref.startswith(JOB_REF_PREFIX):
        raise InvalidJobRef(ref, f"must start with {JOB_REF_PREFIX!r}")
    rest = ref[len(JOB_REF_PREFIX) :]
    parts = rest.split(".")
    if len(parts) == 1:
        return ("", parts[0])
    if len(parts) == 2:
        return (parts[0], parts[1])
    raise InvalidJobRef(ref, "must be jobs.<name> or jobs.<section>.<name>")


def short_name(ref: str) -> str:
    """Extract the name (last component) from a job_ref."""
    return ref.rsplit(".", 1)[-1]


def resolve_job_ref(
    ref: str,
    jobs: Optional[List[TJobDefinition]] = None,
) -> TJobRef:
    """Resolve a user-provided ref to canonical job_ref form.

    Accepts:
      - `"name"` — resolve via jobs list (raises without it)
      - `"section.name"` — prepend `"jobs."`
      - `"jobs.section.name"` — validate and pass through
      - `"jobs.name"` — validate and pass through (module-level)

    Args:
        ref: User-provided job reference string.
        jobs: Optional list of job definitions for bare name resolution.

    Raises:
        InvalidJobRef: If ref format is invalid.
        JobRefNotFound: If ref not found in jobs list.
        AmbiguousJobRef: If bare name matches multiple jobs.
    """
    ref = ref.strip()
    if not ref:
        raise InvalidJobRef(ref, "must not be empty")

    if ref.startswith(JOB_REF_PREFIX):
        result = TJobRef(ref)
        parse_job_ref(result)
        if jobs is not None:
            _check_exists(result, jobs)
        return result

    parts = ref.split(".")
    if len(parts) == 2:
        # section.name
        result = TJobRef(f"jobs.{ref}")
        if jobs is not None:
            _check_exists(result, jobs)
        return result

    if len(parts) == 1:
        # bare name — requires jobs list
        if jobs is None:
            raise InvalidJobRef(
                ref,
                "bare job name requires a jobs list for resolution;"
                " use section.name or jobs.section.name form",
            )
        return _resolve_bare_name(ref, jobs)

    raise InvalidJobRef(ref, "use name, section.name, or jobs.section.name form")


def _check_exists(ref: TJobRef, jobs: List[TJobDefinition]) -> None:
    """Raise if ref not found in the jobs list."""
    for j in jobs:
        if j["job_ref"] == ref:
            return
    raise JobRefNotFound(ref)


def _resolve_bare_name(name: str, jobs: List[TJobDefinition]) -> TJobRef:
    """Resolve a bare name against a jobs list. Raises on ambiguity or miss."""
    matches: List[TJobRef] = []
    for j in jobs:
        if short_name(j["job_ref"]) == name:
            matches.append(j["job_ref"])
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise JobRefNotFound(TJobRef(name))
    raise AmbiguousJobRef(name, [str(m) for m in matches])
