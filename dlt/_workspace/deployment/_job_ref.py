from typing import List, Optional, Tuple

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
        ValueError: If ref is not a valid job_ref.
    """
    if not ref.startswith(JOB_REF_PREFIX):
        raise ValueError(f"job_ref must start with {JOB_REF_PREFIX!r}, got {ref!r}")
    rest = ref[len(JOB_REF_PREFIX) :]
    parts = rest.split(".")
    if len(parts) == 1:
        return ("", parts[0])
    if len(parts) == 2:
        return (parts[0], parts[1])
    raise ValueError(f"job_ref must be jobs.<name> or jobs.<section>.<name>, got {ref!r}")


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
        ValueError: If ref is ambiguous, not found, or invalid.
    """
    ref = ref.strip()
    if not ref:
        raise ValueError("job ref must not be empty")

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
            raise ValueError(
                f"bare job name {ref!r} requires a jobs list for resolution;"
                " use section.name or jobs.section.name form"
            )
        return _resolve_bare_name(ref, jobs)

    raise ValueError(f"invalid job ref {ref!r}")


def _check_exists(ref: TJobRef, jobs: List[TJobDefinition]) -> None:
    """Raise if ref not found in the jobs list."""
    for j in jobs:
        if j["job_ref"] == ref:
            return
    raise ValueError(f"job ref {ref!r} not found in manifest")


def _resolve_bare_name(name: str, jobs: List[TJobDefinition]) -> TJobRef:
    """Resolve a bare name against a jobs list. Raises on ambiguity or miss."""
    matches: List[TJobRef] = []
    for j in jobs:
        if short_name(j["job_ref"]) == name:
            matches.append(j["job_ref"])
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"no job matching name {name!r}")
    raise ValueError(f"ambiguous job name {name!r}, matches: {', '.join(matches)}")
