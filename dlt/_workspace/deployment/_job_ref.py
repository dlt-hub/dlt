from typing import List, Optional, Sequence, Tuple

from dlt._workspace.deployment.exceptions import AmbiguousJobRef, InvalidJobRef, JobRefNotFound
from dlt._workspace.deployment.typing import TJobRef

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
    job_refs: Optional[Sequence[TJobRef]] = None,
) -> TJobRef:
    """Resolve a user-provided ref to canonical job_ref form.

    Accepts:
      - `"name"` — resolve via job_refs list (raises without it)
      - `"section.name"` — prepend `"jobs."`
      - `"jobs.section.name"` — validate and pass through
      - `"jobs.name"` — validate and pass through (module-level)

    Args:
        ref: User-provided job reference string.
        job_refs: Optional list of known job refs for bare name resolution.

    Raises:
        InvalidJobRef: If ref format is invalid.
        JobRefNotFound: If ref not found in job_refs list.
        AmbiguousJobRef: If bare name matches multiple jobs.
    """
    ref = ref.strip()
    if not ref:
        raise InvalidJobRef(ref, "must not be empty")

    if ref.startswith(JOB_REF_PREFIX):
        result = TJobRef(ref)
        parse_job_ref(result)
        if job_refs is not None:
            _check_exists(result, job_refs)
        return result

    parts = ref.split(".")
    if len(parts) == 2:
        # section.name
        result = TJobRef(f"jobs.{ref}")
        if job_refs is not None:
            _check_exists(result, job_refs)
        return result

    if len(parts) == 1:
        # bare name — requires job_refs list
        if job_refs is None:
            raise InvalidJobRef(
                ref,
                "bare job name requires a job_refs list for resolution;"
                " use section.name or jobs.section.name form",
            )
        return _resolve_bare_name(ref, job_refs)

    raise InvalidJobRef(ref, "use name, section.name, or jobs.section.name form")


def _check_exists(ref: TJobRef, job_refs: Sequence[TJobRef]) -> None:
    """Raise if ref not found in the job_refs list."""
    if ref not in job_refs:
        raise JobRefNotFound(ref)


def _resolve_bare_name(name: str, job_refs: Sequence[TJobRef]) -> TJobRef:
    """Resolve a bare name against a job_refs list. Raises on ambiguity or miss."""
    matches: List[TJobRef] = []
    for r in job_refs:
        if short_name(r) == name:
            matches.append(r)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise JobRefNotFound(TJobRef(name))
    raise AmbiguousJobRef(name, [str(m) for m in matches])
