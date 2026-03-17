"""Trigger scheduler for local workspace runner."""

import time
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from dlt.common.pendulum import pendulum

from dlt._workspace.deployment._job_ref import short_name as job_short_name
from dlt._workspace.deployment._trigger_helpers import parse_trigger
from dlt._workspace.deployment.typing import TJobDefinition, TTrigger


class ScheduledItem:
    """A job+trigger pair scheduled to fire at a specific time."""

    def __init__(
        self, job_def: TJobDefinition, trigger: TTrigger, fire_at: float, repeating: bool = False
    ) -> None:
        self.job_def = job_def
        self.trigger = trigger
        self.fire_at = fire_at
        self.repeating = repeating


# trigger type groups for filtering
IMMEDIATE_TYPES: FrozenSet[str] = frozenset({"http", "deployment", "manual", "tag", "every"})
TIMED_TYPES: FrozenSet[str] = frozenset({"schedule", "every", "once"})
EVENT_TYPES: FrozenSet[str] = frozenset({"job.success", "job.fail"})


class TriggerScheduler:
    """Manages trigger evaluation and future job scheduling."""

    def __init__(
        self,
        with_future: bool = False,
        with_future_once: bool = False,
    ) -> None:
        self.with_future = with_future or with_future_once
        self.with_future_once = with_future_once
        self._event_triggers: Dict[str, List[Tuple[TJobDefinition, TTrigger]]] = {}
        self._timed: List[ScheduledItem] = []
        self._warnings: List[str] = []

    def register_job(
        self,
        job_def: TJobDefinition,
        only: Optional[FrozenSet[str]] = None,
    ) -> List[Tuple[TJobDefinition, TTrigger]]:
        """Register a job's triggers. Returns (job, trigger) pairs that fire immediately.

        Args:
            job_def: Job definition with triggers.
            only: If set, only process trigger types in this set. Others are skipped.
        """
        immediate: List[Tuple[TJobDefinition, TTrigger]] = []

        for trigger in job_def.get("triggers", []):
            try:
                parsed = parse_trigger(trigger)
            except ValueError:
                continue

            tt = parsed.type
            if only is not None and tt not in only:
                continue

            if tt in ("http", "deployment", "manual", "tag"):
                immediate.append((job_def, trigger))
            elif tt == "every":
                immediate.append((job_def, trigger))
                if self.with_future:
                    period = parsed.expr
                    if isinstance(period, (int, float)):
                        self._timed.append(
                            ScheduledItem(
                                job_def,
                                trigger,
                                fire_at=time.time() + period,
                                repeating=not self.with_future_once,
                            )
                        )
            elif tt == "schedule" and self.with_future:
                self._schedule_cron(job_def, trigger, str(parsed.expr))
            elif tt == "once" and self.with_future:
                if isinstance(parsed.expr, pendulum.DateTime):
                    self._timed.append(
                        ScheduledItem(
                            job_def,
                            trigger,
                            fire_at=parsed.expr.timestamp(),
                            repeating=False,
                        )
                    )
            elif tt in ("job.success", "job.fail"):
                event_key = f"{tt}:{parsed.expr}"
                self._event_triggers.setdefault(event_key, []).append((job_def, trigger))

        return immediate

    def fire_event(self, event: str) -> List[Tuple[TJobDefinition, TTrigger]]:
        """Fire a job event (e.g. 'job.success:jobs.mod.name'). Returns triggered jobs.

        Event triggers are persistent — they fire every time the event occurs,
        not just once.
        """
        return list(self._event_triggers.get(event, []))

    def pop_due_jobs(self) -> List[Tuple[TJobDefinition, TTrigger]]:
        """Pop all jobs whose scheduled time has arrived."""
        now = time.time()
        due: List[Tuple[TJobDefinition, TTrigger]] = []
        remaining: List[ScheduledItem] = []
        for item in self._timed:
            if item.fire_at <= now:
                due.append((item.job_def, item.trigger))
                if item.repeating:
                    try:
                        parsed = parse_trigger(item.trigger)
                    except ValueError:
                        continue
                    if parsed.type == "every" and isinstance(parsed.expr, (int, float)):
                        remaining.append(
                            ScheduledItem(
                                item.job_def,
                                item.trigger,
                                fire_at=now + parsed.expr,
                                repeating=True,
                            )
                        )
                    elif parsed.type == "schedule":
                        self._schedule_cron(
                            item.job_def, item.trigger, str(parsed.expr), into=remaining
                        )
                else:
                    short = job_short_name(item.job_def["job_ref"])
                    self._warnings.append(
                        f"{short}: {item.trigger} fired — will not run again this session"
                    )
            else:
                remaining.append(item)
        self._timed = remaining
        return due

    def pop_warnings(self) -> List[str]:
        """Return and clear accumulated warnings."""
        warnings = self._warnings
        self._warnings = []
        return warnings

    def get_next_fire_time(self) -> Optional[float]:
        """Seconds until next scheduled event, or None if empty."""
        if not self._timed:
            return None
        earliest = min(item.fire_at for item in self._timed)
        return max(0.0, earliest - time.time())

    def is_empty(self) -> bool:
        """True if no timed items remain. Event triggers are passive (persistent)."""
        return not self._timed

    def has_only_event_triggers(self) -> bool:
        """True if event triggers exist but no timed items."""
        return bool(self._event_triggers) and not self._timed

    def pending_event_jobs(self) -> List[str]:
        """Unique job refs that are waiting for events that will never fire."""
        seen: Set[str] = set()
        refs: List[str] = []
        for pairs in self._event_triggers.values():
            for job_def, _ in pairs:
                ref = job_def["job_ref"]
                if ref not in seen:
                    seen.add(ref)
                    refs.append(ref)
        return refs

    def _schedule_cron(
        self,
        job_def: TJobDefinition,
        trigger: TTrigger,
        cron_expr: str,
        into: Optional[List[ScheduledItem]] = None,
    ) -> None:
        """Schedule a cron job using croniter if available."""
        try:
            from croniter import croniter
        except ImportError:
            import sys

            print(  # noqa: T201
                "warning: croniter not installed, skipping schedule trigger"
                f" for {job_def['job_ref']}",
                file=sys.stderr,
            )
            return
        c = croniter(cron_expr, pendulum.now("UTC"))
        next_dt = c.get_next(pendulum.DateTime)
        target = into if into is not None else self._timed
        target.append(
            ScheduledItem(
                job_def,
                trigger,
                fire_at=next_dt.timestamp(),
                repeating=not self.with_future_once,
            )
        )
