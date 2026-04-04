"""Trigger scheduler for local workspace runner."""

import time
from typing import Dict, List, Optional, Set, Tuple

from dlt.common.pendulum import pendulum

from dlt._workspace.deployment._job_ref import short_name as job_short_name
from dlt._workspace.deployment.exceptions import InvalidFreshnessConstraint, InvalidTrigger
from dlt._workspace.deployment._trigger_helpers import (
    parse_trigger,
)
from dlt._workspace.deployment.freshness import parse_freshness_constraint
from dlt._workspace.deployment.typing import TJobDefinition, TTrigger
from dlt._workspace.deployment.interval import next_scheduled_run


class ScheduledItem:
    """A job+trigger pair scheduled to fire at a specific time."""

    def __init__(
        self, job_def: TJobDefinition, trigger: TTrigger, fire_at: float, repeating: bool = False
    ) -> None:
        self.job_def = job_def
        self.trigger = trigger
        self.fire_at = fire_at
        self.repeating = repeating


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
        self._last_scheduled: Dict[str, pendulum.DateTime] = {}
        """Tracks last scheduled_at per (job_ref, trigger) for every: rescheduling."""

    def register_triggers(
        self,
        job_def: TJobDefinition,
        triggers: List[TTrigger],
    ) -> List[Tuple[TJobDefinition, TTrigger]]:
        """Register specific triggers for a job. Returns pairs that fire immediately."""
        immediate: List[Tuple[TJobDefinition, TTrigger]] = []

        for trigger in triggers:
            try:
                parsed = parse_trigger(trigger)
            except InvalidTrigger:
                continue

            tt = parsed.type

            if tt in ("http", "deployment", "manual", "tag"):
                immediate.append((job_def, trigger))
            elif tt == "every" and self.with_future:
                self._schedule_timed(job_def, trigger)
            elif tt == "schedule" and self.with_future:
                self._schedule_timed(job_def, trigger)
            elif tt == "once" and self.with_future:
                self._schedule_timed(job_def, trigger)
            elif tt in ("job.success", "job.fail"):
                event_key = f"{tt}:{parsed.expr}"
                self._event_triggers.setdefault(event_key, []).append((job_def, trigger))

        return immediate

    def register_freshness_listeners(self, job_def: TJobDefinition) -> None:
        """Register internal job.success listeners for freshness constraints.

        When an upstream job completes, the downstream is re-dispatched
        using its default_trigger. These listeners are runtime-only.
        """
        default_trigger = job_def.get("default_trigger")
        if not default_trigger:
            return
        for constraint in job_def.get("freshness", []):
            try:
                fc = parse_freshness_constraint(constraint)
            except InvalidFreshnessConstraint:
                continue
            event_key = f"job.success:{fc.expr}"
            self._event_triggers.setdefault(event_key, []).append((job_def, default_trigger))

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
                    # record this fire time for every: rescheduling
                    key = f"{item.job_def['job_ref']}:{item.trigger}"
                    self._last_scheduled[key] = pendulum.from_timestamp(item.fire_at, tz="UTC")
                    self._schedule_timed(item.job_def, item.trigger, into=remaining)
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

    def _schedule_timed(
        self,
        job_def: TJobDefinition,
        trigger: TTrigger,
        into: Optional[List[ScheduledItem]] = None,
    ) -> None:
        """Schedule a timed trigger using next_scheduled_run."""
        tz = job_def.get("require", {}).get("timezone", "UTC")
        key = f"{job_def['job_ref']}:{trigger}"
        prev = self._last_scheduled.get(key)

        try:
            result = next_scheduled_run(
                trigger, pendulum.now("UTC"), tz=tz, prev_scheduled_run=prev
            )
        except InvalidTrigger:
            return

        target = into if into is not None else self._timed
        target.append(
            ScheduledItem(
                job_def,
                trigger,
                fire_at=result.scheduled_at.timestamp(),
                repeating=not self.with_future_once,
            )
        )
