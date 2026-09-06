"""Deterministic, timezone-aware admission scheduling."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as local_time
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from post_pulsar.state import ScheduleRecord, ScheduleRunRecord

OccurrenceState = Literal["queued", "due", "missed"]


@dataclass(frozen=True, slots=True)
class ScheduledOccurrence:
    """One canonical local-date occurrence and its original timezone facts."""

    local_date: date
    scheduled_at: datetime
    utc_offset_minutes: int
    gap_delay_seconds: int = 0


@dataclass(frozen=True, slots=True)
class ScheduleEvaluation:
    """Pure decision for the current local date at one wall-clock instant."""

    occurrence: ScheduledOccurrence
    state: OccurrenceState


@dataclass(frozen=True, slots=True)
class ScheduledWork:
    """A durable occurrence ready for application dispatch."""

    run_id: int
    schedule_key: int
    profile_id: str
    schedule_id: str
    bucket: str
    scheduled_at: datetime
    retry: bool


class ScheduleRepository(Protocol):
    """Persistence surface required by the scheduler loop."""

    def list_enabled_schedules(self) -> tuple[ScheduleRecord, ...]: ...

    def list_recoverable_schedule_runs(
        self, now: datetime
    ) -> tuple[ScheduleRunRecord, ...]: ...

    def get_schedule(self, schedule_key: int) -> ScheduleRecord: ...

    def latest_schedule_local_date(self, schedule_key: int) -> date | None: ...

    def create_schedule_occurrence(
        self,
        schedule_key: int,
        *,
        local_date: str,
        scheduled_at: datetime,
        utc_offset_minutes: int,
        schedule_hash: str,
    ) -> ScheduleRunRecord: ...

    def transition_schedule_run(
        self,
        run_id: int,
        state: OccurrenceState,
        *,
        expected_revision: int,
    ) -> ScheduleRunRecord: ...


def resolve_occurrence(
    schedule: ScheduleRecord, local_date_value: date
) -> ScheduledOccurrence:
    """Resolve a local schedule slot, canonicalizing folds and timezone gaps."""

    hour, minute = (int(part) for part in schedule.local_time.split(":"))
    nominal = datetime.combine(local_date_value, local_time(hour, minute))
    zone = ZoneInfo(schedule.timezone)
    candidates = _valid_instants(nominal, zone)
    gap_delay = 0
    resolved_local = nominal
    if not candidates:
        while resolved_local.date() == local_date_value:
            resolved_local += timedelta(minutes=1)
            gap_delay += 60
            candidates = _valid_instants(resolved_local, zone)
            if candidates:
                break
    if not candidates:
        raise ValueError("local schedule date has no valid timezone instant")
    scheduled_at, offset_minutes = min(candidates, key=lambda item: item[0])
    return ScheduledOccurrence(
        local_date_value,
        scheduled_at,
        offset_minutes,
        gap_delay,
    )


def evaluate_schedule(
    schedule: ScheduleRecord, now: datetime
) -> ScheduleEvaluation | None:
    """Evaluate only the date currently visible in the schedule's timezone."""

    now_utc = _aware_utc(now)
    local_date_value = now_utc.astimezone(ZoneInfo(schedule.timezone)).date()
    if local_date_value.weekday() not in schedule.weekdays:
        return None
    occurrence = resolve_occurrence(schedule, local_date_value)
    if now_utc < occurrence.scheduled_at:
        return ScheduleEvaluation(occurrence, "queued")
    lateness = occurrence.gap_delay_seconds + int(
        (now_utc - occurrence.scheduled_at).total_seconds()
    )
    state: OccurrenceState = (
        "due" if lateness <= schedule.misfire_grace_seconds else "missed"
    )
    return ScheduleEvaluation(occurrence, state)


class DeterministicScheduler:
    """Persist current-date slots and return retry-first deterministic work."""

    def __init__(
        self,
        repository: ScheduleRepository,
        *,
        wall_clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._repository = repository
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._sleeper = sleeper or time.sleep

    def tick(self) -> tuple[ScheduledWork, ...]:
        """Re-read wall time, persist today's occurrences, and return due work."""

        now = _aware_utc(self._wall_clock())
        retries = tuple(
            self._work(run, self._repository.get_schedule(run.schedule_key), True)
            for run in self._repository.list_recoverable_schedule_runs(now)
        )
        new_work: list[ScheduledWork] = []
        for schedule in self._repository.list_enabled_schedules():
            evaluation = evaluate_schedule(schedule, now)
            if evaluation is None:
                continue
            occurrence = evaluation.occurrence
            latest = self._repository.latest_schedule_local_date(schedule.schedule_key)
            if latest is not None and occurrence.local_date < latest:
                continue
            run = self._repository.create_schedule_occurrence(
                schedule.schedule_key,
                local_date=occurrence.local_date.isoformat(),
                scheduled_at=occurrence.scheduled_at,
                utc_offset_minutes=occurrence.utc_offset_minutes,
                schedule_hash=schedule.config_hash,
            )
            if run.state == "queued" and evaluation.state in {"due", "missed"}:
                run = self._repository.transition_schedule_run(
                    run.run_id,
                    evaluation.state,
                    expected_revision=run.revision,
                )
            if run.state == "due":
                new_work.append(self._work(run, schedule, False))
        return tuple(sorted(retries, key=_work_order)) + tuple(
            sorted(new_work, key=_work_order)
        )

    def wait(self, seconds: float) -> None:
        """Wait using only monotonic elapsed time; wall time is read by `tick`."""

        if seconds <= 0:
            return
        deadline = self._monotonic_clock() + seconds
        remaining = seconds
        while True:
            self._sleeper(remaining)
            remaining = deadline - self._monotonic_clock()
            if remaining <= 0:
                return

    def wake_after(self, seconds: float) -> tuple[ScheduledWork, ...]:
        """Wait, then recalculate wall time and evaluate a fresh tick."""

        self.wait(seconds)
        return self.tick()

    @staticmethod
    def _work(
        run: ScheduleRunRecord, schedule: ScheduleRecord, retry: bool
    ) -> ScheduledWork:
        return ScheduledWork(
            run.run_id,
            schedule.schedule_key,
            schedule.profile_id,
            schedule.schedule_id,
            schedule.bucket,
            run.scheduled_at,
            retry,
        )


def _valid_instants(
    naive: datetime, zone: ZoneInfo
) -> tuple[tuple[datetime, int], ...]:
    values: dict[datetime, int] = {}
    for fold in (0, 1):
        aware = naive.replace(tzinfo=zone, fold=fold)
        instant = aware.astimezone(UTC)
        if instant.astimezone(zone).replace(tzinfo=None) != naive:
            continue
        offset = aware.utcoffset()
        if offset is None:
            continue
        values[instant] = int(offset.total_seconds() // 60)
    return tuple(values.items())


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("scheduler wall clock must be timezone-aware")
    return value.astimezone(UTC)


def _work_order(item: ScheduledWork) -> tuple[datetime, str, str]:
    return (item.scheduled_at, item.profile_id, item.schedule_id)


__all__ = [
    "DeterministicScheduler",
    "ScheduleEvaluation",
    "ScheduledOccurrence",
    "ScheduledWork",
    "evaluate_schedule",
    "resolve_occurrence",
]
