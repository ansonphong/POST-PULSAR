"""Deterministic wall-clock scheduler tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from post_pulsar.scheduler import DeterministicScheduler, evaluate_schedule
from post_pulsar.state import ScheduleRecord, ScheduleRunRecord


def _schedule(
    *,
    key: int = 1,
    profile_id: str = "alpha",
    schedule_id: str = "morning",
    timezone: str = "UTC",
    local_time: str = "09:00",
    weekdays: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6),
    grace: int = 600,
) -> ScheduleRecord:
    return ScheduleRecord(
        key,
        profile_id,
        schedule_id,
        "QUEUE",
        timezone,
        weekdays,
        local_time,
        grace,
        True,
        f"{key:064x}",
        1,
    )


def test_fold_uses_earlier_utc_and_gap_uses_first_valid_tick_with_bounded_grace() -> (
    None
):
    fold = _schedule(timezone="America/Vancouver", local_time="01:30", grace=3600)
    folded = evaluate_schedule(fold, datetime(2026, 11, 1, 9, 15, tzinfo=UTC))
    assert folded is not None
    assert folded.occurrence.local_date == date(2026, 11, 1)
    assert folded.occurrence.scheduled_at == datetime(2026, 11, 1, 8, 30, tzinfo=UTC)
    assert folded.occurrence.utc_offset_minutes == -420
    assert folded.state == "due"

    gap = _schedule(timezone="America/Vancouver", local_time="02:30", grace=1800)
    first_valid = evaluate_schedule(gap, datetime(2026, 3, 8, 10, tzinfo=UTC))
    assert first_valid is not None
    assert first_valid.occurrence.scheduled_at == datetime(2026, 3, 8, 10, tzinfo=UTC)
    assert first_valid.occurrence.utc_offset_minutes == -420
    assert first_valid.occurrence.gap_delay_seconds == 1800
    assert first_valid.state == "due"
    missed = evaluate_schedule(
        replace(gap, misfire_grace_seconds=1799),
        datetime(2026, 3, 8, 10, tzinfo=UTC),
    )
    assert missed is not None and missed.state == "missed"


class _Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value
        self.reads = 0

    def __call__(self) -> datetime:
        self.reads += 1
        return self.value


class _Repository:
    def __init__(self) -> None:
        self.schedules = (
            _schedule(key=2, profile_id="zulu", schedule_id="same"),
            _schedule(key=1, profile_id="alpha", schedule_id="later"),
            _schedule(key=3, profile_id="alpha", schedule_id="first"),
        )
        self.runs: dict[tuple[int, str], ScheduleRunRecord] = {}
        self.next_id = 10
        self.retry = ScheduleRunRecord(
            1,
            99,
            42,
            "2026-09-07",
            datetime(2026, 9, 7, 8, 59, tzinfo=UTC),
            0,
            "f" * 64,
            "failed",
            3,
        )
        self.retry_schedule = _schedule(key=99, profile_id="omega", schedule_id="retry")

    def list_enabled_schedules(self) -> tuple[ScheduleRecord, ...]:
        return self.schedules

    def list_recoverable_schedule_runs(
        self, now: datetime
    ) -> tuple[ScheduleRunRecord, ...]:
        del now
        return (self.retry,)

    def get_schedule(self, schedule_key: int) -> ScheduleRecord:
        assert schedule_key == 99
        return self.retry_schedule

    def latest_schedule_local_date(self, schedule_key: int) -> date | None:
        values = [
            date.fromisoformat(local_date)
            for key, local_date in self.runs
            if key == schedule_key
        ]
        return max(values, default=None)

    def create_schedule_occurrence(
        self,
        schedule_key: int,
        *,
        local_date: str,
        scheduled_at: datetime,
        utc_offset_minutes: int,
        schedule_hash: str,
    ) -> ScheduleRunRecord:
        identity = (schedule_key, local_date)
        if identity not in self.runs:
            self.runs[identity] = ScheduleRunRecord(
                self.next_id,
                schedule_key,
                None,
                local_date,
                scheduled_at,
                utc_offset_minutes,
                schedule_hash,
                "queued",
                1,
            )
            self.next_id += 1
        return self.runs[identity]

    def transition_schedule_run(
        self, run_id: int, state: str, *, expected_revision: int
    ) -> ScheduleRunRecord:
        run = next(item for item in self.runs.values() if item.run_id == run_id)
        assert run.revision == expected_revision
        updated = replace(run, state=state, revision=run.revision + 1)
        self.runs[(run.schedule_key, run.local_date)] = updated
        return updated


def test_tick_recalculates_wall_time_orders_deterministically_and_prioritizes_retry() -> (
    None
):
    repository = _Repository()
    wall = _Clock(datetime(2026, 9, 7, 9, 5, tzinfo=UTC))
    monotonic_reads: list[float] = []

    def monotonic() -> float:
        monotonic_reads.append(7.0)
        return 7.0

    scheduler = DeterministicScheduler(
        repository,  # type: ignore[arg-type]
        wall_clock=wall,
        monotonic_clock=monotonic,
        sleeper=lambda _seconds: None,
    )

    first = scheduler.tick()
    second = scheduler.tick()

    assert [item.schedule_id for item in first] == [
        "retry",
        "first",
        "later",
        "same",
    ]
    assert first[0].retry
    assert all(not item.retry for item in first[1:])
    assert [(item.run_id, item.schedule_id) for item in second] == [
        (item.run_id, item.schedule_id) for item in first
    ]
    assert wall.reads == 2
    assert monotonic_reads == []  # monotonic time is reserved for waits/timeouts


def test_wait_recomputes_wall_after_monotonic_sleep() -> None:
    repository = _Repository()
    wall = _Clock(datetime(2026, 9, 7, 8, 59, tzinfo=UTC))
    monotonic_values = iter((20.0, 80.0))
    sleeps: list[float] = []
    scheduler = DeterministicScheduler(
        repository,  # type: ignore[arg-type]
        wall_clock=wall,
        monotonic_clock=lambda: next(monotonic_values),
        sleeper=sleeps.append,
    )

    scheduler.wait(60.0)
    wall.value += timedelta(minutes=1)
    work = scheduler.tick()

    assert sleeps == [60.0]
    assert wall.reads == 1
    assert work


def test_paused_scheduler_does_not_admit_or_recover_work() -> None:
    repository = _Repository()
    repository.get_pause_state = lambda: SimpleNamespace(paused=True)  # type: ignore[attr-defined]
    scheduler = DeterministicScheduler(
        repository,  # type: ignore[arg-type]
        wall_clock=lambda: datetime(2026, 9, 7, 9, 5, tzinfo=UTC),
    )
    assert scheduler.tick() == ()
    assert repository.runs == {}


def test_occurrence_gate_stops_later_admission_and_failure_isolates_profiles() -> None:
    repository = _Repository()
    scheduler = DeterministicScheduler(
        repository, wall_clock=lambda: datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
    )
    started = 0

    def gate(callback):
        nonlocal started
        if started:
            return False
        started += 1
        callback()
        return True

    scheduler.tick(dispatch_gate=gate)
    assert len(repository.runs) == 1
    repository = _Repository()
    repository.schedules = (
        replace(repository.schedules[0], timezone="Invalid/Zone"),
        *repository.schedules[1:],
    )
    failures = []
    scheduler = DeterministicScheduler(
        repository, wall_clock=lambda: datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
    )
    scheduler.tick(
        failure_handler=lambda schedule: failures.append(schedule.profile_id)
    )
    assert failures == ["zulu"]
    assert len(repository.runs) == 2
